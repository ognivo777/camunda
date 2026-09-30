/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.lite;

import io.camunda.zeebe.engine.state.QueryService;
import io.camunda.zeebe.gateway.cmd.BrokerErrorException;
import io.camunda.zeebe.gateway.cmd.BrokerRejectionException;
import io.camunda.zeebe.gateway.cmd.IllegalBrokerResponseException;
import io.camunda.zeebe.gateway.impl.broker.BrokerClient;
import io.camunda.zeebe.gateway.impl.broker.BrokerResponseConsumer;
import io.camunda.zeebe.gateway.impl.broker.cluster.BrokerTopologyManager;
import io.camunda.zeebe.gateway.impl.broker.request.BrokerExecuteCommand;
import io.camunda.zeebe.gateway.impl.broker.request.BrokerRequest;
import io.camunda.zeebe.gateway.impl.broker.response.BrokerResponse;
import io.camunda.zeebe.gateway.query.impl.BrokerExecuteQuery;
import io.camunda.zeebe.logstreams.impl.log.LogEntryDescriptor;
import io.camunda.zeebe.logstreams.log.LogAppendEntry;
import io.camunda.zeebe.logstreams.log.LogStreamWriter;
import io.camunda.zeebe.protocol.Protocol;
import io.camunda.zeebe.protocol.impl.encoding.ErrorResponse;
import io.camunda.zeebe.protocol.impl.encoding.ExecuteQueryRequest;
import io.camunda.zeebe.protocol.impl.encoding.ExecuteQueryResponse;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.camunda.zeebe.protocol.record.ErrorCode;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.scheduler.future.ActorFuture;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-process replacement for the broker's {@code BrokerClientImpl} + {@code
 * BrokerRequestManager}.
 *
 * <p>Instead of sending gRPC requests over the network to a broker, every gateway request is
 * handled directly in this process:
 *
 * <ul>
 *   <li>Command requests ({@link BrokerExecuteCommand}, which covers all gateway commands —
 *       deployment, instance creation, job activation/completion, ...) are appended to the log
 *       stream as {@code COMMAND} records, exactly the way the broker's {@code
 *       CommandApiRequestHandler} does. The engine processes them and the {@link
 *       InMemoryCommandResponseWriter} delivers the serialized response back here, which
 *       completes the pending request future.
 *   <li>Query requests ({@link BrokerExecuteQuery}) are answered directly from the engine state
 *       via the {@link QueryService}.
 *   <li>Job availability notifications are routed in-process to the gateway's long-polling
 *       handler (replacing the atomix {@code ClusterEventService} broadcast).
 * </ul>
 */
@SuppressWarnings("removal")
public final class InProcessBrokerClient implements BrokerClient {

  public static final String JOBS_AVAILABLE_TOPIC = "jobsAvailable";

  private static final Logger LOG = LoggerFactory.getLogger(InProcessBrokerClient.class);
  private static final int REQUEST_STREAM_ID = 1;

  private final LogStreamWriter logStreamWriter;
  private final BrokerTopologyManager topologyManager;
  private final QueryService queryService;
  private final AtomicLong requestIdGenerator = new AtomicLong(0);
  private final Map<Long, PendingRequest> pendingRequests = new ConcurrentHashMap<>();
  private final Map<String, List<Consumer<String>>> jobAvailableHandlers = new ConcurrentHashMap<>();
  private volatile boolean closed;

  private record PendingRequest(
      BrokerRequest<?> request, CompletableFuture<BrokerResponse<?>> future) {}

  public InProcessBrokerClient(
      final LogStreamWriter logStreamWriter,
      final BrokerTopologyManager topologyManager,
      final QueryService queryService) {
    this.logStreamWriter = logStreamWriter;
    this.topologyManager = topologyManager;
    this.queryService = queryService;
  }

  @Override
  public Collection<ActorFuture<Void>> start() {
    return List.of();
  }

  @Override
  public void close() {
    closed = true;
    pendingRequests
        .values()
        .forEach(
            pending ->
                pending.future().completeExceptionally(new IllegalStateException("Broker client closed")));
    pendingRequests.clear();
    jobAvailableHandlers.clear();
  }

  @Override
  public <T> CompletableFuture<BrokerResponse<T>> sendRequest(final BrokerRequest<T> request) {
    return sendRequestInternal(request, null);
  }

  @Override
  public <T> CompletableFuture<BrokerResponse<T>> sendRequest(
      final BrokerRequest<T> request, final Duration requestTimeout) {
    return sendRequestInternal(request, requestTimeout);
  }

  @Override
  public <T> CompletableFuture<BrokerResponse<T>> sendRequestWithRetry(
      final BrokerRequest<T> request) {
    // no retries needed: the "broker" is this process
    return sendRequest(request);
  }

  @Override
  public <T> CompletableFuture<BrokerResponse<T>> sendRequestWithRetry(
      final BrokerRequest<T> request, final Duration requestTimeout) {
    return sendRequest(request, requestTimeout);
  }

  @Override
  public <T> void sendRequestWithRetry(
      final BrokerRequest<T> request,
      final BrokerResponseConsumer<T> responseConsumer,
      final Consumer<Throwable> throwableConsumer) {
    sendRequest(request)
        .whenComplete(
            (response, error) -> {
              if (error == null) {
                responseConsumer.accept(response.getKey(), response.getResponse());
              } else {
                throwableConsumer.accept(error);
              }
            });
  }

  @Override
  public BrokerTopologyManager getTopologyManager() {
    return topologyManager;
  }

  @Override
  public void subscribeJobAvailableNotification(
      final String topic, final Consumer<String> handler) {
    jobAvailableHandlers.computeIfAbsent(topic, key -> new CopyOnWriteArrayList<>()).add(handler);
  }

  /**
   * Called by the {@link InMemoryJobStreamer} when work for the given job type became available.
   * Notifies all subscribers of the {@link #JOBS_AVAILABLE_TOPIC}, e.g. the gateway's long
   * polling job activation handler.
   */
  public void notifyJobAvailable(final String jobType) {
    final List<Consumer<String>> handlers = jobAvailableHandlers.get(JOBS_AVAILABLE_TOPIC);
    if (handlers != null) {
      handlers.forEach(handler -> handler.accept(jobType));
    }
  }

  /**
   * Called by the {@link InMemoryCommandResponseWriter} when the engine produced a response for
   * the given request. Completes the pending request future with the deserialized response.
   */
  public void onResponse(final long requestId, final DirectBuffer responseBuffer) {
    final PendingRequest pending = pendingRequests.remove(requestId);
    if (pending == null) {
      LOG.debug("Received response for unknown or already completed request {}", requestId);
      return;
    }
    try {
      final BrokerResponse<?> response = pending.request().getResponse(responseBuffer);
      completeWithBrokerResponse(pending.future(), response);
    } catch (final Throwable t) {
      LOG.error("Failed to read response for request {}", requestId, t);
      pending.future().completeExceptionally(t);
    }
  }

  /**
   * Completes the given future with the deserialized broker response. Rejections and errors are
   * converted into the exceptions the gateway maps to gRPC statuses, mirroring the broker's
   * {@code BrokerRequestManager#handleResponse}.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void completeWithBrokerResponse(
      final CompletableFuture future, final BrokerResponse<?> response) {
    if (response.isResponse()) {
      future.complete(response);
    } else if (response.isRejection()) {
      future.completeExceptionally(new BrokerRejectionException(response.getRejection()));
    } else if (response.isError()) {
      future.completeExceptionally(new BrokerErrorException(response.getError()));
    } else {
      future.completeExceptionally(
          new IllegalBrokerResponseException(
              "Expected broker response to be either response, rejection, or error, but is neither of them"));
    }
  }

  private <T> CompletableFuture<BrokerResponse<T>> sendRequestInternal(
      final BrokerRequest<T> request, final Duration requestTimeout) {
    if (closed) {
      return CompletableFuture.failedFuture(new IllegalStateException("Broker client closed"));
    }

    if (request instanceof final BrokerExecuteQuery queryRequest) {
      return handleQuery(queryRequest);
    }

    if (request instanceof final BrokerExecuteCommand<?> command) {
      return handleCommand(command, requestTimeout);
    }

    return CompletableFuture.failedFuture(
        new UnsupportedOperationException("Unsupported request type: " + request.getType()));
  }

  @SuppressWarnings("unchecked")
  private <T> CompletableFuture<BrokerResponse<T>> handleCommand(
      final BrokerExecuteCommand<?> command, final Duration requestTimeout) {
    final var future = new CompletableFuture<BrokerResponse<T>>();

    if (command.requiresPartitionId() && !command.addressesSpecificPartition()) {
      command.setPartitionId(topologyManager.getTopology().getPartition(0));
    }

    final long requestId = requestIdGenerator.incrementAndGet();
    pendingRequests.put(
        requestId,
        new PendingRequest(
            command,
            // the future is single-use, so erasing its response type is safe
            (CompletableFuture<BrokerResponse<?>>) (CompletableFuture<?>) future));

    if (requestTimeout != null) {
      future.orTimeout(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    final var metadata =
        new RecordMetadata()
            .protocolVersion(Protocol.PROTOCOL_VERSION)
            .requestId(requestId)
            .requestStreamId(REQUEST_STREAM_ID)
            .recordType(RecordType.COMMAND)
            .intent(command.getIntent())
            .valueType(command.getValueType());

    final long key = command.getKey();
    final UnifiedRecordValue value = (UnifiedRecordValue) command.getRequestWriter();

    final LogAppendEntry appendEntry =
        key == LogEntryDescriptor.KEY_NULL_VALUE
            ? LogAppendEntry.of(metadata, value)
            : LogAppendEntry.of(key, metadata, value);

    final var writeResult = logStreamWriter.tryWrite(appendEntry);
    if (writeResult.isLeft()) {
      pendingRequests.remove(requestId);
      future.completeExceptionally(
          new IllegalStateException(
              "Failed to write command %s to the log stream: %s"
                  .formatted(command.getType(), writeResult.getLeft())));
    }

    return future;
  }

  @SuppressWarnings("unchecked")
  private <T> CompletableFuture<BrokerResponse<T>> handleQuery(final BrokerExecuteQuery request) {
    final var future = new CompletableFuture<BrokerResponse<T>>();
    if (queryService == null) {
      future.completeExceptionally(new UnsupportedOperationException("Query API is not available"));
      return future;
    }

    try {
      final var requestBuffer = new UnsafeBuffer(new byte[request.getLength()]);
      request.write(requestBuffer, 0);

      final var queryRequest = new ExecuteQueryRequest();
      queryRequest.wrap(requestBuffer, 0, requestBuffer.capacity());
      final long key = queryRequest.getKey();
      final ValueType valueType = queryRequest.getValueType();

      final Optional<DirectBuffer> bpmnProcessId;
      switch (valueType) {
        case PROCESS -> bpmnProcessId = queryService.getBpmnProcessIdForProcess(key);
        case PROCESS_INSTANCE ->
            bpmnProcessId = queryService.getBpmnProcessIdForProcessInstance(key);
        case JOB -> bpmnProcessId = queryService.getBpmnProcessIdForJob(key);
        default -> {
          future.completeExceptionally(
              new UnsupportedOperationException("Unsupported query value type: " + valueType));
          return future;
        }
      }

      final UnsafeBuffer responseBuffer;
      if (bpmnProcessId.isEmpty()) {
        final var errorResponse =
            new ErrorResponse()
                .setErrorCode(ErrorCode.PROCESS_NOT_FOUND)
                .setErrorData(BufferUtil.wrapString("No resource found for key %d".formatted(key)));
        responseBuffer = new UnsafeBuffer(new byte[errorResponse.getLength()]);
        errorResponse.write(responseBuffer, 0);
      } else {
        final var queryResponse = new ExecuteQueryResponse().setBpmnProcessId(bpmnProcessId.get());
        responseBuffer = new UnsafeBuffer(new byte[queryResponse.getLength()]);
        queryResponse.write(responseBuffer, 0);
      }
      completeWithBrokerResponse(future, request.getResponse(responseBuffer));
    } catch (final Throwable t) {
      future.completeExceptionally(t);
    }
    return future;
  }
}
