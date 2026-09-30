/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.lite;

import io.camunda.zeebe.protocol.impl.stream.job.ActivatedJob;
import io.camunda.zeebe.protocol.impl.stream.job.JobActivationProperties;
import io.camunda.zeebe.scheduler.future.ActorFuture;
import io.camunda.zeebe.scheduler.future.CompletableActorFuture;
import io.camunda.zeebe.transport.stream.api.ClientStreamConsumer;
import io.camunda.zeebe.transport.stream.api.ClientStreamId;
import io.camunda.zeebe.transport.stream.api.ClientStreamer;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * In-memory {@link ClientStreamer} for the gateway's {@code StreamJobs} RPC. Registered job
 * streams are kept in a map; {@link InMemoryJobStreamer} looks them up by job type and pushes
 * serialized {@link ActivatedJob} payloads straight into the registered consumer.
 */
public final class InMemoryClientStreamer implements ClientStreamer<JobActivationProperties> {

  private final Map<Long, StreamEntry> streams = new ConcurrentHashMap<>();
  private final AtomicLong streamIdGenerator = new AtomicLong(0);

  public record StreamEntry(
      long streamId,
      String jobType,
      JobActivationProperties metadata,
      ClientStreamConsumer consumer) {}

  public record StreamId(long id) implements ClientStreamId {}

  @Override
  public ActorFuture<ClientStreamId> add(
      final DirectBuffer streamType,
      final JobActivationProperties metadata,
      final ClientStreamConsumer consumer) {
    final var streamId = new StreamId(streamIdGenerator.incrementAndGet());
    streams.put(
        streamId.id(),
        new StreamEntry(streamId.id(), BufferUtil.bufferAsString(streamType), metadata, consumer));
    return CompletableActorFuture.completed(streamId);
  }

  @Override
  public ActorFuture<Void> remove(final ClientStreamId streamId) {
    if (streamId instanceof final StreamId id) {
      streams.remove(id.id());
    }
    return CompletableActorFuture.<Void>completed(null);
  }

  @Override
  public void close() {
    streams.clear();
  }

  /**
   * Finds the first registered stream whose job type matches and whose activation properties
   * satisfy the given filter.
   */
  public Optional<StreamEntry> findStream(
      final DirectBuffer jobType, final Predicate<JobActivationProperties> filter) {
    final String type = BufferUtil.bufferAsString(jobType);
    return streams.values().stream()
        .filter(entry -> entry.jobType().equals(type))
        .filter(entry -> filter.test(entry.metadata()))
        .findFirst();
  }

  /**
   * Serializes the given activated job (msgpack, exactly like the broker's remote stream push)
   * and pushes it to the stream's consumer.
   */
  public void push(final StreamEntry entry, final ActivatedJob job) {
    final var payload = new UnsafeBuffer(new byte[job.getLength()]);
    job.write(payload, 0);
    entry.consumer().push(payload);
  }
}
