/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.lite;

import io.camunda.zeebe.protocol.impl.encoding.ExecuteCommandResponse;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.stream.api.CommandResponseWriter;
import io.camunda.zeebe.util.buffer.BufferWriter;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * In-process replacement for the broker's {@code CommandResponseWriterImpl}.
 *
 * <p>Instead of writing the response to a server output (gRPC stream), it serializes the exact
 * same {@code ExecuteCommandResponse} SBE message (message header + response block) and hands it
 * to the {@link InProcessBrokerClient}, which completes the pending request future of the
 * gateway. The gateway then deserializes it exactly as it would over the wire.
 *
 * <p>This writer is only ever invoked from the stream processor's actor thread, so no internal
 * synchronization is needed (the same model as the broker implementation).
 */
public final class InMemoryCommandResponseWriter implements CommandResponseWriter {

  private final InProcessBrokerClient client;
  private final ExecuteCommandResponse response = new ExecuteCommandResponse();

  public InMemoryCommandResponseWriter(final InProcessBrokerClient client) {
    this.client = client;
  }

  @Override
  public CommandResponseWriter partitionId(final int partitionId) {
    response.setPartitionId(partitionId);
    return this;
  }

  @Override
  public CommandResponseWriter key(final long key) {
    response.setKey(key);
    return this;
  }

  @Override
  public CommandResponseWriter intent(final Intent intent) {
    response.setIntent(intent);
    return this;
  }

  @Override
  public CommandResponseWriter recordType(final RecordType recordType) {
    response.setRecordType(recordType);
    return this;
  }

  @Override
  public CommandResponseWriter valueType(final ValueType valueType) {
    response.setValueType(valueType);
    return this;
  }

  @Override
  public CommandResponseWriter rejectionType(final RejectionType rejectionType) {
    response.setRejectionType(rejectionType);
    return this;
  }

  @Override
  public CommandResponseWriter rejectionReason(final DirectBuffer rejectionReason) {
    response.setRejectionReason(rejectionReason, 0, rejectionReason.capacity());
    return this;
  }

  @Override
  public CommandResponseWriter valueWriter(final BufferWriter writer) {
    final int length = writer.getLength();
    final var buffer = new UnsafeBuffer(new byte[length]);
    if (length > 0) {
      writer.write(buffer, 0);
    }
    response.setValue(buffer, 0, length);
    return this;
  }

  @Override
  public void tryWriteResponse(final int requestStreamId, final long requestId) {
    final var buffer = new UnsafeBuffer(new byte[response.getLength()]);
    response.write(buffer, 0);
    client.onResponse(requestId, buffer);
    response.reset();
  }
}
