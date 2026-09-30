/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.lite;

import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.stream.api.InterPartitionCommandSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A no-op {@link InterPartitionCommandSender} for the single-partition lite deployment. With only
 * one partition, no command should ever need to be sent to another partition; if one is
 * attempted, it is dropped with a warning instead of failing.
 */
public final class NoOpInterPartitionCommandSender implements InterPartitionCommandSender {

  private static final Logger LOG = LoggerFactory.getLogger(NoOpInterPartitionCommandSender.class);

  @Override
  public void sendCommand(
      final int receiverPartitionId,
      final ValueType valueType,
      final Intent intent,
      final UnifiedRecordValue command) {
    LOG.warn(
        "Dropped inter-partition command {}#{} addressed to partition {}; the lite deployment is single-partition",
        valueType,
        intent,
        receiverPartitionId);
  }

  @Override
  public void sendCommand(
      final int receiverPartitionId,
      final ValueType valueType,
      final Intent intent,
      final Long recordKey,
      final UnifiedRecordValue command) {
    sendCommand(receiverPartitionId, valueType, intent, command);
  }
}
