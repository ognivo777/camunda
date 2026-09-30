/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.lite;

import io.camunda.zeebe.engine.processing.streamprocessor.JobStreamer;
import io.camunda.zeebe.protocol.impl.stream.job.ActivatedJob;
import io.camunda.zeebe.protocol.impl.stream.job.JobActivationProperties;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.agrona.DirectBuffer;

/**
 * In-process replacement for the broker's {@code RemoteJobStreamer}.
 *
 * <ul>
 *   <li>{@link #streamFor} resolves registered {@code StreamJobs} client streams (via {@link
 *       InMemoryClientStreamer}) so the engine can push activated jobs directly to them.
 *   <li>{@link #notifyWorkAvailable} wakes up long-polling job workers by notifying the {@link
 *       InProcessBrokerClient} on the {@code jobsAvailable} topic — the same topic the gateway's
 *       long-polling handler subscribes to (replacing the atomix cluster event broadcast).
 * </ul>
 */
public final class InMemoryJobStreamer implements JobStreamer {

  private final InMemoryClientStreamer clientStreamer;
  private final Consumer<String> workAvailableListener;

  public InMemoryJobStreamer(
      final InMemoryClientStreamer clientStreamer, final Consumer<String> workAvailableListener) {
    this.clientStreamer = clientStreamer;
    this.workAvailableListener = workAvailableListener;
  }

  @Override
  public Optional<JobStream> streamFor(
      final DirectBuffer jobType, final Predicate<JobActivationProperties> filter) {
    return clientStreamer.findStream(jobType, filter).map(entry -> new JobStream() {
      @Override
      public JobActivationProperties properties() {
        return entry.metadata();
      }

      @Override
      public void push(final ActivatedJob payload) {
        clientStreamer.push(entry, payload);
      }
    });
  }

  @Override
  public void notifyWorkAvailable(final String jobType) {
    workAvailableListener.accept(jobType);
  }
}
