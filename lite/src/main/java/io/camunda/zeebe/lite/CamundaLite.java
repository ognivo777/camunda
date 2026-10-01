/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.lite;

import io.camunda.zeebe.db.ConsistencyChecksSettings;
import io.camunda.zeebe.db.impl.rocksdb.RocksDbConfiguration;
import io.camunda.zeebe.db.impl.rocksdb.ZeebeRocksDbFactory;
import io.camunda.zeebe.engine.Engine;
import io.camunda.zeebe.engine.EngineConfiguration;
import io.camunda.zeebe.engine.processing.EngineProcessors;
import io.camunda.zeebe.engine.processing.message.command.SubscriptionCommandSender;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessorFactory;
import io.camunda.zeebe.engine.state.query.StateQueryService;
import io.camunda.zeebe.gateway.Gateway;
import io.camunda.zeebe.gateway.impl.configuration.GatewayCfg;
import io.camunda.zeebe.logstreams.log.LogStream;
import io.camunda.zeebe.logstreams.log.LogStreamWriter;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import io.camunda.zeebe.protocol.record.intent.MessageIntent;
import io.camunda.zeebe.protocol.record.intent.TimerIntent;
import io.camunda.zeebe.scheduler.ActorScheduler;
import io.camunda.zeebe.stream.impl.StreamProcessor;
import io.camunda.zeebe.stream.impl.StreamProcessorMode;
import io.camunda.zeebe.util.FeatureFlags;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launcher for a single-node, local Camunda (Zeebe) BPMN executor.
 *
 * <p>Instead of booting a full broker (partition manager, raft, atomix cluster, exporters, ...),
 * this wires the production {@link StreamProcessor} + workflow {@link Engine} directly to an
 * official gRPC {@link Gateway}, exactly like the engine's {@code TestStreams} test infrastructure
 * does:
 *
 * <ul>
 *   <li>commands from the gateway are appended to the log stream in-process ({@link
 *       InProcessBrokerClient}),
 *   <li>engine command responses are handed back to the gateway in-process ({@link
 *       InMemoryCommandResponseWriter}),
 *   <li>job availability notifications are routed in-process ({@link InMemoryJobStreamer}),
 *   <li>the topology is fixed to a single broker/partition ({@link StaticBrokerTopologyManager}).
 * </ul>
 *
 * <p>State is kept in a RocksDB database in the local data directory; the log itself is in
 * memory. Standard {@code ZeebeClient} workers (Java, Go, ...) work unchanged against the gRPC
 * port.
 */
public final class CamundaLite {

  private static final Logger LOG = LoggerFactory.getLogger(CamundaLite.class);

  public static final int NODE_ID = 1;
  public static final int PARTITION_ID = 1;
  public static final int PARTITION_COUNT = 1;
  public static final int DEFAULT_GATEWAY_PORT = 26500;
  public static final Path DEFAULT_DATA_DIR = Path.of("data");
  private static final int STARTUP_TIMEOUT_SECONDS = 60;

  private CamundaLite() {}

  /**
   * A running Camunda Lite runtime. Close it (in reverse order of creation) to stop the gateway,
   * stream processor, log stream, database and actor scheduler.
   */
  public static final class CamundaLiteRuntime implements AutoCloseable {
    private final List<AutoCloseable> closeables;
    private final ActorScheduler scheduler;

    CamundaLiteRuntime(final List<AutoCloseable> closeables, final ActorScheduler scheduler) {
      this.closeables = closeables;
      this.scheduler = scheduler;
    }

    @Override
    public void close() {
      LOG.info("Shutting down Camunda Lite...");
      for (int i = closeables.size() - 1; i >= 0; i--) {
        try {
          closeables.get(i).close();
        } catch (final Exception e) {
          LOG.warn("Error while closing {}", closeables.get(i), e);
        }
      }
      try {
        scheduler.close();
      } catch (final Exception e) {
        LOG.warn("Error while closing the actor scheduler", e);
      }
    }
  }

  /**
   * Boots the full runtime: actor scheduler, in-memory log, RocksDB state, stream processor,
   * engine and gRPC gateway. Blocks until the gateway is accepting requests.
   */
  public static CamundaLiteRuntime start(final int gatewayPort, final Path dataDir) {
    LOG.info(
        "Starting Camunda Lite (gateway gRPC port: {}, data directory: {})",
        gatewayPort,
        dataDir.toAbsolutePath());

    // 1. the actor scheduler drives all actors (log stream, stream processor, gateway)
    final var scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();

    final List<AutoCloseable> closeables = new ArrayList<>();
    try {
      // 2. in-memory log storage and the partition's log stream
      final var logStorage = new InMemoryLogStorage();
      final var logStream =
          LogStream.builder()
              .withLogName("lite-partition-" + PARTITION_ID)
              .withLogStorage(logStorage)
              .withPartitionId(PARTITION_ID)
              .withNodeId(NODE_ID)
              .withActorSchedulingService(scheduler)
              .buildAsync()
              .toCompletableFuture()
              .get(STARTUP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      closeables.add(logStream);

      // separate writer for commands coming from the gateway (like the broker's command API)
      final LogStreamWriter commandLogWriter =
          logStream
              .newLogStreamWriter()
              .toCompletableFuture()
              .get(STARTUP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      // 3. engine state in RocksDB inside the local data directory.
      // The log stream is in-memory, so it is gone after a restart. Reusing an old data
      // directory would leave a stale "last processed position" in RocksDB that no longer
      // exists in the (empty) in-memory log, breaking snapshot recovery. v1 has no restart
      // persistence, so every start begins from a clean data directory.
      startFromCleanDataDir(dataDir);

      final var zeebeDb =
          new ZeebeRocksDbFactory<ZbColumnFamilies>(
                  new RocksDbConfiguration(), new ConsistencyChecksSettings())
              .createDb(dataDir.toFile());
      closeables.add(zeebeDb);

      final var queryService = new StateQueryService(zeebeDb);

      // 4. in-process replacement of the networked broker client / topology
      final var topologyManager = new StaticBrokerTopologyManager(NODE_ID, PARTITION_COUNT);
      final var clientStreamer = new InMemoryClientStreamer();
      final var brokerClient =
          new InProcessBrokerClient(commandLogWriter, topologyManager, queryService);
      closeables.add(brokerClient);

      // 5. engine -> gateway job availability routing (long polling + StreamJobs)
      final var jobStreamer =
          new InMemoryJobStreamer(clientStreamer, brokerClient::notifyJobAvailable);

      // 6. the unmodified workflow engine
      final var noOpSender = new NoOpInterPartitionCommandSender();
      final TypedRecordProcessorFactory processorFactory =
          ctx ->
              EngineProcessors.createEngineProcessors(
                  ctx,
                  PARTITION_COUNT,
                  new SubscriptionCommandSender(PARTITION_ID, noOpSender),
                  noOpSender,
                  FeatureFlags.createDefault(),
                  jobStreamer);
      final var engine = new Engine(processorFactory, new EngineConfiguration());

      // 7. engine -> gateway command response routing
      final var commandResponseWriter = new InMemoryCommandResponseWriter(brokerClient);

      // 8. the stream processor drives the engine over the log stream
      // a real (not no-op) scheduled-command cache, like the stock broker: it dedupes the
      // timer triggers, job timeouts, job backoffs and message expiries that the engine's
      // periodic checkers schedule
      final var scheduledCommandCache =
          new InMemoryScheduledCommandCache(
              TimerIntent.TRIGGER,
              JobIntent.TIME_OUT,
              JobIntent.RECUR_AFTER_BACKOFF,
              MessageIntent.EXPIRE);
      final var streamProcessor =
          StreamProcessor.builder()
              .logStream(logStream)
              .actorSchedulingService(scheduler)
              .zeebeDb(zeebeDb)
              .recordProcessors(List.of(engine))
              .commandResponseWriter(commandResponseWriter)
              .streamProcessorMode(StreamProcessorMode.PROCESSING)
              .partitionCommandSender(noOpSender)
              .maxCommandsInBatch(16)
              .nodeId(NODE_ID)
              .scheduledCommandCache(scheduledCommandCache)
              .build();
      closeables.add(streamProcessor);
      streamProcessor
          .openAsync(false)
          .toCompletableFuture()
          .get(STARTUP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      // 9. the official gRPC gateway (deploy, instances, jobs, queries, long polling)
      final var gatewayCfg = new GatewayCfg();
      gatewayCfg.init();
      gatewayCfg.getNetwork().setPort(gatewayPort);
      final var gateway = new Gateway(gatewayCfg, brokerClient, scheduler, clientStreamer);
      closeables.add(gateway);
      gateway.start().toCompletableFuture().get(STARTUP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      return new CamundaLiteRuntime(closeables, scheduler);
    } catch (final Exception e) {
      LOG.error("Failed to start Camunda Lite", e);
      closeAll(closeables, scheduler);
      throw new IllegalStateException("Failed to start Camunda Lite", e);
    }
  }

  public static void main(final String[] args) {
    final int port = intOption(args, "--port", DEFAULT_GATEWAY_PORT);
    final String dataDirArg = findOption(args, "--data-dir");
    final Path dataDir = Path.of(dataDirArg != null ? dataDirArg : DEFAULT_DATA_DIR.toString());

    final var runtime = start(port, dataDir);
    LOG.info("Camunda Lite is up and running.");
    LOG.info("Point your ZeebeClient at grpc://localhost:{} (gateway protocol port)", port);
    LOG.info("Ctrl+C stops the engine.");

    Runtime.getRuntime()
        .addShutdownHook(new Thread(runtime::close, "camunda-lite-shutdown"));
  }

  /**
   * Deletes any previous engine state in the data directory so the run starts from a clean
   * slate. v1 keeps the log in memory, so there is nothing meaningful to carry over across
   * restarts; reusing an old RocksDB state would break snapshot recovery (see step 3).
   */
  private static void startFromCleanDataDir(final Path dataDir) {
    if (!Files.exists(dataDir)) {
      return;
    }
    LOG.warn(
        "Removing existing data directory {} (v1 has no restart persistence; starting clean).",
        dataDir.toAbsolutePath());
    try (final var paths = Files.walk(dataDir)) {
      paths
          .sorted(Comparator.reverseOrder()) // children before parents
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (final IOException e) {
                  throw new UncheckedIOException(
                      "Could not delete "
                          + path
                          + " - the file is still locked by another process. If a previous"
                          + " Camunda Lite instance is still running, stop it first, or start"
                          + " with a fresh --data-dir.",
                      e);
                }
              });
    } catch (final IOException e) {
      throw new UncheckedIOException("Could not clear data directory " + dataDir, e);
    }
  }

  private static void closeAll(final List<AutoCloseable> closeables, final ActorScheduler scheduler) {
    for (int i = closeables.size() - 1; i >= 0; i--) {
      try {
        closeables.get(i).close();
      } catch (final Exception e) {
        LOG.warn("Error while closing {}", closeables.get(i), e);
      }
    }
    try {
      scheduler.close();
    } catch (final Exception e) {
      LOG.warn("Error while closing the actor scheduler", e);
    }
  }

  private static int intOption(final String[] args, final String name, final int defaultValue) {
    final String value = findOption(args, name);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Invalid value for %s: %s".formatted(name, value), e);
    }
  }

  private static String findOption(final String[] args, final String name) {
    for (int i = 0; i < args.length - 1; i++) {
      if (name.equals(args[i])) {
        return args[i + 1];
      }
    }
    return null;
  }
}
