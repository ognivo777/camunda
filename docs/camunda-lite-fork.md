# Camunda Lite Fork — Task Document

A single-node, local BPMN executor forked from the Zeebe/Camunda monorepo.

- **Keep:** full BPMN/DMN/FEEL engine semantics, official gRPC gateway + official job workers (Java/Go clients unchanged).
- **Drop:** cluster scaling, Raft replication, multi-partition routing, exporters (ES/OS), backup/restore, admin/backup APIs.
- **Persistence v1:** in-memory log; local RocksDB state directory (state may survive restart of the DB files, but not guaranteed — durable snapshots are phase 5).

This document is written to be executed **twice**:

| Part | Target | Notes |
|---|---|---|
| **A** | Current checkout: `L:\Sources\camunda` at **8.4.12** (`zeebe-parent`) | All class/module names below are verified against this tree. |
| **B** | A **fresh Camunda source checkout** (latest release / `main`) | Re-derive the seams; Part B lists what changed and how to re-map. |

---

## Architecture (why this works)

The Zeebe engine (`engine/`) is a pure record-driven state machine with **no knowledge of clustering**.
All distribution machinery plugs in through four small interfaces. The fork replaces each with a tiny local implementation:

| Seam (interface) | Production impl (dormant/dropped) | Lite impl (new) | Module |
|---|---|---|---|
| `io.camunda.zeebe.logstreams.storage.LogStorage` | `broker/.../AtomixLogStorage` (Raft journal) | `InMemoryLogStorage` | `lite` |
| `io.camunda.zeebe.stream.api.CommandResponseWriter` | `broker/.../CommandResponseWriterImpl` (CLQ) | `InMemoryCommandResponseWriter` | `lite` |
| `io.camunda.zeebe.stream.api.InterPartitionCommandSender` | `broker/.../InterPartitionCommandSenderService` (CLQ) | `NoOpInterPartitionCommandSender` | `lite` |
| `io.camunda.zeebe.gateway.impl.broker.BrokerClient` | `BrokerClientImpl` + `BrokerRequestManager` (atomix messaging) | `InProcessBrokerClient` | `lite` / `gateway` |
| `...gateway.impl.broker.cluster.BrokerTopologyManager` | `BrokerTopologyManagerImpl` (atomix membership) | `StaticBrokerTopologyManager` (1 node / 1 partition) | `lite` |

Reference wiring already in the tree (do **not** reinvent — copy the structure):
- `engine/src/test/java/io/camunda/zeebe/engine/util/TestStreams.java` — `createLogContext`, `buildStreamProcessor`: boots `LogStream` + `ZeebeDb` + `StreamProcessor` + `Engine` without any broker.
- `engine/src/main/java/io/camunda/zeebe/engine/processing/EngineProcessors.java` — `createEngineProcessors(TypedRecordProcessorContext, partitionsCount, SubscriptionCommandSender, InterPartitionCommandSender, FeatureFlags, JobStreamer)`.
- `broker/src/main/java/io/camunda/zeebe/broker/system/partitions/impl/steps/StreamProcessorTransitionStep.java` — `createStreamProcessor(...)`: the exact `StreamProcessor.builder()` call list.
- `broker/src/main/java/io/camunda/zeebe/broker/transport/commandapi/CommandApiServiceImpl.java` — how gateway requests become log records, and how `CommandResponseWriter` responses are addressed by `(requestStreamId, requestId)`.

Key mechanism for command/response correlation:
- Each gateway→broker request is tagged with `requestStreamId` + `requestId` (record metadata).
- The engine writes a response via `CommandResponseWriter.tryWriteResponse(requestStreamId, requestId)`.
- The lite `InMemoryCommandResponseWriter` holds a `Map<StreamId, Map<Long, CompletableFuture<T>>>` of pending requests and completes them on `tryWriteResponse`.

---

## Part A — Execute on current checkout (8.4.12)

### A1. Branch / worktree — DONE

- [x] Create worktree + branch, e.g. `git worktree add ../camunda-lite -b lite L:\Sources\camunda` (or plain branch `lite`). (Done: in-repo branch `lite-camunda`, no worktree.)
- [x] All further tasks in Part A run in that worktree.
- [x] Record base commit in `lite/README.md`. (`5a537e202c7911c54541d17957bcbd93fe29dc8f`, `8.4.12-1-g5a537e202c`.)

### A2. Trim the module list — DONE

Edit root `pom.xml` `<modules>`. Current tree has 45 modules.

**Removed from the build:**

- [x] `broker` (whole module — Raft partitioning, CLQ server transport, admin API, snapshots, exporter director; nothing we keep needs it)
- [x] `dist`, `qa`, `benchmarks/project`
- [x] `exporter-api`, `exporter-test`, `exporters/elasticsearch-exporter`, `exporters/opensearch-exporter`
- [x] `backup`, `backup-stores/testkit`, `backup-stores/s3`, `backup-stores/gcs`, `backup-stores/azure`
- [x] `restore`

> **Decision (final):** **keep `transport`, `atomix`, `journal`, `topology`, `protocol-asserts`** as unused classpath deps. Reason: `gateway`'s `BrokerRequest` implements `io.camunda.zeebe.transport.ClientRequest`, the `Gateway` constructor takes a `ClientStreamer<JobActivationProperties>`, and the experimental job-stream API (`gateway/impl/stream/*`) is transport-based. Keeping them (and therefore `atomix` cluster/utils/raft) means **zero structural changes** to the gateway. Clustering code is simply never instantiated.
> If a lighter classpath matters later, see **A7 (optional slim-down)** for removing `transport`/`atomix` for real.

**Keep:** `bom`, `parent`, `build-tools`, `util`, `scheduler`, `protocol`, `protocol-impl`, `protocol-jackson`, `msgpack-core`, `msgpack-value`, `bpmn-model`, `logstreams`, `stream-platform`, `engine`, `zb-db`, `expression-language`, `feel`, `dmn`, `gateway-protocol`, `gateway-protocol-impl`, `gateway` (trimmed), `clients/java`, `auth`, `test-util`, `snapshot` (harmless; needed for phase 5), `transport`, `atomix`, `journal`, `topology`, `protocol-asserts` (see decision above). Plus the new **`lite`** module.

- [x] Verify: `mvn -Dquickly=true -DskipTests -pl lite -am install` is green (trimmed reactor = 33 modules; shaded `lite/target/camunda-lite-8.4.12.jar` produced).

### A3. Trim the gateway module — DONE

- [x] Delete `gateway/src/main/java/io/camunda/zeebe/gateway/impl/broker/BrokerClientImpl.java`.
- [x] Delete `gateway/src/main/java/io/camunda/zeebe/gateway/impl/broker/BrokerRequestManager.java`.
- [x] Delete `gateway/src/main/java/io/camunda/zeebe/gateway/impl/broker/cluster/BrokerTopologyManagerImpl.java` (keep the `BrokerTopologyManager` interface).
- [x] Also deleted 5 gateway tests that depended on the deleted classes: `BrokerClientTest`, `InterceptorIT`, `SecurityTest`, `TopologyUpdateTest`, `UnavailableBrokersTest`.
- [x] Fix all compile breakages caused by the deletions:
  - [x] Any factory/builder that constructed `BrokerClientImpl` (e.g. in `gateway/src/test/...` or bootstrap helpers) now constructs `InProcessBrokerClient` (A4). (In practice only the test classes above referenced them; `Gateway` itself takes the interface.)
  - [x] `LongPollingActivateJobsHandler` / `RoundRobinActivateJobsHandler` only consume `BrokerClient` + `BrokerTopologyManager` — no changes expected. (Confirmed.)
- [x] `Gateway.java` constructor takes `ClientStreamer<JobActivationProperties> jobStreamer`:
  - [x] Inspect `Gateway.start()`/`GatewayImplBase` for the stream-jobs wiring (`StreamJobsHandler`).
  - [x] Option 1 (preferred): pass a no-op / `null`-tolerated streamer... → **Option 2 variant**: wired the real stream API with `InMemoryClientStreamer` (tiny impl of `ClientStreamer`) + `InMemoryJobStreamer`, so both long polling and (experimental) stream jobs work. The 8.4 Java client uses **long-polling `ActivateJobs` by default** — that is the tested path.
- [x] Identity: keep `auth` wiring; configure gateway to **anonymous** mode (`SecurityCfg.AuthMode.ANONYMOUS` / `IdentityCfg` disabled) so no ID token is required locally. Verify the gRPC interceptor chain (`IdentityInterceptor`) is skipped in anonymous mode. (Default `GatewayCfg` auth mode is `NONE` — verified working e2e with a plaintext, unauthenticated client.)
- [x] `mvn -q -pl gateway -am -DskipTests compile` green.

### A4. New `lite` module (the launcher) — DONE

Create module `lite` (artifactId e.g. `camunda-lite`), parent `zeebe-parent`.
Dependencies: `zeebe-stream-platform`, `zeebe-engine`, `zeebe-logstreams`, `zeebe-zb-db`, `zeebe-scheduler`, `zeebe-protocol(-impl)`, `zeebe-gateway`, `zeebe-gateway-protocol-impl`, `zeebe-util`, `zeebe-auth` (transitively via gateway), `zeebe-client-java` (for the e2e test only, test scope).

New classes (all small; total ≈ 800–1200 lines):

- [x] **`InMemoryLogStorage implements LogStorage`**
  - Storage: `ConcurrentSkipListMap<Long, DirectBuffer>` keyed by `lowestPosition` (mirrors how `AtomixLogStorageReader` reads blocks).
  - `append(lowestPosition, highestPosition, bufferWriter, listener)`: copy bytes into an `UnsafeBuffer`, put in map, call `listener.onWrite(address)`, then immediately `listener.onCommit(address)` **and** notify all `CommitListener`s (no Raft ⇒ write == commit).
  - `newReader()`: iterate map entries ≥ seek position; `seek(position)` = ceiling/floor lookup per `LogStorageReader` contract (negative → first; beyond last → last block).
  - `addCommitListener` / `removeCommitListener`: `CopyOnWriteArraySet`.
  - Reference for reader contract: `AtomixLogStorageReader` and `logstreams/src/test/.../ListLogStorage.java` (test impl, same interface).
  - ⚠ Thread-safety: `LogStreamWriter`/`LogStreamReader` run on actor threads; the map must be read-writer safe. `append` is called from a single writer actor in practice, but readers (`LogStreamReader`, `LogStorageAppender`) read concurrently — use a synchronized append + concurrent reads.
- [x] **`InMemoryCommandResponseWriter implements CommandResponseWriter`**
  - Holds `Map<io.camunda.zeebe.protocol.impl.record.stream.StreamId, Map<Long /*requestId*/, CompletableFuture<...>>>`.
  - Builder-style setters chain onto a small mutable record; `tryWriteResponse(requestStreamId, requestId)` looks up the future and completes it with the assembled response (key, recordType, intent, valueType, rejectionType/reason, value buffer).
  - Reference: `CommandResponseWriterImpl` (broker/transport) + how `Engine` writes responses (`Engine.tryWriteRejection`/success paths use `getRejectionRecord`, `getRequestStreamId/getRequestId`).
- [x] **`NoOpInterPartitionCommandSender implements InterPartitionCommandSender`** — `send(...)` no-op (single partition; engine's `CommandDistributionBehavior`/`DeploymentDistributionCommandSender` become no-ops when `partitionsCount == 1` — pass `1` everywhere).
- [x] **`InProcessBrokerClient implements BrokerClient`**
  - Implements: `start()`, `close()`, `sendRequest(...)`, `sendRequestWithRetry(...)` (all overloads), `getTopologyManager()`, `subscribeJobAvailableNotification(...)`.
  - Request routing: for each `BrokerRequest` subclass, encode the same record the broker's `CommandApiServiceImpl` would write — port the small amount of logic from `BrokerRequestManager`/`BrokerExecuteCommand`: assign a fresh `requestStreamId` (e.g. incrementing `StreamId`) + `requestId`, build the command payload with the existing record encoders (`protocol-impl`), register a pending future in the `InMemoryCommandResponseWriter`, then `LogStreamWriter.append(...)` with metadata (partitionId=1, requestStreamId, requestId) via `RecordMetadataEncoder` + the command value writer.
  - Requests to handle (mirror `BrokerRequest` subclasses): `DeployResourceRequest`, `ProcessInstanceCreationRequest`, `ProcessInstanceCancellationRequest` (+with variables), `JobActivationRequest` (`ActivateJobsRequest`), `JobCompletionRequest` (`CompleteJobRequest`), `JobFailureRequest` (`FailJobRequest`), `MessagePublishRequest`, `DecisionEvaluationRequest` (if present in 8.4), `SetVariablesRequest` (if present), `TopologyRequest`, exporting/backup admin requests → answer directly with "unsupported" (they exist in the gRPC API surface but no exporters/backup exist).
  - `getTopologyManager()`: returns `StaticBrokerTopologyManager`.
  - `subscribeJobAvailableNotification(topic, handler)`: in-process pub/sub — a `Consumer<String>` set that the lite bootstrap triggers from the `StreamProcessor`'s `onProcessed` listener when a `Job CREATED`/activated event is observed (see `LongPollingActivateJobsHandler`'s use of the notification topic `JOBS_AVAILABLE_TOPIC`).
  - ⚠ Keep the actor-thread rules: record appending must happen on the log writer actor (use `LogStreamWriter` API which already hops threads).
- [x] **`StaticBrokerTopologyManager implements BrokerTopologyManager`**
  - Fixed topology: 1 broker (nodeId 0), 1 partition (partitionId 1, `LEADER`-equivalent status), `ClusterTopology` built with the `protocol`'s topology types (see `BrokerTopologyManagerImpl.getTopology()` for the shape).
- [x] **`CamundaLite` main class** (bootstrap, ≈150 lines), wiring in this exact order (mirror `TestStreams` + `StreamProcessorTransitionStep`):
  1. `ActorSchedulingService` (`ActorScheduler` with configurable thread count, default CPU count) — start it.
  2. `InMemoryLogStorage` → `LogStream.builder().withLogStorage(...).withActorSchedulingService(...).withPartitionId(1).withLogName("camunda-lite").build()` → start.
  3. `ZeebeDbFactory(1).create(dataDir)` — **8.4 has no in-memory factory**; use a local dir (e.g. `--data-dir` arg, default `./data`). (In-memory RocksDB is not required: this is a *local* executor, not a replica.)
  4. `ProcessingState.build(zeebeDb, 1)` + `Writers` + `ProcessingScheduleService` (see `TestStreams.createLogContext` for the exact construction) → `TypedRecordProcessorContext` (implement the 7-method interface from A's seam table or reuse a small adapter).
  5. `TypedRecordProcessorsFactory` → `TypedRecordProcessorFactory` via `EngineProcessors.createEngineProcessors(ctx, /*partitionsCount*/ 1, subscriptionCommandSender, new NoOpInterPartitionCommandSender(), FeatureFlags.createDefault(), /*jobStreamer*/ null)`; `new Engine(factory, new EngineConfiguration() /* defaults */)`.
     - ⚠ Check the `JobStreamer` parameter: if the engine requires a non-null `JobStreamer` for job activation (8.4 job streaming hook), pass a no-op implementation (`engine/.../streamprocessor/JobStreamer` is a tiny interface).
  6. `StreamProcessor.builder()` with the exact field list from `StreamProcessorTransitionStep.createStreamProcessor`: `logStream`, `actorSchedulingService`, `zeebeDb`, `recordProcessors(List.of(engine))`, `nodeId(0)`, `commandResponseWriter(inMemory)`, `maxCommandsInBatch(default)`, `setEnableAsyncScheduledTasks(true)`, `processingFilter(processingFilter.none() equivalent)`, `listener(onProcessed → job-available notification)`, `streamProcessorMode(PROCESSING)`, `partitionCommandSender(noop)`, `scheduledCommandCache(BoundedScheduledCommandCache for TimerIntent.TRIGGER, JobIntent.TIME_OUT/RECUR_AFTER_BACKOFF, MessageIntent.EXPIRE)` — start.
     - ⚠ `scheduledCommandCache` is built by the broker (`BoundedScheduledCommandCache`) — broker-only. **Resolved:** we implemented `InMemoryScheduledCommandCache` in `lite`: same semantics (intent-scoped, staged `add`/`contains`/`remove`/`persist`) but unbounded, which is fine for a single partition. Without a real cache (the builder's `NoopScheduledCommandCache` default), the engine's periodic checkers can append duplicate `TIMER_TRIGGER` / `JOB TIME_OUT` commands when they re-run before the previous command has been processed.
  7. `InProcessBrokerClient` (wired to the `LogStreamWriter` of the started `LogStream` + the `InMemoryCommandResponseWriter`) + `StaticBrokerTopologyManager`.
  8. `Gateway` (`GatewayCfg` from a small YAML/JSON or CLI args: gRPC port, `longPolling.enabled=true`, max message size, anonymous auth; `IdentityConfiguration` null/disabled), start gRPC server.
  9. Graceful shutdown hook: stop gateway → stream processor → log stream → actor scheduling service → close ZeebeDb.
- [x] Shaded jar: `maven-shade-plugin` in `lite/pom.xml` with main class `CamundaLite`; CLI args: `--port`, `--data-dir` (implemented; `--config` omitted in v1).

- [x] `mvn -Dquickly=true -DskipTests -pl lite -am install` produces a runnable jar (`lite/target/camunda-lite-8.4.12.jar`).

**Final implementation notes (8.4.12) — read before re-running Part B:**

- **Command path (no SBE round-trip).** Every gateway command request is a `BrokerExecuteCommand`; `command.getRequestWriter()` *is* the raw `protocol-impl` DTO (`UnifiedRecordValue`). Build `RecordMetadata` (protocolVersion, requestId, requestStreamId, recordType=COMMAND, intent, valueType) and append `LogAppendEntry.of(key, metadata, value)` via `LogStreamWriter.tryWrite(entry)`. No request serialization needed — the value DTO is written straight into the log.
- **Correlation.** `requestStreamId` is a fixed constant `1`; `requestId` comes from an `AtomicLong`. Pending requests live in a single `ConcurrentHashMap<Long, PendingRequest>` in `InProcessBrokerClient` (not per-`StreamId`). Timeouts via `CompletableFuture.orTimeout` (the `ActorSchedulingService` has only `submitActor` — no delay API needed).
- **⚠ Rejection/error classification (the one real bug found in e2e debugging).** In `InProcessBrokerClient.onResponse`, the deserialized `BrokerResponse` must be classified exactly like the stock `BrokerRequestManager.handleResponse`: `isResponse()` → complete normally; `isRejection()` → `completeExceptionally(new BrokerRejectionException(response.getRejection()))`; `isError()` → `completeExceptionally(new BrokerErrorException(response.getError()))`. Without this, an engine rejection (e.g. invalid BPMN) surfaces as a `BrokerRejectionResponse` whose DTO is `null`; the gateway's `EndpointManager.consumeResponse` then NPEs *inside* the `whenComplete` callback, the exception is silently swallowed, and the client hangs until its own deadline (DEADLINE_EXCEEDED) instead of getting the proper gRPC `INVALID_ARGUMENT`.
- **Query path.** `BrokerExecuteQuery` is answered in-process from the engine's `QueryService` (no log round-trip). Decode with the `protocol-impl` DTO `new ExecuteQueryRequest().wrap(buffer, 0, capacity)` (the SBE `ExecuteQueryRequestDecoder` 4-arg `wrap` does not fit). Not-found → `ErrorResponse` DTO with `ErrorCode.PROCESS_NOT_FOUND` (mirrors broker `QueryApiRequestHandler`). Response DTOs (`ExecuteCommandResponse`, `ExecuteQueryResponse`, `ErrorResponse`) write their own SBE message header in `write()`.
- **Job availability.** `InMemoryJobStreamer` delegates to (a) `InMemoryClientStreamer` — pushes raw msgpack `ActivatedJob` over the `StreamJobs` API — and (b) `InProcessBrokerClient::notifyJobAvailable` (topic `"jobsAvailable"`), which wakes the gateway's long-polling handler (replaces the atomix `ClusterEventService` broadcast).
- **Timers do fire.** The engine's periodic checkers (`DueDateTimerChecker`, `JobTimeoutCheckerScheduler`, `JobBackoffChecker`, `PendingProcessMessageSubscriptionChecker`) live in the `engine` module and register themselves as stream-processor lifecycle listeners through `EngineProcessors` → `Engine.init()`, so no extra wiring is needed. The only missing piece was the builder's `NoopScheduledCommandCache` default — `CamundaLite` now passes an `InMemoryScheduledCommandCache` for `TimerIntent.TRIGGER`, `JobIntent.TIME_OUT`, `JobIntent.RECUR_AFTER_BACKOFF`, `MessageIntent.EXPIRE` (the same intents as the stock broker). Timer boundary/catch/start events, job timeouts, retry backoff and message expiry all work; verified e2e by `LiteEndToEndTest.timerCatchEventFiresAndFinishesInstance`. Bonus: timer state lives in RocksDB and the checkers rescan it in `onRecovered`, so timers survive restarts better than in-memory log commands.
- **`EngineProcessors.createEngineProcessors(...)`** takes a `JobStreamer`; pass `InMemoryJobStreamer` (non-null is required).
- **RocksDB:** `ZeebeRocksDbFactory<ZbColumnFamilies>(new RocksDbConfiguration(), new ConsistencyChecksSettings()).createDb(File)` — 8.4 has no in-memory factory.

### A5. End-to-end verification (the acceptance test) — DONE

New test (in `lite/src/test` or a small `qa-lite` test target) using the **official Java client** (`zeebe-client-java`) against the in-process/localhost gateway:

- [x] `CamundaLite.start()` on an ephemeral port (reused bootstrap as a testable `CamundaLiteRuntime` class; test grabs a free port via `new ServerSocket(0)`).
- [x] Deploy `process.bpmn`: start → service task (`zeebe:taskDefinition` type `lite-test-job`) → end.
- [x] `ZeebeClient` (gRPC, anonymous, plaintext): `newDeployCommand().addResourceBytes(xml, "name.bpmn")`.
- [x] `newCreateInstanceCommand().bpmnProcessId(...).latestVersion().withResult().send()`.
- [x] `newWorker().jobType("lite-test-job").handler((jobClient, job) -> jobClient.newCompleteCommand(job).variables(Map.of("done", true)).send()).timeout(Duration).open()` — **this is the "worker support" proof**.
- [x] Assert: instance completes with the expected variables (`ProcessInstanceResult.getVariablesAsMap()` contains `done=true`), and the process instance key is positive.
- [x] Timer catch event: `LiteEndToEndTest.timerCatchEventFiresAndFinishesInstance` deploys start → service task → `PT2S` timer catch → end, completes the job, and asserts the instance only finishes ≥ 2s later (i.e. it really waited for the timer).
- [ ] Also verify: job `fail` path with retries decrement; message publish/correlation (1 service task on message) — optional but cheap. (Skipped in v1.)
- [ ] Assert topology query returns 1 broker / 1 partition. (Skipped in v1; the fixed topology is exercised implicitly by every request.)

> **8.4 client API spellings (verified in-tree — older examples online use different names):**
> - Deploy: `client.newDeployCommand().addResourceBytes(byte[], String)` (or `addProcessModel(BpmnModelInstance, String)`).
> - Instance with result: `client.newCreateInstanceCommand().bpmnProcessId(id).latestVersion().withResult().send()` → `ZeebeFuture<ProcessInstanceResult>` (package `io.camunda.zeebe.client.api.response`).
> - Worker: `client.newWorker().jobType(t).handler(handler).timeout(Duration).open()` (`timeout` is a step-3 method — call it *after* `handler`); `JobHandler.handle(JobClient, ActivatedJob)`.
> - Complete: `jobClient.newCompleteCommand(job).variables(Map<String, Object>).send()`.
> - **BPMN (8.4 extension):** the task definition uses a **`type` attribute**: `<zeebe:taskDefinition type="my-job"/>`. (Newer Zeebe versions use a `<zeebe:taskType>` child element instead — re-verify against the fresh tree's `ZeebeTaskDefinitionImpl` in Part B.)
>
> **Build note:** the parent pom binds `skipTests` to `${quickly}`, so running tests requires an explicit `-DskipTests=false`: `.\mvnw.cmd -Dquickly=true -DskipTests=false -pl lite test`. On Windows/PowerShell, quote every `-D` argument.

### A6. Docs — DONE

- [x] `lite/README.md`: what it is, what's dropped, how to build (`mvn -pl lite -am install`), how to run (`java -jar camunda-lite-*.jar --port 26500`), how to connect a worker (Java snippet + Go note + 8.4 BPMN extension format), config reference, limitations (no durability guarantees, no multi-tenancy admin, no exporters, no metrics/actuator; long polling is the tested job path).

### A7 (optional) — Slim down further

- [ ] Remove `transport` module: patch `BrokerRequest` (drop `implements ClientRequest`, keep methods), delete `gateway/impl/stream/*`, drop `ClientStreamer` param from `Gateway` (make stream-jobs RPCs return `UNIMPLEMENTED`), remove `zeebe-atomix-*` deps from `gateway/pom.xml`.
- [ ] Remove `atomix` module entirely; verify `gateway`, `engine`, `stream-platform` compile without it (they shouldn't reference it after the above).
- [ ] Drop `snapshot` module if phase 5 is deferred.

---

## Part B — Reproduce on fresh Camunda sources

Run this on a **fresh clone** (latest release tag or `main`). The *concept* is identical; the *names and wiring shift between versions*. Do B1–B2 first (10 minutes) to produce an updated seam table, then re-execute A2–A6 with the mapped names.

### B1. Re-orient on the fresh tree

- [ ] Check `parent/pom.xml` version + license header (Zeebe Community License vs. Camunda license change — note for downstream use).
- [ ] Diff the root `pom.xml` module list against Part A's list (45 modules in 8.4.12). Known shifts to check:
  - `zeebe/` launcher/distribution module layout (8.8+ ships a **Zeebe standalone "lite"** distribution — check whether it already does most of this; if so, decide whether the fork adds value on top of it).
  - `atomix` module boundary changes; `AtomixLogStorage` may have moved out of `broker` into `atomix`/`journal`.
  - `journal` module may absorb the `LogStorage` file impl.
- [ ] Find the 4 seam interfaces in the fresh tree and confirm they still exist (they are stable APIs):
  - [ ] `io.camunda.zeebe.logstreams.storage.LogStorage`
  - [ ] `io.camunda.zeebe.stream.api.CommandResponseWriter`
  - [ ] `io.camunda.zeebe.stream.api.InterPartitionCommandSender`
  - [ ] `io.camunda.zeebe.gateway.impl.broker.BrokerClient` (verify package; gateway refactors in 8.6/8.7 moved identity into `io.camunda:camunda-identity` SDK plugins)

### B2. Re-derive the production wiring (replace Part A's references)

- [ ] Locate the new `StreamProcessorTransitionStep` (or equivalent broker step) → new `StreamProcessor.builder()` field list. Known additions since 8.4 to expect:
  - **Snapshots moved into stream-platform** (8.5+): builder gains snapshot store / `StateController` / `ProcessingDbState`-style params. With an in-memory log, decide: disable snapshots (if the API allows) or wire a minimal `SnapshotStore` writing to the local data dir.
  - **Checkpoint/backup processor** (`CheckpointRecordsProcessor`) — pass nothing (no backup).
  - `ProcessingState.build(...)` signature changes (e.g. `ProcessingState.build(db, partitionId, ...)` with feature flags / `ZeebeDbFactory` replaced by `ZeebeDb` creation helpers) — copy from the fresh engine test utils (`engine/src/test/.../EngineRule`/`TestStreams`/`RecordingExporter` era utilities).
- [ ] Locate the fresh engine test boot utility (equivalent of `TestStreams.createLogContext`/`buildStreamProcessor`) and use it as the wiring reference instead of the 8.4 one.
- [ ] Locate the fresh `EngineProcessors.createEngineProcessors(...)` signature (params: partitions count, command senders, feature flags, job streamer, and possibly more).
- [ ] Check whether the **Java client's JobWorker enables job streaming by default** in this version (8.8+ stream jobs became default/stable). If yes:
  - [ ] Keep `transport` (or its successor) and wire the client-side `ClientStreamer` on the gateway as production does, **or**
  - [ ] Configure the client to long-poll (client flag) and keep stream jobs disabled — document the flag in the README.
- [ ] Check gateway identity plumbing (8.7+ uses identity SDK plugins): configure anonymous/no-auth for local use.
- [ ] Check `ZeebeDbFactory` / `createInMemory`: newer versions may offer an in-memory DB factory (8.4 does not) — if available, prefer it for the "in-memory first" goal.

### B3. Re-execute A2–A6 with the mapped names

- [ ] A2 (module trim) with the fresh module list — same drop categories: broker, transport?(per B2 decision), topology, backup/restore/exporters, dist, qa, benchmarks.
- [ ] A3 (gateway trim) — delete the fresh equivalents of `BrokerClientImpl` / `BrokerRequestManager` / `BrokerTopologyManagerImpl`; adapt `Gateway` constructor changes (new params since 8.4, e.g. identity config, streamer, exporter control).
- [ ] A4 (new `lite` module) — same 6 classes; port `InProcessBrokerClient`'s request table to the fresh `BrokerRequest` subclasses (the set grows over versions: e.g. `EvaluateDecisionRequest`, `BroadcastSignalRequest`, `MigrateProcessInstanceRequest`, `SetVariablesRequest`, resource deletion, etc. — route supported ones, reject admin/exporting/backup/backup-store ones with `NOT_FOUND`/`UNIMPLEMENTED`).
- [ ] A5 (e2e test) — same flow; add migration/signals/message-correlation coverage if trivial.
- [ ] A6 (README) — update build/run/config/limitations.

### B4. Bump & record

- [ ] Update this document's seam table + reference-file list for the new version (keep this file as the living runbook; commit it into the fork).
- [ ] Record: base commit, version, list of deviating commits (the lite glue classes + deletions) in `lite/README.md`.

### B5. Upstream-tracking strategy (maintenance)

- [ ] Keep `engine`, `stream-platform`, `logstreams`, `zb-db`, `bpmn-model`, `protocol*` **100% untouched** — the fork's own code is only: the `lite` module + gateway deletions/additions. Rebased onto a new upstream tag by: cherry-pick/merge upstream, re-run B1–B2 seam check, re-run A5 e2e test.
- [ ] The e2e test from A5 is the regression gate for every rebase.

---

## Acceptance criteria (final)

- [x] `java -jar camunda-lite-*.jar --port 26500` starts a single process; gRPC port accepts connections (smoke-tested: process up, TCP connect to the gateway port OK, log prints "Camunda Lite is up and running").
- [x] Official Java client: deploy → start → job worker completes → process instance completes. Verified **externally**: shaded jar in one JVM, official `ZeebeClient` in a second JVM (separate process) — deploy OK, long-polling worker received the job and completed it, `withResult()` instance finished with the worker's variables. Go client: same protocol, untested (optional).
- [x] No Raft, no CLQ, no topology service, no exporter ever instantiated — verified: the shaded jar contains **zero** `io/camunda/zeebe/broker/**` classes (the whole `broker` module is out of the reactor); no Zeebe CLQ/exporter classes (only unrelated Spring Boot tracing classes match).
- [x] Build of the trimmed module set is smaller/faster (sanity): 33 modules in the reactor (was 45); `-pl lite -am install` ≈ 1 min after the first full build; `-pl lite` alone ≈ 13 s once siblings are in the local repo.
- [x] Timer events fire: `LiteEndToEndTest.timerCatchEventFiresAndFinishesInstance` (in-process e2e) — a `PT2S` timer catch event after a service task; the instance completes with the worker's variables only after ≥ 2s.
- [x] README explains limits: in-memory log (best-effort restart), single partition, anonymous auth, no exporters, no backup/restore, no metrics/actuator.

**Gotchas found during A6 (re-verify on Part B):**

1. **`Log4j2Plugins.dat` collision.** `spring-boot-*.jar` and `zeebe-util` each ship a tiny `META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat`; shade keeps only one, and the tiny one clobbers log4j-core's full plugin cache (21 KB). Symptom: log4j logs `Unrecognized conversion specifier [...]` for every pattern token and silently falls back to the default config. Fix: shade `<filter>` excluding that path from `org.springframework.boot:spring-boot` and `io.camunda:zeebe-util`.
2. **`log4j2.springboot` marker.** `spring-boot-*.jar` ships a `log4j2.springboot` file that switches log4j into Spring Boot config mode, ignoring our `log4j2.xml`. Exclude it in the same filter.
3. **Manifest.** The shade plugin needs an explicit `ManifestResourceTransformer` with `<mainClass>`, otherwise `java -jar` fails with "no main manifest attribute".
4. **`log4j-core` is not a transitive runtime dep** of the trimmed modules (it comes from `dist` in stock builds) — declare it explicitly in `lite/pom.xml` (runtime scope).
5. **RocksDB native-library extraction vs. `%TEMP%` (environment-specific, hit on the A6 dev machine).** Something on the machine (AV real-time protection / indexer) intermittently blocks ~8 MB writes into `%TEMP%` with a sharing violation ("file in use by another process"). Victims: `RocksDB.loadLibrary()`'s fallback (extracts the ~8 MB `librocksdbjni-win64.dll` into `java.io.tmpdir`) and surefire's large forked-JVM output files. Symptoms: `Unable to load the RocksDB shared library` in the forked test JVM, surefire `ForkStarter IOException` + "Process Exit Code: 156", or a surefire run that hangs after "Running ...". The plain `java -jar` smoke test usually works (the block is intermittent). Workaround: point the test JVM's `java.io.tmpdir` at a non-Temp dir, e.g. `lite\target\tmp`:
   - surefire: `mvn -pl lite test -Dquickly=true -DskipTests=false -Djacoco.skip=true "-DargLine=-Djava.io.tmpdir=L:\...\lite\target\tmp"` (`-Djacoco.skip=true` is required so the CLI `argLine` isn't overridden by jacoco's `prepare-agent` project property).
   - or skip surefire: run the test class directly with a JUnit Platform launcher (`junit-platform-launcher` from the local repo + `lite\target\testcp.txt` classpath) with the same `-Djava.io.tmpdir` flag.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| `LogStorage` reader contract subtleties (seek-to-position, block framing) break the stream reader | Copy semantics from `ListLogStorage` (test impl) + `AtomixLogStorageReader`; run `logstreams` unit tests against the new impl in Part A (`mvn -pl logstreams test` with the test harness pointed at `InMemoryLogStorage`). |
| `CommandResponseWriter` is used from multiple engine paths (success, rejection, batch) | Port `CommandResponseWriterImpl` field-by-field; cover reject paths in e2e (invalid start → expect gRPC error). |
| Gateway handlers assume cluster topology (retry/backoff over partitions) | Single-partition fixed topology; handlers degrade to one target. Watch `RoundRobinActivateJobsHandler`/`LongPollingActivateJobsHandler` for partition-id assumptions. |
| Job streaming (transport `ClientStreamer`) required by newer client defaults | B2 check; keep transport or force long-poll via client config. |
| 8.4 `EngineProcessors` needs `JobStreamer` non-null | No-op `JobStreamer` impl (tiny interface) in `lite`. |
| Snapshots/checkpoint params appear in newer `StreamProcessorBuilder` | Disable snapshots in v1; if builder requires a store, minimal file-based store in data dir. |
| Checkstyle/spotbugs/revapi plugins fail on new module | Copy the `lite/pom.xml` profiles from an existing simple module (e.g. `util`); disable revapi for the new module (no published API to check against). |
