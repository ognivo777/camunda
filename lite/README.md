# Camunda Lite

A single-node, local BPMN executor forked from the Camunda (Zeebe) monorepo.

It runs the **real Zeebe 8.4.12 engine + the official gRPC gateway** in one JVM, with
none of the cluster machinery: no Raft, no multi-partition, no exporters, no
backup/restore, no admin API. Existing Java/Go `ZeebeClient` job workers connect and
work **unchanged**.

- **Kept 100% as-is:** the `engine` module (BPMN/DMN/FEEL semantics are exactly stock
  Camunda), `stream-platform`, `logstreams`, `zb-db`, the gateway protocol.
- **Dropped:** the `broker` module, cluster membership/topology service, CLQ transport,
  exporters, backup/restore, benchmarks, QA.
- **New:** this `lite` module — five small glue classes + the `CamundaLite` launcher
  that wires `LogStream` → `StreamProcessor` → `Engine` → `Gateway` directly (the same
  shape the engine's own `TestStreams` test harness uses).

## Base

- Fork of `io.camunda:zeebe-parent` **8.4.12**, base commit `5a537e202c7911c54541d17957bcbd93fe29dc8f`
  (`8.4.12-1-g5a537e202c`, "Timers performance test").
- Branch: `lite-camunda`.
- Reuse runbook (including how to reproduce this fork on a newer Camunda version):
  [`docs/camunda-lite-fork.md`](../docs/camunda-lite-fork.md).

## Build

Requires JDK 21. From the repository root:

```
.\mvnw.cmd -Dquickly=true -DskipTests -pl lite -am install
```

This produces the shaded single jar:

```
lite/target/camunda-lite-8.4.12.jar
```

`-am` builds the ~30 required sibling modules. Once they are installed in the local
repository, `-pl lite` alone is enough for later iterations.

## Run

```
java -jar lite/target/camunda-lite-8.4.12.jar
```

| Flag         | Default | Meaning                                    |
|--------------|---------|--------------------------------------------|
| `--port`     | `26500` | gRPC gateway port                          |
| `--data-dir` | `data`  | Local directory for RocksDB state/snapshots |

The gateway listens on `grpc://localhost:26500` with **anonymous (no-auth)** gRPC.
Point your client at it with plaintext:

```
ZeebeClient.newClientBuilder().gatewayAddress("localhost:26500").usePlaintext().build()
```

## Connecting a worker

A service task with a job worker (Zeebe 8.4 extension format — note the `type`
attribute on `zeebe:taskDefinition`):

```xml
<bpmn:serviceTask id="task">
  <bpmn:extensionElements>
    <zeebe:taskDefinition type="my-task"/>
  </bpmn:extensionElements>
  <bpmn:incoming>...</bpmn:incoming>
  <bpmn:outgoing>...</bpmn:outgoing>
</bpmn:serviceTask>
```

Java worker (official `zeebe-client-java`, no changes needed):

```java
final var client = ZeebeClient.newClientBuilder()
    .gatewayAddress("localhost:26500")
    .usePlaintext()
    .build();

try (var worker = client.newWorker()
        .jobType("my-task")
        .handler((jobClient, job) -> jobClient.newCompleteCommand(job)
            .variables(Map.of("done", true))
            .send())
        .timeout(Duration.ofSeconds(30))
        .open()) {

  final var result = client.newCreateInstanceCommand()
      .bpmnProcessId("my-process")
      .latestVersion()
      .withResult()
      .send()
      .toCompletableFuture()
      .join();
}
```

Go workers work the same way with the standard `zeebe-go` client, dialing
`localhost:26500` in plaintext.

## How it works

All distribution machinery in Zeebe plugs in through small seams; the lite module
replaces each with an in-process implementation:

| Seam (interface)                        | Lite impl (in `lite`)                      |
|-----------------------------------------|--------------------------------------------|
| `LogStorage` (logstreams)               | `InMemoryLogStorage`                       |
| `CommandResponseWriter` (stream-platform)| `InMemoryCommandResponseWriter`           |
| `InterPartitionCommandSender`           | `NoOpInterPartitionCommandSender`          |
| `StageableScheduledCommandCache` (stream-platform) | `InMemoryScheduledCommandCache`    |
| `BrokerClient` / `BrokerTopologyManager` (gateway) | `InProcessBrokerClient` / `StaticBrokerTopologyManager` |

**Command path.** A gRPC request from the client is a `BrokerExecuteCommand` in the
gateway. `InProcessBrokerClient` assigns a `requestId`, writes the request's DTO
directly as a `COMMAND` record into the log stream (skipping the broker's SBE
request round-trip), and parks a pending future. The engine processes the record and
answers via `InMemoryCommandResponseWriter`, which hands the serialized response
straight back to `InProcessBrokerClient`, completing the pending future. The gateway
then maps the response to the gRPC reply. Rejections and broker errors are converted
to `BrokerRejectionException` / `BrokerErrorException` exactly like the stock broker's
`BrokerRequestManager`, so the client receives the same gRPC statuses as on a normal
cluster.

**Job path.** When a job becomes available, `InMemoryJobStreamer` (a) pushes it over
the (experimental) `StreamJobs` API via `InMemoryClientStreamer`, and (b) notifies the
gateway's long-polling `ActivateJobs` handler in-process, replacing the atomix
cluster broadcast that stock brokers use. Standard long-polling workers (the Java
client default) therefore keep working.

## Limitations (v1)

- **Single node, single partition.** No failover, no scaling, no multi-partition
  routing (the gateway always talks to "partition 1").
- **No restart persistence.** The log stream is in-memory, so nothing is carried over
  between runs. On every start the launcher removes the previous `--data-dir` contents
  (a warning is logged) and begins from a clean state; after a restart, previous
  deployments, process instances and jobs are gone.
- **No auth** (anonymous only), **no multi-tenancy administration**, **no exporters**,
  **no backup/restore**, **no admin API**, **no metrics/actuator**.
- The experimental **job streaming** (`StreamJobs`) is wired but long polling is the
  supported, tested path.

## Tests

```
.\mvnw.cmd -Dquickly=true -DskipTests=false -Dmaven.javadoc.skip=true -pl lite test
```

`LiteEndToEndTest` boots the full runtime in-process (free port, temp data dir) and
drives it with the official `ZeebeClient`: deploy → start instance → job worker
completes → instance finishes with the expected variables. A second test verifies that
a timer catch event fires: the instance waits at a `PT2S` timer after the service task
and only finishes once the due date has passed. (Note: the parent pom binds
`skipTests` to the `quickly` profile, so tests need the explicit `-DskipTests=false`.)

If the forked test JVM dies with `Unable to load the RocksDB shared library` or surefire
reports `ForkStarter IOException` / "Process Exit Code: 156" on Windows, the machine's AV
is likely blocking the ~8 MB native-library extraction into `%TEMP%`. Re-run with the
test JVM's temp dir pointed elsewhere:

```
.\mvnw.cmd -Dquickly=true -DskipTests=false -Djacoco.skip=true "-DargLine=-Djava.io.tmpdir=<repo>\lite\target\tmp" -pl lite test
```
