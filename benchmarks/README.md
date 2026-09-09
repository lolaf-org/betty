# betty-benchmarks

JMH comparisons of betty against the network stacks it competes with, plus two hand-written NIO clients as a floor.
**Not published** — the module depends on Aeron, Netty, Jetty and Mina, and a published pom is a dependency
contract that would advertise six competing network stacks.

## What is measured

Both benchmarks talk to `BenchmarkServer`, a plain blocking-socket echo server on `localhost:9000` exchanging
16-byte packets, so what is compared is the client stack and nothing else.

| Benchmark | Mode | What one operation is |
|---|---|---|
| `ClientRTTBenchmark` | `AverageTime`, microseconds | send one message, wait for the ack — round-trip latency |
| `ClientThroughputBenchmark` | `Throughput`, with `@OperationsPerInvocation` | 1024 messages of 16 bytes, acked as a batch |

Eight clients run in each: `betty`, `netty`, `aeron`, `mina`, `jetty`, `asynchronousSocketChannel`, and
`NIOBlockingBusySpin` / `NIONonBlockingBusySpin` — the last two being what the JDK gives you with no library at
all, which is the number betty has to beat to be worth using.

## The parameter matrix, and why a run can fail by design

Every client is parameterised twice, in `AbstractClientBenchmark`:

- `clientSettings` — `STOCK` or `LOW_LATENCY` (busy-spinning selector, pinned IO thread)
- `clientSelectorSettings` — `JDK_STOCK` or `JDK_EPOLL`, set through `java.nio.channels.spi.SelectorProvider`

**Not every client supports every combination, and an unsupported one throws in `@Setup` rather than being
skipped.** Only betty is written for all of them:

| Client | Supported |
|---|---|
| `betty` | every combination |
| `netty`, `mina`, `jetty`, `asynchronousSocketChannel` | `STOCK` only — Netty states its NIO transport does not support a busy-spin selector |
| `NIOBlockingBusySpin` | `LOW_LATENCY` only |
| `aeron` | `JDK_STOCK` only; it does not go through a JDK selector at all |

So run one benchmark and one parameter set at a time (`-p clientSettings=STOCK`) rather than the whole jar, unless
errors in the log for the combinations that cannot exist are acceptable.

## Core affinity

The JMH thread and betty's IO thread are pinned with OpenHFT `Affinity` to cores derived from
`availableProcessors()`, so **results depend on the machine's core count and hyper-threading layout** and two
machines are not comparable. Aeron overrides this and runs unpinned: pinning the JMH thread destroys its
throughput, and why is not understood.

Each fork runs with `-Xms8g -Xmx8g` and the two `--add-opens` betty needs; those are in the `@Fork` annotations,
not in the surefire argLine, because nothing here runs under surefire.

## Running them

```bash
./mvnw -T1C clean install          # from the repository root
./run-benchmark.sh                 # from this directory
```

`run-benchmark.sh` runs each benchmark class in turn under `$JAVA_HOME/bin/java` when one is set, with the GC
profiler on, and files each JSON as `results/jmh-result-<Benchmark>-<date>.json` — the same script ringos uses, so
a result from either repository is named the same way. To run one on its own:

```bash
java -jar target/benchmarks.jar -rf json -prof gc ClientRTTBenchmark -p clientSettings=STOCK
```

Drop the JSON on <https://jmh.morethan.io/> to read it.

`results/` also holds three runs from November 2024, from before the per-benchmark naming. They predate most of the
library, so treat them as history rather than as current numbers — the comparison worth having is one you ran
yourself, on your own hardware, today.
