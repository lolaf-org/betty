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

`results/` holds the run of 2026-09-10, one JSON per benchmark class. The tables below are read off them.

## Results

From the run of 2026-09-10 — JMH 1.37, JDK 21.0.12.1 (Zulu), one fork, one thread, 10 s iterations (two warmup for
`ClientRTTBenchmark`, three for `ClientThroughputBenchmark`, three measured in both), `-Xms8g -Xmx8g`, against
`BenchmarkServer` over loopback with 16-byte packets. `±` is JMH's 99.9 % confidence interval and *Alloc* is
`gc.alloc.rate.norm`.

**These are one machine and one fork,** so treat them as an illustration rather than as a specification. Both
benchmarks pin threads to cores derived from `availableProcessors()`, so the numbers move with core count and
hyper-threading layout and two machines are not comparable — the comparison worth having is one you ran yourself, on
your own hardware, today. Both tables use the `JDK_STOCK` selector provider: every `JDK_EPOLL` score fell inside its
`JDK_STOCK` counterpart's error bar, so the provider is left out rather than doubling the tables. The one row where
the two are far apart is betty's `STOCK` throughput — 24.52 Mops/s on `JDK_STOCK` against 20.83 ± 1.41 on
`JDK_EPOLL` — and that is the row whose `JDK_STOCK` interval is ± 4.01, so the run does not separate them either.

### Round-trip latency — `ClientRTTBenchmark`

One message out, wait for the ack. Lower is better.

| Client | Settings | µs/op | Alloc (B/op) |
|---|---|---:|---:|
| `aeron` | `LOW_LATENCY` | 7.37 ± 0.48 | 472.67 |
| `NIONonBlockingBusySpin` | `LOW_LATENCY` | 8.60 ± 1.45 | 0.008 |
| **`betty`** | **`LOW_LATENCY`** | **8.64 ± 0.70** | **0.073** |
| `NIOBlockingBusySpin` | `LOW_LATENCY` | 10.93 ± 0.36 | 0.010 |
| **`betty`** | **`STOCK`** | **11.70 ± 0.37** | **0.100** |
| `netty` | `STOCK` | 11.91 ± 1.13 | 430.17 |
| `asynchronousSocketChannel` † | `STOCK` | 12.99 ± 12.79 | 166.47 |
| `mina` | `STOCK` | 13.60 ± 1.66 | 632.02 |
| `jetty` | `STOCK` | 22.37 ± 26.27 | 342.36 |
| `aeron` | `STOCK` | 194.29 ± 28.27 | 0.894 |

### Throughput — `ClientThroughputBenchmark`

1024 messages of 16 bytes acked as a batch, scored per message. Higher is better.

| Client | Settings | Mops/s | Alloc (B/op) |
|---|---|---:|---:|
| **`betty`** | **`LOW_LATENCY`** | **25.78 ± 0.19** | **0.00033** |
| `NIOBlockingBusySpin` | `LOW_LATENCY` | 24.85 ± 0.17 | 0.00004 |
| `NIONonBlockingBusySpin` | `LOW_LATENCY` | 24.77 ± 0.30 | 0.00004 |
| **`betty`** | **`STOCK`** | **24.52 ± 4.01** | **0.00035** |
| `netty` | `STOCK` | 23.40 ± 0.95 | 0.84 |
| `aeron` | `LOW_LATENCY` | 16.69 ± 1.10 | 4.96 |
| `jetty` | `STOCK` | 13.27 ± 15.46 | 0.81 |
| `aeron` | `STOCK` | 4.85 ± 0.23 | 0.00096 |
| `mina` | `STOCK` | 3.81 ± 5.71 | 33.46 |

† `asynchronousSocketChannel` is the one client this run did not measure cleanly: the round-trip file has only its
`JDK_EPOLL` row, quoted above in place of the missing `JDK_STOCK` one, and the throughput file has no row for it at
all, so it is absent from the second table. Every other client completed its whole supported matrix in a single run
of `run-benchmark.sh`.

### Reading them

- **The floor is the number that matters.** In `LOW_LATENCY` betty's round-trip is level with a hand-rolled
  busy-spinning NIO client — 8.64 µs against 8.60, a difference well inside both error bars — and ahead of both
  hand-rolled clients on throughput by more than the intervals overlap. That is the case for using a library instead
  of writing the selector loop yourself: it costs nothing.
- **Only Aeron beats it on latency, and only in one configuration.** 7.37 µs against betty's 8.64, at 473 B/op and a
  core; in `STOCK` it goes to 194 µs. Betty is ahead of it on throughput in both configurations, by 9 Mops/s in
  `LOW_LATENCY` and fivefold in `STOCK`. Aeron also runs unpinned here — pinning the JMH thread destroys its
  throughput, for reasons nobody has got to the bottom of.
- **Allocation is the unambiguous win.** Betty stays at 0.10 B/op on round-trip and 0.00035 B/op per message on
  throughput, against 166–632 B/op and 0.81–33.5 B/op for Netty, Jetty, Mina and the JDK's async API. The sub-byte
  figures are JMH averaging away one rare allocation, not a fractional object: the hot path allocates nothing.
- **Two rows in each table have an error bar as wide as their own score** — `jetty` and `asynchronousSocketChannel`
  on round-trip, `jetty` and `mina` on throughput. Read those as unresolved: one fork of three iterations does not
  place them. Every other row in this run came out with an interval well inside its own score, and the allocation
  column is stable throughout.
- **The JDK's own async API is not the cheap way to get out of writing a selector loop.**
  `asynchronousSocketChannel` is a microsecond behind betty's `STOCK` round-trip, with an interval wide enough that
  the gap could be several, and allocates 166 B/op doing it — which is what the pooled-buffer machinery exists to
  avoid.
- **`STOCK` is close behind `LOW_LATENCY` on throughput, not on latency.** Betty gives up 1.3 Mops/s by not
  busy-spinning but 3 µs of round-trip, which is the trade the select strategies exist to let you make.
