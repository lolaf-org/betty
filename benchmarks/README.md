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
| `BackPressureBenchmark` | `AverageTime`, milliseconds | 4 MB handed to a session whose peer has stopped reading, timed until the last byte reaches the socket |

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

## The back-pressure benchmark

`BackPressureBenchmark` is not part of the comparison above and runs betty only. It exists because the other two
never take the path it measures: `BenchmarkServer` drains as fast as it is fed, so its socket never refuses a byte.
Instrumenting a throughput run showed exactly one `socket.write` call per message and **zero** zero-length writes, so
nothing about how a refused write is handled can be measured there.

`SlowConsumerServer` pauses 20 µs between 8 KB reads with a 32 KB receive buffer, which closes the window often
enough that the writer sees partial and zero-length writes throughout the run. One invocation hands the session 4 MB
and waits on `waitForAllMessagesSent`, so the score is the time to push a payload through a socket that keeps saying
no.

The consumer sets the floor, and what is worth reading is how much the writer adds to it — which is why the run is
parameterised over `clientSettings` and not over the selector provider. A busy-spun selector notices the window
reopening on its next pass; a blocking one has to be woken, so `STOCK` is where the cost of deferring a refused write
shows up and `LOW_LATENCY` is where it mostly does not.

```bash
java -jar target/benchmarks.jar -rf json -prof gc BackPressureBenchmark
```

Run it before and after any change to `writeToSocket`, `continueWriteToSocket` or the write cycle around them.

It needs more than the default single fork to say anything: at `-f 1 -i 3` one run came back 41.3 ± 26.4 ms, and at
`-f 2 -i 5` the same build gave 40.4 ± 0.62 ms `STOCK` and 39.5 ± 0.39 ms `LOW_LATENCY`. So it resolves a couple of
percent and no better — enough for a change that alters how often the writer waits for the selector, not for one that
shaves instructions off the write path.

## Measuring `orderedWrites`

`BettyClient` answers reads from the IO thread, so `IOSettings.orderedWrites` decides whether each answer is a socket
write or a ring round trip, and both benchmarks sit on that path. It is a `@Param` on `BettyClient` alone, fixed to
`false` so the tables above are unaffected; pass both values to compare:

```bash
java -jar target/benchmarks.jar -f 2 -p orderedWrites=false,true ClientRTTBenchmark.betty
```

On the run of this repository's own machine it cost 7.31 % of round-trip latency in `STOCK` and 3.35 % in
`LOW_LATENCY` - the gap between them being the selector wakeup a busy-spun group does not pay - and nothing outside
the error bars on throughput, where 1024 messages leave in about two socket writes either way.

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

`results/` holds the runs of 2026-09-10 and 2026-09-11, one JSON per benchmark class per run. The tables below
are read off the 2026-09-11 pair.

## Results

From the run of 2026-09-11 — JMH 1.37, JDK 21.0.12.1 (Zulu), one fork, one thread, 10 s iterations (two warmup for
`ClientRTTBenchmark`, three for `ClientThroughputBenchmark`, three measured in both), `-Xms8g -Xmx8g`, against
`BenchmarkServer` over loopback with 16-byte packets. `±` is JMH's 99.9 % confidence interval and *Alloc* is
`gc.alloc.rate.norm`.

**These are one machine and one fork,** so treat them as an illustration rather than as a specification. Both
benchmarks pin threads to cores derived from `availableProcessors()`, so the numbers move with core count and
hyper-threading layout and two machines are not comparable — the comparison worth having is one you ran yourself, on
your own hardware, today. Both tables use the `JDK_STOCK` selector provider: every `JDK_EPOLL` score falls inside its
`JDK_STOCK` counterpart's interval once both intervals are counted, so the provider is left out rather than doubling
the tables. The 2026-09-10 run had one row where the two were far apart — betty's `STOCK` throughput, 24.52 Mops/s on
`JDK_STOCK` against 20.83 ± 1.41 on `JDK_EPOLL`, against a `JDK_STOCK` interval of ± 4.01 that did not separate them
either. In this run that gap is gone: 24.75 ± 0.17 and 24.60 ± 0.43.

### Round-trip latency — `ClientRTTBenchmark`

One message out, wait for the ack. Lower is better.

| Client | Settings | µs/op | Alloc (B/op) |
|---|---|---:|---:|
| `aeron` | `LOW_LATENCY` | 6.89 ± 0.67 | 538.90 |
| `NIONonBlockingBusySpin` | `LOW_LATENCY` | 8.53 ± 1.24 | 0.008 |
| **`betty`** | **`LOW_LATENCY`** | **9.15 ± 0.23** | **0.074** |
| `NIOBlockingBusySpin` | `LOW_LATENCY` | 11.03 ± 0.13 | 0.011 |
| **`betty`** | **`STOCK`** | **11.76 ± 0.32** | **0.095** |
| `netty` | `STOCK` | 12.02 ± 0.89 | 429.74 |
| `asynchronousSocketChannel` | `STOCK` | 13.00 ± 11.22 | 164.98 |
| `mina` | `STOCK` | 13.62 ± 1.65 | 632.02 |
| `jetty` | `STOCK` | 22.34 ± 7.68 | 419.95 |
| `aeron` | `STOCK` | 191.86 ± 11.24 | 0.891 |

### Throughput — `ClientThroughputBenchmark`

1024 messages of 16 bytes acked as a batch, scored per message. Higher is better.

| Client | Settings | Mops/s | Alloc (B/op) |
|---|---|---:|---:|
| **`betty`** | **`LOW_LATENCY`** | **25.28 ± 0.37** | **0.00032** |
| `NIOBlockingBusySpin` | `LOW_LATENCY` | 24.98 ± 0.47 | 0.00004 |
| **`betty`** | **`STOCK`** | **24.75 ± 0.17** | **0.00033** |
| `NIONonBlockingBusySpin` | `LOW_LATENCY` | 24.54 ± 0.71 | 0.00004 |
| `netty` | `STOCK` | 23.37 ± 9.49 | 0.84 |
| `aeron` | `LOW_LATENCY` | 16.91 ± 3.97 | 4.97 |
| `asynchronousSocketChannel` | `STOCK` | 13.43 ± 10.45 | 0.59 |
| `jetty` | `STOCK` | 13.20 ± 14.53 | 0.81 |
| `aeron` | `STOCK` | 4.84 ± 0.78 | 0.00096 |
| `mina` | `STOCK` | 3.65 ± 6.02 | 33.70 |

Every client completed its whole supported matrix in a single run of `run-benchmark.sh`, including
`asynchronousSocketChannel`, which the 2026-09-10 run did not measure cleanly — it had only a `JDK_EPOLL` round-trip
row and no throughput row at all. Its intervals here are still too wide to place it (see below), but it is measured.

### Reading them

- **The floor is the number that matters.** In `LOW_LATENCY` betty's round-trip is 9.15 µs against a hand-rolled
  busy-spinning NIO client's 8.53 — a gap inside that client's own ± 1.24 interval, so this run does not separate
  them. On throughput betty leads both hand-rolled clients, 25.28 against 24.98 and 24.54, but by less than the
  intervals overlap: unlike the 2026-09-10 run, this one does not establish that lead. Either way the cost of using
  the library instead of writing the selector loop yourself is at or below what the run can resolve.
- **Only Aeron beats it on latency, and only in one configuration.** 6.89 µs against betty's 9.15, at 539 B/op and a
  core; in `STOCK` it goes to 192 µs. Betty is ahead of it on throughput in both configurations, by 8 Mops/s in
  `LOW_LATENCY` and fivefold in `STOCK`. Aeron also runs unpinned here — pinning the JMH thread destroys its
  throughput, for reasons nobody has got to the bottom of.
- **Allocation is the unambiguous win.** Betty stays at
  0.095 B/op on round-trip and 0.00033 B/op per message on throughput, against 165–632 B/op and 0.59–33.7 B/op for
  Netty, Jetty, Mina and the JDK's async API. The sub-byte figures are JMH averaging away one rare allocation, not a
  fractional object: the hot path allocates nothing. Betty's two figures are also the ones that did not move between
  the 2026-09-10 and 2026-09-11 runs — 0.073 to 0.074 B/op and 0.00033 to 0.00032 — while Aeron's `LOW_LATENCY`
  round-trip went 473 to 539 B/op and Jetty's 342 to 420, which is what allocating on the hot path looks like from
  one run to the next.
- **The JDK's own async API is not the cheap way to get out of writing a selector loop.**
  `asynchronousSocketChannel` is a microsecond behind betty's `STOCK` round-trip, with an interval wide enough that
  the gap could be several, and allocates 165 B/op doing it — which is what the pooled-buffer machinery exists to
  avoid.
- **`STOCK` is close behind `LOW_LATENCY` on throughput, not on latency.** Betty gives up 0.5 Mops/s by not
  busy-spinning but 2.6 µs of round-trip, which is the trade the select strategies exist to let you make.
