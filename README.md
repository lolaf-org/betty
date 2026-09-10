# Betty

[![build](https://github.com/lolaf-org/betty/actions/workflows/build.yml/badge.svg)](https://github.com/lolaf-org/betty/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.txt)

Asynchronous NIO networking for Java that must not allocate or block on its hot path.

Betty is the transport under a low-latency trading stack: selector-driven IO workers you can pin and balance sessions
across, pooled buffers so a read or a write allocates nothing, SSL that hands you the peer certificates, and select
strategies that let you choose where on the CPU-burn / wake-up-latency curve you want to sit.

- **Java 11 or later**, compiled to Java 11 bytecode.
- **Depends on [ringos](https://github.com/lolaf-org/ringos)** for its ring buffers, idle strategies and threading
  primitives, and on SLF4J. Nothing else.
- **Apache License 2.0.**

## Why it exists

Betty comes out of designing low-latency systems in finance, where the transport is TCP, the latency target and the
throughput target are both real numbers somebody signed up to, and the way you hit them is by tuning: how the
selector waits for readiness, which core a session is served on, how much of the machine you are willing to burn to
take a microsecond off. Two libraries dominate that space and neither leaves those dials where you can reach them.

**Netty** is very good, and it is what most of this industry runs on. But its NIO transport will not busy-spin a
selector — the [parameter matrix](benchmarks/README.md) cannot even run Netty in `LOW_LATENCY`, because there is no
such configuration to run — so you block in `select()` and pay the wake-up on every cross-thread write, and you
allocate about 430 B per round trip, which is a young collection on a schedule and a tail latency you did not
choose. **Aeron** is excellent, and on this run it is the only client that beats betty on latency. But it is not
TCP: it puts its own reliable delivery over UDP, so adopting it means adopting its protocol, its media driver and
its operational model at both ends of every link — and its 7.37 µs costs 473 B/op and a core it does not give back.
Neither of them is wrong. They are simply not a TCP framework you can tune.

There is another tier below all of this — a specific network adapter and a kernel-bypass stack, DPDK, TCP moved into
userspace — and it does go lower. What it costs is being stock: a NIC you have to specify, a driver and a stack to
install, tune and operate, and a deployment that no longer runs unchanged on whatever machine the JVM lands on. That
is a decision about the whole system, not about a library, and plenty of desks are right to take it. Betty stops
deliberately on this side of that line: it is for getting every microsecond and every message you can out of a
standard JVM talking to its host OS's TCP stack, on hardware nobody had to requisition. The heavy artillery stays
where it is, for the day this is genuinely not enough.

So that is what betty is. Plain NIO over TCP; a busy-spinning selector when a session deserves one and a blocking
one when it does not, chosen per thread group rather than per process, because the sessions that matter are usually
a handful and the rest should give the core back. On this run, over loopback with 16-byte packets, that dial prices
out at about 3 µs of round-trip against 1.3 Mops/s — betty at 11.70 µs and 24.52 Mops/s blocking in `select()`,
8.64 µs and 25.78 Mops/s busy-spinning. The microseconds are what the wake-up costs locally and do not shrink over
a real NIC; they just sit inside a larger total.

And close to nothing allocated while it runs: 0.10 B per round trip, 0.00035 B per message on the throughput
benchmark, against 166–632 B/op for Netty, Mina, Jetty and the JDK's own async API. That number matters out of all
proportion to its size. Allocating is not itself slow — it is a pointer bump — but everything allocated is
eventually collected, and a collection is a pause you did not schedule, cannot tune away after the fact, and will
meet again at the far end of the distribution you are actually judged on. A hot path that allocates nothing does not
fill Eden, and a young collection that never runs costs nothing. It also means that when you go through the GC log
looking for what is filling the heap, for once it is not the networking library.

Everything else in the API follows from those two commitments: a receive timestamp handed to `onRead`, `IOStats`
hooks that timestamp every hop so a latency budget can be attributed rather than guessed at, and a load balancer
that reads the NIC's `SO_INCOMING_NAPI_ID` to keep a session on the CPU the kernel is already steering its packets
to.

The bar it had to clear was never Netty, though. It was the hand-rolled selector loop that every shop with a latency
budget ends up writing, and then owning forever. [The benchmarks](#benchmarks) say it clears it: round-trip level
with a hand-rolled busy-spinning NIO client, throughput ahead of it, at a fraction of a byte per operation. Betty is
the transport half of a pair — [ringos](https://github.com/lolaf-org/ringos) is the other, and holds the ring
buffers, idle strategies and threading primitives both of them stand on.

## The two artifacts

| Module | What it gives you |
|---|---|
| `betty-api` | `Client`, `Server` and their builders, `IOSession`, `IOWorker`, `IOWorkersGroup`, the `IOEventsListener` callbacks, buffer-pool and SSL settings, select strategies, I/O statistics |
| `betty-impl` | The NIO implementation of all of it, selected at runtime |

`betty-api` holds no networking code. You compile against it and put `betty-impl` on the runtime classpath, which is
how the implementation stays swappable and how nothing in your code ends up naming an internal class:

```xml
<dependency>
    <groupId>org.lolaf.betty</groupId>
    <artifactId>betty-api</artifactId>
    <version>${betty.version}</version>
</dependency>
<dependency>
    <groupId>org.lolaf.betty</groupId>
    <artifactId>betty-impl</artifactId>
    <version>${betty.version}</version>
    <scope>runtime</scope>
</dependency>
```

`betty-impl` pulls the ringos implementations it needs at runtime scope of its own, so there is nothing else to add.

## Getting started

A client that connects, reads, and never allocates a buffer of its own:

```java
import org.lolaf.betty.api.Client;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.settings.IOBufferPoolSettings;
import org.lolaf.betty.api.settings.IOSettings;

import java.net.InetSocketAddress;

Client client = ClientBuilder.builder()
        .connectAddress(new InetSocketAddress("localhost", 9090))
        .ioEventsListener((session, message, localReceiveTimeInNanos) -> {
            // `message` is a pooled buffer owned by the worker: read it here, do not retain it
            message.position(message.limit());
        })
        .ioSettings(IOSettings.builder()
                .tasksRingBufferSize(2 * 1024)
                .writeIoBufferPoolSettings(IOBufferPoolSettings.builder()
                        .zone(IOBufferPoolSettings.IOBufferPoolZone.builder()
                                .poolSize(2 * 1024)
                                .bufferSize(1024)
                                .build())
                        .build())
                .build())
        .id("example-client")
        .build()
        .newInstance()
        .start();
```

`ServerBuilder` mirrors it. The single-method listener above is the read callback;
[`IOEventsListener`](api/src/main/java/org/lolaf/betty/api/io/IOEventsListener.java) has defaulted callbacks for
connection, disconnection, writes, write failures, errors, submitted tasks, and the three SSL events — implement the
interface rather than passing a lambda when you want them.

### The buffer contract

The `ByteBuffer` handed to `onRead` belongs to the pool and is recycled the moment the callback returns. Read what you
need inside the callback, or copy it. Retaining one is the one mistake that will bite you.

## Examples

`betty-examples` holds the compiling, running form of the snippet above:
[`PingPongExample`](examples/src/main/java/org/lolaf/betty/examples/ping/PingPongExample.java) starts a betty client
and a betty server in one JVM and bounces a 16-byte ping between them once a second. It is not published.

```bash
./mvnw -T 1C clean install     # from the repository root
java -jar examples/target/ping-example.jar
```

```text
22:02:52.078 [IO-worker-ping-client-0] INFO PingPongExample - connected to localhost/127.0.0.1:9099
22:02:52.078 [IO-worker-ping-server-0] INFO PingPongExample - client connected from /127.0.0.1:44054
22:02:53.069 [IO-worker-ping-client-0] INFO PingPongExample - pong seq=1 rtt=2604us
22:02:54.066 [IO-worker-ping-client-0] INFO PingPongExample - pong seq=2 rtt=228us
```

Everything not needed to show a round trip is left at its default. What it does show is the buffer contract above,
the framing loop `while (message.remaining() >= FRAME_SIZE)` — betty compacts a trailing partial frame into the
front of the next read, so you never buffer one yourself — and the fact that the one-second wait belongs on a
scheduler rather than inside a callback, because an IO thread that sleeps stops serving every other session on that
worker. [`examples/README.md`](examples/README.md) has the detail.

## Choosing a select strategy

`SelectStrategy` decides how a worker waits for readiness, and it is the main latency/CPU dial:

| Strategy | Behaviour |
|---|---|
| `WakeupSelectStrategy` | The default. Blocks in `Selector.select()` until readiness or its timeout, giving the core back. A write from another thread has to wake the selector, so it costs a syscall |
| `IdleStrategySelectStrategy` | Never blocks: `selectNow()`, then a ringos `IdleStrategy` decides whether to spin, yield or park. Readiness is seen on the next pass and a cross-thread write needs no wakeup. The cost is a core, held whether or not there is traffic |

It is set on an `IOThreadGroup`, not on the whole `IOWorkersGroup`, so one client or server can spend a core on the
thread group carrying the sessions that matter and block on the rest. Unset, a thread group gets
`WakeupSelectStrategy` with a 10 ms timeout.

## Load balancing sessions across workers

An `IOWorkersGroup` owns a set of worker threads and an `IOWorkerLoadBalancer` decides which one a new session lands
on. Four are shipped: `DedicatedIOWorkerLoadBalancer` (one session per worker),
`MinRegisteredSessionLoadBalancer` (fewest sessions, and the default), `MinIOThreadLoadSessionLoadBalancer` (least
busy by measured thread load), and `NapIdLoadBalancer`, which reads Linux's `SO_INCOMING_NAPI_ID` so a session is
handled on the worker whose CPU the NIC already steered its packets to. The other three fall back to the default
when what they count is unavailable, so the choice is a tuning one and never a correctness one.

## Building

JDK 17 or newer to build; the jars themselves target Java 11 and run on it. The build enforces both.

```bash
./mvnw -T 1C clean install
```

The build also gates on things worth knowing about before you send a change:

- **Javadoc references** — `maven-javadoc-plugin` runs with `-Xdoclint:all,-missing`, so a `{@link}` that does not
  resolve fails the build. Lombok-generated accessors are visible to it because javadoc reads a delomboked copy of
  the sources rather than the originals.
- **Licence headers** — `license-maven-plugin` checks every source file against `HEADER.txt`.
- **The module path** — `betty-module-path-test` resolves the published jars as JPMS modules and fails if a package
  is ever contributed by more than one of them. That is a real defect that only shows up for consumers with their own
  `module-info`, and no other test would catch it.
- **A module must never declare its own `<argLine>`** — it replaces the shared surefire arguments wholesale and the
  pieces it does not restate disappear silently. Add to `surefire.argLine.extra` instead.

## Benchmarks

`betty-benchmarks` holds JMH comparisons against Aeron, Netty, Jetty and Mina, plus two hand-written NIO clients as
the floor a library has to beat to be worth using. It is not published.

```bash
./mvnw -T 1C clean install     # from the repository root
cd benchmarks && ./run-benchmark.sh
```

On the 2026-09-10 run — 16-byte packets over loopback, one machine, one fork — betty's round-trip latency is level
with a hand-rolled busy-spinning NIO client (8.64 µs against 8.60, inside both error bars) and ahead of both
hand-rolled clients on throughput by more than their intervals overlap. It is ahead of Netty, Mina, Jetty and the
JDK's own `AsynchronousSocketChannel` on latency, and behind Aeron on latency in the one configuration Aeron is
tuned for — while staying ahead of Aeron on throughput in both. Jetty on both benchmarks, Mina on throughput and the
JDK client on round-trip carry error bars as wide as their own scores, so their placement is unresolved. The gap
that is not close is allocation: 0.10 B/op per round-trip against 166–632 B/op for Netty, Mina, Jetty, the JDK
client and Aeron in the configuration Aeron is quickest in — which is the whole point of the buffer pooling.

**Those numbers are one machine, and both benchmarks pin threads to cores derived from `availableProcessors()`, so
two machines are not comparable.** Run them on your own hardware before believing any of it.

[`benchmarks/README.md`](benchmarks/README.md) has the full tables, what each number does and does not support, the
parameter matrix, which client supports which combination, and why running the whole jar at once logs errors by
design. The raw JSON of every run is in [`benchmarks/results/`](benchmarks/results); drop a file on
<https://jmh.morethan.io/> to read it.
