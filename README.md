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

## Why another network library

Betty was written for one requirement: the lowest latency and the least memory allocation a TCP transport library can manage
on a stock JVM. That is what the low-latency finance space asks of one, where the targets are numbers somebody signed
up to, and you hit them by tuning — how the selector waits for readiness, which core serves a session, how much of the
machine you will burn to take a microsecond off. The two libraries that dominate that space do not leave those dials
where you can reach them.

**Netty** will not busy-spin a selector, so you block in `select()`, pay the wake-up on every cross-thread write,
and allocate on every round trip.

**Aeron** is the only client here that beats betty on latency, and that is exactly what you should expect: it is not
TCP — nor is it plain UDP, but a lightweight TCP-like reliable protocol of its own built over UDP. So this is two
different transports being compared, not two implementations of the same one. A connection is bidirectional the moment
TCP gives it to you; in Aeron each direction is a separate publication and subscription you have to configure, address
and operate, so a request/response link is two one-way streams you set up yourself. Adopting it means adopting that
protocol, its media driver and its operational model at both ends of every link — fine where both ends are yours,
disqualifying where they are not. A TCP port can be dialled by anything: another language, a stock client library,
somebody else's gateway, a counterparty who was told a host and a port and nothing else. An Aeron endpoint can only be
reached by another Aeron, so a public API is not something you serve over it.

Below both is kernel bypass — a NIC you have to buy for it, DPDK, TCP moved into userspace — which does go lower, and
costs you being stock. Betty stops deliberately on this side of that line: a standard JVM, its host OS's TCP stack,
hardware nobody had to requisition.

What that buys is a client that allocates essentially nothing per operation, where every library above allocates on
every one. Allocating is not itself slow, but everything allocated is eventually collected, and a collection is a
pause you did not schedule. [`benchmarks/README.md`](benchmarks/README.md) has the round-trip, throughput and
allocation tables, and what each number does and does not support.

The rest of the API is there so you can tune with numbers instead of guesses: a receive timestamp handed to `onRead`,
`IOStats` hooks that timestamp every hop so a latency budget can be attributed rather than guessed at, and a load
balancer that reads the NIC's `SO_INCOMING_NAPI_ID` so the sessions fed by one RX queue are served by one worker
thread.

The bar was never Netty, though. It was the hand-rolled selector loop that every shop with a latency budget ends up
writing, and then owning forever. [The benchmarks](#benchmarks) say it clears it: round-trip level with a hand-rolled
busy-spinning NIO client, throughput ahead of it. Betty is the transport half of a pair —
[ringos](https://github.com/lolaf-org/ringos) holds the ring buffers, idle strategies and threading primitives both
stand on.

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

Betty reads and writes `java.nio.ByteBuffer`, the JDK's own type — there is no `ByteBuf`, no `IoBuffer`, no
`UnsafeBuffer` to learn, and nothing to convert at the boundary. Netty, Mina and Aeron each define a buffer type of
their own, so a payload on its way to anything that speaks the standard API — a `CharsetDecoder`, a `MessageDigest`,
an SBE or protobuf codec, a `FileChannel` — leaves their world through a wrapper call or a copy. Here it is already
the type those APIs take. Buffers from [`IOWriter#borrow(int)`](api/src/main/java/org/lolaf/betty/api/io/IOWriter.java)
come from the session's [`IOBufferPool`](api/src/main/java/org/lolaf/betty/api/io/IOBufferPool.java) and are direct by
default (`IOBufferPoolSettings.directBuffers`), as is the read buffer (`IOSettings.readDirectBuffer`), so neither a
read nor a write pays the JDK's own heap-to-direct staging copy either.

There is no reference counting to get wrong. The `ByteBuffer` handed to
[`IOEventsListener#onRead`](api/src/main/java/org/lolaf/betty/api/io/IOEventsListener.java) is the session's own read
buffer, which the next read overwrites, and a buffer borrowed for a write goes back to the pool on its own once the
write completes or fails. Read what you need inside the callback, or copy it. Retaining one is the one mistake that
will bite you.

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
worker.

[`NapIdExample`](examples/src/main/java/org/lolaf/betty/examples/napid/NapIdExample.java) is the second one, and
exists to be run on two machines: a server and four clients on `NapIdLoadBalancer`, bound to the host's first real
adapter rather than loopback, printing each session's NAPI ID so the placement can be checked rather than assumed.
Run both halves on one host and every ID reads 0 — same-host traffic is delivered locally whatever address it is
sent to, so it never reaches an RX queue.

[`SlowConsumerExample`](examples/src/main/java/org/lolaf/betty/examples/watermark/SlowConsumerExample.java) is the
third, and shows what `writeHighWatermark` and `writeLowWatermark` are for: a server producing flat out against a
client that reads 256 KB a second, throttling itself on the high mark and starting again on the low one, so that
averaged over a cycle it sends at exactly the rate the client reads.
[`examples/README.md`](examples/README.md) has the detail on all three.

## Choosing a select strategy

`SelectStrategy` decides how a worker waits for readiness, and it is the main latency/CPU dial:

| Strategy | Behaviour |
|---|---|
| `WakeupSelectStrategy` | The default. Blocks in `Selector.select()` until readiness or its timeout, giving the core back. A write from another thread has to wake the selector, so it costs a syscall |
| `IdleStrategySelectStrategy` | Never blocks: `selectNow()`, then a ringos `IdleStrategy` decides whether to spin, yield or park. Readiness is seen on the next pass and a cross-thread write needs no wakeup. The cost is a core, held whether or not there is traffic |

It is set on an `IOThreadGroup`, not on the whole `IOWorkersGroup`, so one client or server can spend a core on the
thread group carrying the sessions that matter and block on the rest. Unset, a thread group gets
`WakeupSelectStrategy` with a 10 ms timeout.

### Which idle strategy

`IdleStrategySelectStrategy` takes a ringos `IdleStrategy`, consulted after every `selectNow()` pass with the number
of keys that pass found — so the select strategy picks *whether* to block and the idle strategy picks *how hard to
wait*. Four of the six ringos ships suit a selector loop:

| Idle strategy | Behaviour | Cost while idle |
|---|---|---|
| `BusySpinIdleStrategy` | One `Thread.onSpinWait()`. Readiness is seen on the next pass, tens of nanoseconds later — the reason to choose this select strategy at all | A whole core, traffic or not |
| `YieldingIdleStrategy` | One `Thread.yield()`: offers the core to anything else runnable, but never parks, so a wake-up is a scheduler decision away rather than a timer away | A core, unless something else wants it |
| `BackoffIdleStrategy` | Escalates — spins, then yields, then parks for a period that doubles up to a ceiling. A pass that finds a ready key resets it to spinning. The no-arg constructor defaults to 10 spins, 5 yields and parks doubling from 50 µs to 1 ms; the four-arg one sets `maxSpins`, `maxYields`, `minParkPeriodNs` and `maxParkPeriodNs` yourself | Almost nothing once quiet, for up to the park ceiling in wake-up delay |
| `TimerSlackAwareBackoffIdleStrategy` | The same escalation and the same two constructors, and it narrows the thread's OS timer slack through `prctl` so a `minParkPeriodNs` under 50 µs is not rounded back up to it. Linux only | As `BackoffIdleStrategy` |

The other two, `WaitNotifyIdleStrategy` and `TimedWaitNotifyIdleStrategy`, park until a `wakeup()` a select loop
never sends, so `IdleStrategySelectStrategy` rejects them in its constructor.

## Load balancing sessions across workers

An `IOWorkersGroup` owns a set of worker threads, and an `IOWorkerLoadBalancer` decides which one a new session lands
on. Four are shipped:

| Balancer | Picks the worker with | Choose it when |
|---|---|---|
| `MinRegisteredSessionLoadBalancer` *(default)* | the fewest registered sessions | sessions cost roughly the same. Counting them is free, so this is the one to keep unless something below earns its price |
| `MinIOThreadLoadSessionLoadBalancer` | the lowest EMA of CPU time spent on IO | a few sessions are far busier than the rest. It needs per-session CPU accounting, which puts `System.nanoTime()` calls on the IO hot path — skew has to be real to be worth that |
| `DedicatedIOWorkerLoadBalancer` | no session at all | a session's tail latency matters more than how many connections fit: it gets a thread to itself and nothing else interferes with it. Oversubscribe the group and strict pinning is gone |
| `NapIdLoadBalancer` | the same NIC RX queue, read from `SO_INCOMING_NAPI_ID` | Linux, a real NIC, and workers you have pinned. The sessions one queue feeds are then drained by one thread instead of scattered across cores |

None of them is a correctness decision. The last three fall back to the default whenever what they count is missing —
every worker still reporting zero load at startup, more sessions than workers, a socket the kernel has no queue for
yet — so a balancer that finds nothing to go on still spreads the load.

**Placement is once, unless you ask for more.** The balancer is consulted when the channel is registered and that is
final, until the group is given an `ioWorkersRebalanceInterval`. With one set, each balancer gets to revise itself on
a timer: the default drains workers holding more than their share, the load-aware one moves a single session per pass
so its EMA can absorb the last move before deciding the next, the dedicated one restores its 1:1 invariant, and the
NIC-aware one follows a session whose queue has changed.

**`NapIdLoadBalancer` needs that interval on the client side.** A NAPI ID names the RX queue a socket's packets
arrive on, and betty picks a worker before the channel is connected — so an outbound session has no ID yet and is
always placed by the fallback, with the rebalance moving it once the kernel has one. An accepted session is different:
its ID comes in with the SYN and is there at accept time. The ID names a queue and never a core, so CPU locality is
the operator's half of the job: pin the workers to the CPUs taking those queues' interrupts.
[`NapIdExample`](examples/src/main/java/org/lolaf/betty/examples/napid/NapIdExample.java) runs it over a real adapter
and prints what the kernel reports for every session.

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

On the 2026-09-12 run — 16-byte packets over loopback, one machine, one fork — betty's round-trip latency sits level
with a hand-rolled busy-spinning NIO client, 8.72 µs against 8.73, each inside the other's interval, and leads both
hand-rolled clients on throughput by less than those intervals overlap. In its low-latency configuration it is ahead
of Netty, Mina, Jetty and the JDK's own `AsynchronousSocketChannel` on latency; stock against stock, it and Netty land
inside each other's error bars. Aeron is nominally quickest at 6.86 µs, in the one configuration it is tuned for, but
at ±2.60 against betty's ±0.57 this run cannot place the two apart — while betty stays ahead of it on throughput in
both. Jetty on both benchmarks and Mina on throughput carry error bars wider than their own scores and the JDK client
on round-trip very nearly so, and Netty's throughput rows are wide in both selector configurations, so none of those
placements is resolved. The gap that is not close is allocation: 0.094 B/op per round-trip stock, 0.07 low-latency and
nothing measurable on the throughput benchmark, against 165–632 B/op for Netty, Mina, Jetty, the JDK client and Aeron
in the configuration Aeron is quickest in — which is the whole point of the buffer pooling.

**Those numbers are one machine, and both benchmarks pin threads to cores derived from `availableProcessors()`, so
two machines are not comparable.** Run them on your own hardware before believing any of it.

[`benchmarks/README.md`](benchmarks/README.md) has the full tables, what each number does and does not support, the
parameter matrix, which client supports which combination, and why running the whole jar at once logs errors by
design. The raw JSON of every run is in [`benchmarks/results/`](benchmarks/results); drop a file on
<https://jmh.morethan.io/> to read it.
