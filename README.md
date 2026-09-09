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
                .tasksRingBufferSize(16 * 1024)
                .writeIoBufferPoolSettings(IOBufferPoolSettings.builder()
                        .zone(IOBufferPoolSettings.IOBufferPoolZone.builder()
                                .poolSize(8192)
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

`betty-benchmarks` holds JMH comparisons against Aeron, Netty, Jetty and Mina. It is not published.

```bash
./mvnw -T 1C clean install && java -jar benchmarks/target/benchmarks.jar
```
