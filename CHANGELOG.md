# Changelog

All notable changes to this project are recorded here, in the format of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Each released version needs its own `## [x.y.z] - YYYY-MM-DD` heading here **before** the release is
cut: the release workflow refuses to run without one, and the GitHub Release for the tag is created
with that section as its body. Write it in the commit that precedes the release, together with the
matching `[x.y.z]:` link definition at the foot of the file.

## [0.9.2] - 2026-09-26

### Added

- **`IOSession.isConnected()`**, true from `IOEventsListener.onConnected` until `IOEventsListener.onDisconnected`.

### Fixed

- **`processTask` rejects a task once the session has no IO thread left.** A task submitted from another thread to a
  disconnected session, or one whose IO worker is gone, is handed to its callback with an `EOFException` instead of
  being queued where nothing would ever run it. Once the queue was full, such a call used to block forever.
- **`stop()` on a session is more reliable.** It no longer closes the connection in an unsafe way after a short timeout;
  it waits, up to 30 seconds, until the session is fully closed. Stopping an `IOWorker` now also closes the sessions still
  running on it, and a session rejected by the `RemoteSessionsFilter` no longer receives `onShutdown`.

## [0.9.1] - 2026-09-21

### Fixed

- **A session is disconnected once.** Interrupting a `stop()` could fire `onDisconnected` twice and make a client dial
  twice.
- **Cancelling a pending reconnection no longer interrupts the client's scheduler thread**, which could break a task
  the application was running on the same scheduler.
- **A `ReleasableMessageSendingContext` is released when a write is refused up front.** A send to a stopped session,
  and a future-based send from the IO thread that found the write ring full, failed without releasing its context,
  so a pooled context was never returned to its pool.

## [0.9.0] - 2026-09-12

First public release. Betty is asynchronous NIO networking for Java that must not allocate or block on its hot path -
the transport under a low-latency trading stack. It runs on Java 11 or later, is compiled to Java 11 bytecode, and
depends on [ringos](https://github.com/lolaf-org/ringos) and SLF4J and nothing else.

### Added

- **Two artifacts.** `betty-api` carries `Client`, `Server` and their builders, `IOSession`, `IOWorker`,
  `IOWorkersGroup`, the `IOEventsListener` callbacks, the settings types, select strategies and `IOStats`, and holds
  no networking code at all; `betty-impl` is the NIO implementation, selected at runtime through a `ServiceLoader`
  lookup, so nothing in your code ends up naming an internal class. Compile against the first and put the second on
  the runtime classpath.
- **Pooled buffers on both directions.** A read and a write allocate nothing: the buffer handed to `onRead` belongs
  to the pool and is recycled when the callback returns, and a buffer borrowed for a write is returned by betty
  rather than by you. A pool's zones are offered a borrow in declaration order, so declare them smallest first.
- **`MessageSentCallback` and `ByteBufferBuilder`**, the allocation-free alternatives to a `CompletableFuture`: the
  builder defers building until the IO thread is about to write, which is what lets a protocol number its messages in
  send order. A `ReleasableMessageSendingContext` is released once the write is over, on the failure path as well as
  the success one.
- **Select strategies, set per `IOThreadGroup`** rather than per group of workers, so one client or server can spend
  a core on the sessions that matter and block on the rest. `WakeupSelectStrategy` is the default and blocks in
  `select()`; `IdleStrategySelectStrategy` never blocks and takes a ringos `IdleStrategy` to decide how hard to wait,
  so a cross-thread write needs no selector wakeup.
- **Four load balancers** placing sessions across an `IOWorkersGroup`: fewest registered sessions (the default),
  lowest EMA of IO CPU time, one session per worker, and `NapIdLoadBalancer`, which reads the NIC's
  `SO_INCOMING_NAPI_ID` so the sessions one RX queue feeds are drained by one thread. The last three fall back to the
  default whenever what they count is unavailable, so none of them is a correctness decision. Placement is once
  unless an `ioWorkersRebalanceInterval` is set, and then each balancer revises itself on that timer.
- **Write watermarks and back pressure**, so a slow consumer is visible before it is a problem, and an optional
  ordered-writes guarantee for protocols that cannot have an IO-thread send overtake what another thread queued.
- **TLS** through an `SSLContext` you supply, with the peer certificates handed to you and a remote-sessions filter
  consulted after the handshake.
- **`IOStats`**, which is the clock the library reads through rather than an accumulator: an implementation that does
  not time an operation returns a constant and no clock call is made for it, so measurement is genuinely free when it
  is off. A receive timestamp is handed to `onRead` under its own setting.

[0.9.1]: https://github.com/lolaf-org/betty/releases/tag/v0.9.1
[0.9.0]: https://github.com/lolaf-org/betty/releases/tag/v0.9.0
