# Betty Examples

Runnable examples. Nothing here is published: the module exists to be read and run.

```bash
./mvnw -T 1C clean install     # from the repository root
java -jar examples/target/ping-example.jar
```

## Ping

[`PingPongExample`](src/main/java/org/lolaf/betty/examples/ping/PingPongExample.java) starts a betty server and a
betty client in one JVM, and bounces a 16-byte frame — a send timestamp and a sequence number — between them once a
second:

```text
22:02:52.078 [IO-worker-ping-client-0] INFO PingPongExample - connected to localhost/127.0.0.1:9099
22:02:52.078 [IO-worker-ping-server-0] INFO PingPongExample - client connected from /127.0.0.1:44054
22:02:53.069 [IO-worker-ping-client-0] INFO PingPongExample - pong seq=1 rtt=2604us
22:02:54.066 [IO-worker-ping-client-0] INFO PingPongExample - pong seq=2 rtt=228us
22:02:55.066 [IO-worker-ping-client-0] INFO PingPongExample - pong seq=3 rtt=221us
```

Ctrl-C stops it. The first round trip pays for JIT and for the first buffers coming out of the pool, so it is always
the slow one. These are not benchmark numbers — `betty-benchmarks` is where measurement happens, on a tuned
configuration this example deliberately does not have.

Everything not needed to show a round trip is left at its default: no SSL, no buffer-pool sizing, no select
strategy, no thread affinity. What the example is actually there to show is three things.

**The buffer contract.** The `ByteBuffer` handed to `onRead` belongs to the pool and is recycled the moment the
callback returns. Read it inside the callback or copy it; never retain it.

**Framing.** TCP is a stream, so a read can carry several frames or stop part way through one. Both listeners
consume whole frames only:

```java
while (message.remaining() >= FRAME_SIZE) { ... }
```

Whatever is left unread is compacted to the front of the next read, so a partial frame needs no buffering of your
own — it simply comes back with the rest of itself.

**Where the delay lives.** The one-second wait is on a `ScheduledExecutorService`, never inside a callback: an IO
thread that sleeps stops serving every other session on that worker. Writing from that scheduler thread is safe
here because the settings are the defaults — `IOSettings.multiThreadedWriteAPICalls` is true and the write pool is
`MULTIPLE_BORROWER_THREADS`. Narrow either one and the send has to move onto the IO thread through
`IOSession.processTask`.

## Running it the way you would run production

The shaded jar carries `Add-Opens: java.base/jdk.internal.misc` in its manifest, which is what lets ringos reach
`Unsafe` and betty install its selector optimization — pass it on the command line for an application of your own.
The one thing the manifest cannot set is `@Contended` padding, so the example logs an error about head and tail
sharing a cache line until you ask the JVM for it:

```bash
java -XX:-RestrictContended -XX:ContendedPaddingWidth=64 -jar examples/target/ping-example.jar
```

Use your own L1 cache line size for the padding width.
