# Betty Examples

Runnable examples. Nothing here is published: the module exists to be read and run.

```bash
./mvnw -T 1C clean install     # from the repository root
java -jar examples/target/ping-example.jar
java -jar examples/target/napid-example.jar
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

## NAPI ID placement

[`NapIdExample`](src/main/java/org/lolaf/betty/examples/napid/NapIdExample.java) runs a server and four clients on
`NapIdLoadBalancer`, binds to the host's first real adapter instead of loopback, and prints every session's NAPI ID
every five seconds so the placement can be checked rather than assumed:

```text
10:53:01.393 [main] INFO NapIdExample - adapter wlp0s20f3 at 192.168.1.134 with 1 RX queue(s)
10:53:06.437 [napid-reporter] INFO NapIdExample - IO-worker-napid-0: napid-server:/192.168.1.134:59440 napId=0
10:53:06.438 [napid-reporter] INFO NapIdExample - IO-worker-napid-0: napid-client-3 napId=0
10:53:06.438 [napid-reporter] INFO NapIdExample - IO-worker-napid-1: napid-server:/192.168.1.134:59464 napId=0
```

**Those zeros are the point.** A NAPI ID names the RX queue a socket's packets arrived on, and packets that never
reach a NIC have none. Two processes on the same host are delivered locally whatever address they use — binding to
`192.168.1.x` rather than `127.0.0.1` changes nothing — so the single-JVM run above reports `0` for every session
and `MinRegisteredSessionLoadBalancer` places them all. Put the two halves on two machines and the IDs appear:

```bash
host A:  java -cp examples/target/napid-example.jar org.lolaf.betty.examples.napid.NapIdExample server
host B:  java -cp examples/target/napid-example.jar org.lolaf.betty.examples.napid.NapIdExample client 192.168.1.x
```

A second machine running betty is the point, but not the cheapest way to watch the balancer place by ID: any TCP
server on another machine will do, because the ID is set by the packets coming back and not by what is in them.
Pointing the clients at a router's HTTP port is enough:

```text
java -jar examples/target/napid-example.jar client 192.168.1.1 80

11:01:50.888 [main] INFO NapIdExample - adapter wlp0s20f3 at 192.168.1.x with 1 RX queue(s)
11:01:55.927 [napid-reporter] INFO NapIdExample - IO-worker-napid-0: napid-client-1 napId=8205
11:01:55.927 [napid-reporter] INFO NapIdExample - IO-worker-napid-0: napid-client-2 napId=8196
11:01:55.927 [napid-reporter] INFO NapIdExample - IO-worker-napid-0: napid-client-3 napId=8205
11:01:55.927 [napid-reporter] INFO NapIdExample - IO-worker-napid-1: napid-client-0 napId=8204
```

That is the balancer doing its job: the two sessions on queue 8205 share a worker, and 8204 is on the other one. The
run also shows two things sysfs does not. The adapter reports one RX queue and three NAPI IDs — a wifi driver
registers NAPI instances the queue count knows nothing about — so size the group by the IDs you observe rather than
by the queue count alone. And every placement above was made by the fallback: betty picks a worker for a channel
that has not connected yet, so a client socket has no ID at placement time, and the periodic rebalance is what moved
`napid-client-0` onto worker-1 once the ID appeared. On the server side the ID comes in with the SYN and is there at
accept time.

An ID is an opaque number, not a queue index and not a CPU: the kernel allocates it from a global counter that
starts above `NR_CPUS` precisely so it can never be mistaken for a CPU id, so the first queue on a kernel built with
`CONFIG_NR_CPUS=8192` reports 8193. The balancer therefore hands each distinct ID the next slot it has and places on
`slot % workerCount`; what it guarantees is that the sessions sharing a queue share a worker, not that the worker
runs on any particular core. Pinning the workers to the CPUs taking those queues' interrupts is the separate,
operator-side half of the story, and `/proc/interrupts` with `/proc/irq/<n>/smp_affinity_list` is where it is
decided.

The example sizes the group from the adapter's queue count — `ls /sys/class/net/<dev>/queues/` shows it, `ethtool -l
<dev>` changes it — because that is how many distinct IDs its sessions can ever report. Placement at connect time
can still see a `0`, which is why the group sets `ioWorkersRebalanceInterval`: the periodic rebalance is what moves
a session onto the right worker once the kernel has an ID for it.

## Watermarks and a slow consumer

[`SlowConsumerExample`](src/main/java/org/lolaf/betty/examples/watermark/SlowConsumerExample.java) runs a server
producing as fast as the socket will take bytes against a client that consumes 256 KB a second and stops selecting
until the next one. `writeHighWatermark` and `writeLowWatermark` are what keep the two in step:

```text
high watermark 512 KB, low watermark 64 KB, client consuming 256 KB every 1000 ms - Ctrl-C to stop
[watermark-producer]     HIGH watermark with 512 KB in flight - producer throttled
[watermark-reporter]     produced 1008 KB/s, consumed 261 KB/s, producer throttled
[watermark-reporter]     produced 0 KB/s, consumed 307 KB/s, producer throttled
[IO-worker-...-server-0] LOW watermark with 64 KB in flight - producer resumed
[watermark-producer]     HIGH watermark with 512 KB in flight - producer throttled
[watermark-reporter]     produced 512 KB/s, consumed 226 KB/s, producer throttled
```

Betty counts the bytes a session has accepted for writing but has not yet put on the socket. Crossing the high mark
calls `onWatermarkEvent` with `true`, falling back under the low mark calls it with `false`, and each edge is
reported once — which is what makes the pair usable as a switch. The producer here stops on the first and starts
again on the second, so the backlog oscillates between the two marks and, averaged over a cycle, the server sends at
exactly the rate the client reads. Nothing else in the example limits it.

Two settings exist only to keep the watermark in charge of that. `tasksRingBufferSize` and the write pool both have
to hold every chunk the high watermark allows in flight, or the producer blocks on a full ring before the watermark
ever fires, and the throttling you would be watching would be the ring's. Small `SO_SNDBUF` and `SO_RCVBUF` keep the
backlog in betty, where `onWatermarkEvent` can see it, instead of in the kernel where it cannot.

The event is advisory: nothing refuses a send for being over the mark. Ignore it and the producer keeps borrowing
buffers for data the socket cannot take, and the write pool — then the heap behind it — is what pays for the client
being slow.

## Running it the way you would run production

The shaded jar carries `Add-Opens: java.base/jdk.internal.misc` in its manifest, which is what lets ringos reach
`Unsafe` and betty install its selector optimization — pass it on the command line for an application of your own.
The one thing the manifest cannot set is `@Contended` padding, so the example logs an error about head and tail
sharing a cache line until you ask the JVM for it:

```bash
java -XX:-RestrictContended -XX:ContendedPaddingWidth=64 -jar examples/target/ping-example.jar
```

Use your own L1 cache line size for the padding width.
