/*
 * Copyright © 2024-2026 Lolaf.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lolaf.betty.benchmarks;

import lombok.extern.slf4j.Slf4j;
import org.lolaf.betty.benchmarks.clients.*;
import org.openjdk.jmh.annotations.*;

@Slf4j
@Warmup(iterations = 3)
@Measurement(iterations = 3)
@Fork(value = 1, jvmArgsPrepend = {
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "-Xmx8g", "-Xms8g"})
public class ClientThroughputBenchmark {

    private static final int MSG_COUNT = 1024;

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(MSG_COUNT)
    public void betty1k16Bytes(BettyClient client) {
        client.serverSendMessagesAndWaitsForAck(MSG_COUNT);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(MSG_COUNT)
    public void netty1k16Bytes(NettyClient client) {
        client.serverSendMessagesAndWaitsForAck(MSG_COUNT);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(MSG_COUNT)
    public void aeron1k16Bytes(AeronClient client) {
        client.serverSendMessagesAndWaitsForAck(MSG_COUNT);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(MSG_COUNT)
    public void mina1k16Bytes(MinaClient client) {
        client.serverSendMessagesAndWaitsForAck(MSG_COUNT);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(MSG_COUNT)
    public void jetty1k16Bytes(JettyClient client) {
        client.serverSendMessagesAndWaitsForAck(MSG_COUNT);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(MSG_COUNT)
    public void asynchronousSocketChannel1k16Bytes(AsynchronousSocketChannelClient client) {
        client.serverSendMessagesAndWaitsForAck(MSG_COUNT);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(MSG_COUNT)
    public void NIOBlockingBusySpin1k16Bytes(NIOBlockingBusySpinClient client) {
        client.serverSendMessagesAndWaitsForAck(MSG_COUNT);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(MSG_COUNT)
    public void NIONonBlockingBusySpin1k16Bytes(NIONonBlockingBusySpinClient client) {
        client.serverSendMessagesAndWaitsForAck(MSG_COUNT);
    }
}