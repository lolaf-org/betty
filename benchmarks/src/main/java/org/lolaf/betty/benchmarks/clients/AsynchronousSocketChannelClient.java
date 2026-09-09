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
package org.lolaf.betty.benchmarks.clients;

import org.lolaf.betty.benchmarks.impl.AbstractClientBenchmark;
import org.lolaf.betty.benchmarks.impl.BenchmarkServer;
import org.lolaf.betty.benchmarks.impl.ClientSettings;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;

@Slf4j
@State(Scope.Benchmark)
public class AsynchronousSocketChannelClient extends AbstractClientBenchmark {

    private AsynchronousSocketChannel channel;

    @Override
    public boolean isSupported(ClientSettings settings) {
        return settings.equals(ClientSettings.STOCK);
    }

    @Override
    public void setupClient(ClientSettings clientSettings) throws IOException {
        channel = AsynchronousSocketChannel.open();
        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        channel.connect(BenchmarkServer.CONNECT_ADDRESS);
        setupAsyncClientLogic();
    }

    private void setupAsyncClientLogic() {
        ByteBuffer in = ByteBuffer.allocateDirect(READ_BUFFER_SIZE);
        CompletionHandler<Integer, ByteBuffer> voidWriteHandler = new CompletionHandler<>() {
            @Override
            public void completed(Integer result, ByteBuffer attachment) {

            }

            @Override
            public void failed(Throwable exc, ByteBuffer attachment) {
                log.error("Write failed", exc);
            }
        };
        CompletionHandler<Integer, Void> readHandler = new CompletionHandler<>() {

            @Override
            public void completed(Integer result, Void att) {
                if (result == -1) {
                    return;
                }
                in.flip();
                ByteBuffer out = ByteBuffer.allocateDirect(in.remaining());
                while (in.remaining() >= BenchmarkServer.PACKET_SIZE) {
                    out.putLong(in.getLong()).putLong(in.getLong());
                }
                channel.write(out.flip(), out, voidWriteHandler);
                channel.read(in.compact(), null, this);
            }

            @Override
            public void failed(Throwable exc, Void att) {
                // don't care
            }
        };
        channel.read(in, null, readHandler);
    }

    @Override
    public void shutdownClient() throws IOException {
        channel.close();
    }
}
