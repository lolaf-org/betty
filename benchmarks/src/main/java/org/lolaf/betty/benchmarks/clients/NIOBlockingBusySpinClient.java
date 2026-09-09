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
import net.openhft.affinity.Affinity;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

@Slf4j
@State(Scope.Benchmark)
public class NIOBlockingBusySpinClient extends AbstractClientBenchmark {

    private SocketChannel channel;

    @Override
    public boolean isSupported(ClientSettings settings) {
        return settings.equals(ClientSettings.LOW_LATENCY);
    }

    void configureBlocking(SocketChannel channel) throws IOException {
        channel.configureBlocking(true);
    }

    @Override
    public void setupClient(ClientSettings clientSettings) throws IOException {
        channel = SocketChannel.open();
        configureBlocking(channel);
        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        if (!channel.connect(BenchmarkServer.CONNECT_ADDRESS)) {
            channel.finishConnect();
        }
        Thread clientRunner = new Thread(() -> {
            log.info("Setting IO core affinity to core " + IO_THREAD_CORE_AFFINITY);
            Affinity.setAffinity(AbstractClientBenchmark.IO_THREAD_CORE_AFFINITY);
            ByteBuffer in = ByteBuffer.allocateDirect(READ_BUFFER_SIZE);
            ByteBuffer out = ByteBuffer.allocateDirect(IO_WRITE_BUFFER_SIZE);
            int size;
            try {
                while (-1 != (size = channel.read(in))) {
                    if (size < BenchmarkServer.PACKET_SIZE) {
                        continue;
                    }
                    in.flip();
                    out.clear();
                    while (in.remaining() >= BenchmarkServer.PACKET_SIZE) {
                        out.putLong(in.getLong()).putLong(in.getLong());
                    }
                    channel.write(out.flip());
                    in.compact();
                }
            } catch (ClosedChannelException ex) {
                // don't care
            } catch (IOException ex) {
                log.error("Error processing data", ex);
            }
            log.info("Exiting NIOBusySpinClient thread");
        });
        clientRunner.setDaemon(true);
        clientRunner.start();
    }

    @Override
    public void shutdownClient() throws IOException {
        channel.close();
    }
}
