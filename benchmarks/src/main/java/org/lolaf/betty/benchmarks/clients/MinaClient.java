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

import lombok.extern.slf4j.Slf4j;
import org.apache.mina.core.buffer.CachedBufferAllocator;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.FilterEvent;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.lolaf.betty.benchmarks.impl.AbstractClientBenchmark;
import org.lolaf.betty.benchmarks.impl.BenchmarkServer;
import org.lolaf.betty.benchmarks.impl.ClientSettings;

import java.io.IOException;
import java.nio.ByteBuffer;

@Slf4j
@State(Scope.Benchmark)
public class MinaClient extends AbstractClientBenchmark {
    NioSocketConnector connector;
    CachedBufferAllocator cachedBufferAllocator;

    @Override
    public boolean isSupported(ClientSettings settings) {
        return ClientSettings.STOCK.equals(settings);
    }

    @Override
    public void setupClient(ClientSettings clientSettings) throws IOException {
        connector = new NioSocketConnector();
        connector.getSessionConfig().setReceiveBufferSize(READ_BUFFER_SIZE);
        connector.getSessionConfig().setSendBufferSize(IO_WRITE_BUFFER_SIZE);
        cachedBufferAllocator = new CachedBufferAllocator(IO_WRITE_BUFFER_POOL_SIZE, IO_WRITE_BUFFER_SIZE);
        connector.setHandler(new IoHandler() {
            @Override
            public void sessionCreated(IoSession ioSession) {

            }

            @Override
            public void sessionOpened(IoSession ioSession) {

            }

            @Override
            public void sessionClosed(IoSession ioSession) {

            }

            @Override
            public void sessionIdle(IoSession ioSession, IdleStatus idleStatus) {

            }

            @Override
            public void exceptionCaught(IoSession ioSession, Throwable throwable) {

            }

            @Override
            public void messageReceived(IoSession ioSession, Object o) {
                IoBuffer ioBuffer = (IoBuffer) o;
                ByteBuffer in = ioBuffer.buf();
                IoBuffer out = cachedBufferAllocator.allocate(in.remaining(), true);
                while (in.remaining() >= BenchmarkServer.PACKET_SIZE) {
                    out.putLong(in.getLong()).putLong(in.getLong());
                }
                ioSession.write(out.flip());
            }

            @Override
            public void messageSent(IoSession ioSession, Object o) {
                if (o instanceof IoBuffer) {
                    ((IoBuffer) o).free();
                }
            }

            @Override
            public void inputClosed(IoSession ioSession) {

            }

            @Override
            public void event(IoSession ioSession, FilterEvent filterEvent) {

            }
        });
        connector.connect(BenchmarkServer.CONNECT_ADDRESS);
    }

    @Override
    public void shutdownClient() {
        connector.dispose();
        cachedBufferAllocator.dispose();
    }
}