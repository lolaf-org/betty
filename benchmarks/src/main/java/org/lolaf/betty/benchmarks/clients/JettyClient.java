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
import org.eclipse.jetty.io.*;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.Promise;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@Slf4j
@State(Scope.Benchmark)
public class JettyClient extends AbstractClientBenchmark {
    private ClientConnector clientConnector;
    private QueuedThreadPool threadPool;

    @Override
    public boolean isSupported(ClientSettings settings) {
        return ClientSettings.STOCK.equals(settings);
    }

    @Override
    public void setupClient(ClientSettings clientSettings) throws IOException {
        clientConnector = new ClientConnector();
        threadPool = new QueuedThreadPool(3);
        threadPool.setName("jetty-client");
        threadPool.setDaemon(true);
        clientConnector.setExecutor(threadPool);
        clientConnector.setByteBufferPool(new ArrayByteBufferPool());
        clientConnector.setTCPNoDelay(true);

        try {
            threadPool.start();
            clientConnector.start();
        } catch (Exception e) {
            throw new IOException(e);
        }

        ClientConnectionFactory connectionFactory = (endPoint, context) ->
        {
            log.info("Creating connection for {}", endPoint);
            return new RttConnection(endPoint, clientConnector.getExecutor(), clientConnector.getByteBufferPool(), READ_BUFFER_SIZE);
        };
        CompletableFuture<RttConnection> connectionPromise = new Promise.Completable<>();
        connectionPromise.whenComplete((connection, failure) -> log.info("Created connection for {}", connection));
        Map<String, Object> context = new HashMap<>();
        context.put(Transport.class.getName(), Transport.TCP_IP);
        context.put(ClientConnectionFactory.CONTEXT_KEY, connectionFactory);
        context.put(ClientConnector.CONNECTION_PROMISE_CONTEXT_KEY, connectionPromise);
        clientConnector.connect(BenchmarkServer.CONNECT_ADDRESS, context);
    }

    @Override
    public void shutdownClient() throws IOException {
        try {
            clientConnector.stop();
            threadPool.stop();
            clientConnector.destroy();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private static class RttConnection extends AbstractConnection {
        private final RetainableByteBuffer retainableIn;
        private final ByteBufferPool pool;
        private final Callback writeCallback = new Callback() {
        };

        public RttConnection(EndPoint endPoint, Executor executor, ByteBufferPool pool, int readBufferSize) {
            super(endPoint, executor);
            this.retainableIn = pool.acquire(readBufferSize, true);
            this.pool = pool;
        }

        @Override
        public void onOpen() {
            super.onOpen();
            fillInterested();
        }

        @Override
        public void onClose(Throwable cause) {
            log.info("On close", cause);
        }

        @Override
        public void onFillable() {
            int filled;
            ByteBuffer in = retainableIn.getByteBuffer().clear();
            try {
                filled = getEndPoint().fill(in.flip());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            if (filled > 0) {
                RetainableByteBuffer retainableOut = pool.acquire(filled, true);
                ByteBuffer out = retainableOut.getByteBuffer().clear();
                while (in.remaining() >= BenchmarkServer.PACKET_SIZE) {
                    out.putLong(in.getLong()).putLong(in.getLong());
                }
                getEndPoint().write(writeCallback, out.flip());
                fillInterested();
            } else if (filled == 0) {
                log.info("Nothing filled, abnormal");
                fillInterested();
            } else {
                getEndPoint().close();
            }
        }
    }
}
