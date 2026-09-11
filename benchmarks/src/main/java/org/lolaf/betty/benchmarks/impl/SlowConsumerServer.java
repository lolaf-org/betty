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
package org.lolaf.betty.benchmarks.impl;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * A peer that reads slower than a writer can write, so the writer's socket keeps refusing bytes.
 *
 * <p>{@link BenchmarkServer} drains as fast as it is fed and so never stalls a write. This one pauses between reads,
 * which with a small receive buffer is enough to close the window: the writer then sees partial writes and
 * zero-length writes, the state betty has to carry across write cycles.
 */
@Slf4j
public final class SlowConsumerServer extends Thread {

    private static final int PORT = 9001;

    public static final InetSocketAddress CONNECT_ADDRESS = new InetSocketAddress("localhost", PORT);

    /** Small enough that the window closes while the reader is asleep rather than after it. */
    public static final int SOCKET_RECEIVE_BUFFER_SIZE = 32 * 1024;

    private static final int READ_CHUNK_SIZE = 8 * 1024;

    private static final long READ_PAUSE_NANOS = TimeUnit.MICROSECONDS.toNanos(20);

    private final ServerSocket server;
    private final LongAdder bytesRead = new LongAdder();
    private volatile boolean running = true;
    private volatile boolean connected;

    public SlowConsumerServer() {
        setDaemon(true);
        setName("slow-consumer-server");
        try {
            server = new ServerSocket();
            // on the listening socket, so the accepted one inherits it before the handshake sets the window
            server.setReceiveBufferSize(SOCKET_RECEIVE_BUFFER_SIZE);
            server.bind(CONNECT_ADDRESS);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public long getBytesRead() {
        return bytesRead.sum();
    }

    public void waitForConnection() {
        while (!connected) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    public void shutdown() {
        running = false;
        try {
            server.close();
        } catch (IOException e) {
            // don't care
        }
        interrupt();
        try {
            join(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void run() {
        byte[] chunk = new byte[READ_CHUNK_SIZE];
        while (running) {
            try (Socket socket = server.accept(); InputStream in = socket.getInputStream()) {
                connected = true;
                while (running) {
                    int read = in.read(chunk);
                    if (read < 0) {
                        break;
                    }
                    bytesRead.add(read);
                    LockSupport.parkNanos(READ_PAUSE_NANOS);
                }
            } catch (IOException e) {
                if (running) {
                    log.info("Slow consumer connection ended: {}", e.getMessage());
                }
            } finally {
                connected = false;
            }
        }
    }
}
