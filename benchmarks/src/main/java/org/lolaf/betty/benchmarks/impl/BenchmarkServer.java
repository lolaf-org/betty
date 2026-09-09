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

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public final class BenchmarkServer extends Thread {

    public static final int PACKET_SIZE = 16;
    private static final int PORT = 9000;
    public static final InetSocketAddress CONNECT_ADDRESS = new InetSocketAddress("localhost", PORT);
    private final ByteBuffer outBuffer;
    private final ServerSocket server;
    private DataInputStream in;
    private DataOutputStream out;
    private long sequence = 0;

    public BenchmarkServer() {
        this.outBuffer = ByteBuffer.allocate(PACKET_SIZE);
        super.setDaemon(true);
        try {
            server = new ServerSocket(PORT);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    void waitForConnection() {
        while (in == null || out == null) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    void shutdown() {
        try {
            server.close();
        } catch (IOException e) {
            // don't care
        }
        this.interrupt();
        try {
            this.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    void waitForReceivedLastSentSequence() {
        while (true) {
            try {
                in.readLong(); // system time
                long remoteAckedSequence = in.readLong();
                if (remoteAckedSequence == sequence) {
                    return;
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    void sendNextMessage(long systemTime) {
        try {
            out.write(outBuffer.clear().putLong(systemTime).putLong(++sequence).array());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    void flush() {
        try {
            out.flush();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void run() {
        try {
            log.info("Server started");
            while (true) {
                final Socket c = server.accept();
                log.info("Incoming connection {}", c.getRemoteSocketAddress());
                c.setTcpNoDelay(true);
                try {
                    out = new DataOutputStream(new BufferedOutputStream(c.getOutputStream(), 8192));
                    in = new DataInputStream(new BufferedInputStream(c.getInputStream(), 8192));
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        } catch (IOException e) {
            // don't care happens when server is closed
        }
    }
}
