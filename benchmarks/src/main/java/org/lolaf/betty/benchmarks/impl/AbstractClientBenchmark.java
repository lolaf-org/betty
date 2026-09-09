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
import net.openhft.affinity.Affinity;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;

@Slf4j
@State(Scope.Benchmark)
public class AbstractClientBenchmark {

    public static final int READ_BUFFER_SIZE = 8 * 1024;
    public static final int IO_WRITE_BUFFER_POOL_SIZE = 128;
    public static final int IO_WRITE_BUFFER_SIZE = READ_BUFFER_SIZE;

    public static final int IO_THREAD_CORE_AFFINITY = Runtime.getRuntime().availableProcessors() / 2 - 2;
    public static final int JMH_CORE_AFFINITY = Runtime.getRuntime().availableProcessors() / 2 - 1;

    BenchmarkServer server;

    @Param({"STOCK", "LOW_LATENCY"})
    ClientSettings clientSettings;

    @Param({"JDK_STOCK", "JDK_EPOLL"})
    ClientSelectorSettings clientSelectorSettings;

    public ClientSettings getClientSettings() {
        return clientSettings;
    }

    public ClientSelectorSettings getClientSelectorSettings() {
        return clientSelectorSettings;
    }

    public boolean enableBenchmarkThreadCoreAffinity() {
        return true;
    }

    @Setup
    public void setup() {
        if (enableBenchmarkThreadCoreAffinity()) {
            log.info("Setting JMH core affinity to core " + JMH_CORE_AFFINITY);
            Affinity.setAffinity(JMH_CORE_AFFINITY);
        }
        log.info("Starting benchmark for {}:{}, calling system gc", clientSettings, clientSelectorSettings);
        System.gc();
        log.info("System gc done");
        if (!isSupported(clientSettings)) {
            throw new IllegalStateException("Client settings " + clientSettings + " is not supported by client implementation " + this.getClass().getSimpleName());
        }
        String selectorProvider = clientSelectorSettings.equals(ClientSelectorSettings.JDK_EPOLL) ? "sun.nio.ch.EPollSelectorProvider" : "sun.nio.ch.PollSelectorProvider";
        System.setProperty("java.nio.channels.spi.SelectorProvider", selectorProvider);

        try {
            setupServer();
            setupClient(clientSettings);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to setup env", e);
        }
        waitForClientConnection();
    }

    public void setupServer() throws IOException {
        server = new BenchmarkServer();
        server.start();
    }

    public void waitForClientConnection() {
        server.waitForConnection();
    }

    public boolean isSupported(ClientSettings settings) {
        throw new IllegalStateException("Override me");
    }

    public void setupClient(ClientSettings clientSettings) throws IOException {
        throw new IllegalStateException("Override me");
    }

    public void shutdownClient() throws IOException {
        throw new IllegalStateException("Override me");
    }

    @TearDown
    public void tearDown() {
        log.info("Shutdown client");
        try {
            shutdownClient();
        } catch (IOException e) {
            log.info("Failed to stop client {}", e.getMessage());
        }
        log.info("Shutting down server");
        try {
            shutdownServer();
        } catch (IOException e) {
            log.info("Failed to stop server {}", e.getMessage());
        }
    }

    public void shutdownServer() throws IOException {
        server.shutdown();
    }

    public void serverSendMessageAndWaitsForAck() {
        server.sendNextMessage(System.nanoTime());
        server.flush();
        server.waitForReceivedLastSentSequence();
    }

    public void serverSendMessagesAndWaitsForAck(int messagesCount) {
        long time = System.nanoTime();
        for (int i = 0; i < messagesCount; i++) {
            server.sendNextMessage(time);
        }
        server.flush();
        server.waitForReceivedLastSentSequence();
    }
}