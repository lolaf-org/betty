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
package org.lolaf.betty.impl;

import org.lolaf.betty.api.Client;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.Server;
import org.lolaf.betty.api.ServerBuilder;
import org.lolaf.betty.api.settings.IOBufferPoolSettings;
import org.lolaf.betty.api.settings.IOSettings;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class Profiling {

    public static void main(String[] args) {
        byte[] world = "world".getBytes();

        Server srv = ServerBuilder.builder().ioEventsListener((session, message, localReceiveTimeInNanos) -> {
                    message.position(message.limit());
                    session.send(world);
                }).bindAddress(new InetSocketAddress("localhost", 9090))
                .ioSettings(IOSettings.builder()
                        .tasksRingBufferSize(16 * 1024)
                        .writeIoBufferPoolSettings(IOBufferPoolSettings.builder()
                                .zone(IOBufferPoolSettings.IOBufferPoolZone.builder()
                                        .poolSize(8192)
                                        .bufferSize(1024)
                                        .build()).build())
                        .build())
                .id("profiling-server")
                .build().newInstance().start();


        Client client = ClientBuilder.builder()
                .connectAddress(new InetSocketAddress("localhost", 9090))
                .ioEventsListener((session, message, localReceiveTimeInNanos) -> {
                    message.position(message.limit());
                })
                .ioSettings(IOSettings.builder()
                        .tasksRingBufferSize(16 * 1024)
                        .writeIoBufferPoolSettings(IOBufferPoolSettings.builder()
                                .zone(IOBufferPoolSettings.IOBufferPoolZone.builder()
                                        .poolSize(8192)
                                        .bufferSize(1024)
                                        .build()).build())
                        .build())
                .id("profiling-client")
                .build().newInstance().start();

        while (!client.isConnected()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }

        byte[] hello = "hello".getBytes();
        for (int j = 0; j < 1000; j++) {
            log.info("Start sending");
            for (int i = 0; i < 1024 * 1024; i++) {
                client.getIOSession().send(hello);
                if (i % 64 == 0) {
                    LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(50));
                }
            }
            client.getIOSession().waitForAllMessagesSent(org.lolaf.ringos.Deadline.of(Duration.ofSeconds(5)));
            log.info("Finished sending");
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
        client.stop(org.lolaf.ringos.Deadline.of(Duration.ofSeconds(10)));
        srv.stop(org.lolaf.ringos.Deadline.of(Duration.ofSeconds(10)));
    }
}