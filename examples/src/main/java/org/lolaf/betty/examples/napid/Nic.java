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
package org.lolaf.betty.examples.napid;

import lombok.Value;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * The host's first real network adapter, found so the example can bind to something other than loopback.
 *
 * <p>Everything here reads Linux's sysfs, which is where the interesting part is: an interface has a {@code device}
 * symlink only when a driver sits behind it, which is what separates a NIC from a bridge like {@code docker0} or one
 * half of a veth pair, and {@code queues/rx-*} is how many RX queues it has. The queue count is only a starting point
 * for the worker count: it matches the number of distinct NAPI IDs on an ethernet NIC doing RSS, but a wifi driver
 * registers NAPI instances of its own and reports more IDs than sysfs shows queues.
 */
@Value
class Nic {

    String name;
    InetAddress address;
    int rxQueues;

    static Nic firstPhysical() throws SocketException {
        List<NetworkInterface> usable = new ArrayList<>();
        for (NetworkInterface candidate : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            if (!candidate.isLoopback() && candidate.isUp() && ipv4Of(candidate).isPresent()) {
                usable.add(candidate);
            }
        }
        NetworkInterface nic = usable.stream()
                .filter(Nic::isDriverBacked)
                .findFirst()
                .orElseGet(() -> usable.stream()
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("no non-loopback IPv4 interface is up")));
        return new Nic(nic.getName(), ipv4Of(nic).orElseThrow(IllegalStateException::new), rxQueuesOf(nic.getName()));
    }

    private static Optional<InetAddress> ipv4Of(NetworkInterface nic) {
        return Collections.list(nic.getInetAddresses()).stream()
                .filter(Inet4Address.class::isInstance)
                .findFirst();
    }

    private static boolean isDriverBacked(NetworkInterface nic) {
        return Files.exists(Paths.get("/sys/class/net", nic.getName(), "device"));
    }

    private static int rxQueuesOf(String name) {
        Path queues = Paths.get("/sys/class/net", name, "queues");
        try (Stream<Path> entries = Files.list(queues)) {
            return (int) entries.filter(queue -> queue.getFileName().toString().startsWith("rx-")).count();
        } catch (IOException e) {
            return 1;
        }
    }
}
