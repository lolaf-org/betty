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

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hands out the TCP ports the tests bind, so that no test has to hardcode one: a fixed port makes two test classes
 * unable to run at the same time, and lets any stray process on the machine fail the build.
 */
public final class TestPorts {

    static final String FORCED_OFFSET_PROPERTY = "betty.test.ports.offset";
    /**
     * Ports are allocated from below the OS ephemeral range (32768 and up on Linux) so that the kernel never hands one
     * of them out as the source port of an outgoing connection, which would make a later bind fail.
     */
    private static final int FIRST_PORT = 20_000;
    private static final int PORTS_COUNT = 12_768;
    /**
     * How far ahead a refused candidate jumps. Every JVM walks its ports one by one, so the ones another JVM holds
     * come in runs: stepping through such a run one port at a time is how a JVM used to exhaust its attempts against
     * a build mate rather than against a busy machine.
     */
    private static final int REFUSED_CANDIDATE_JUMP = 61;
    /**
     * Randomized so two concurrent builds on the same machine do not walk the same sequence. Free running and mapped
     * into the range above when read, so that a walk that reaches the top carries on from the bottom.
     */
    private static final AtomicInteger NEXT_PORT = new AtomicInteger(initialOffset());

    /**
     * Where the machine-wide reservations live, one file per port. Shared by every test JVM on the machine, which is
     * the point of it.
     */
    private static final Path RESERVATIONS_DIRECTORY = Paths.get(System.getProperty("java.io.tmpdir"), "betty-test-ports");

    private TestPorts() {
    }

    /**
     * Returns a port no caller in this JVM has been given before, that no other test JVM on the machine has been given
     * either, and that nothing else on the machine is listening on.
     * <p>
     * {@code new ServerSocket(0)} is deliberately not used: it allocates from the ephemeral range, which is also where
     * outgoing connections take their source port from, so a port handed out that way can be taken before the caller
     * binds it.
     * <p>
     * The probe below only says the port was free at the instant it ran, which is not what a caller needs: a build
     * runs several modules at once, each in its own surefire JVM, and a port is typically allocated in a {@code
     *
     * @BeforeEach} and bound seconds later when the acceptor starts. Another JVM probing in that gap finds the port
     * free and takes it, and whoever binds second gets a BindException - which is what a random base per JVM only
     * makes rarer, all of them walking the same ports. So the port is reserved through {@link #reserve(int)} before
     * being probed, and the reservation is held until this JVM exits. That reservation is what keeps a port to a
     * single caller, in this JVM as well as across them.
     */
    public static int findFree() {
        for (int attempt = 0; attempt < 200; attempt++) {
            int port = nextCandidate(1);
            if (!reserve(port)) {
                // another test JVM on this machine holds it, and probably the ports after it too
                nextCandidate(REFUSED_CANDIDATE_JUMP);
                continue;
            }
            try (ServerSocket probe = new ServerSocket(port)) {
                return probe.getLocalPort();
            } catch (IOException alreadyBound) {
                // held by a process that is not one of ours, try the next candidate
            }
        }
        throw new IllegalStateException("no free port found below the ephemeral range");
    }

    private static int nextCandidate(int step) {
        return FIRST_PORT + Math.floorMod(NEXT_PORT.getAndAdd(step), PORTS_COUNT);
    }

    private static int initialOffset() {
        String forcedOffset = System.getProperty(FORCED_OFFSET_PROPERTY);
        return forcedOffset != null ? Integer.parseInt(forcedOffset) : new SecureRandom().nextInt(PORTS_COUNT);
    }

    /**
     * Takes the machine-wide reservation for a port, as an exclusive lock on a file named after it. The lock is a
     * process-wide one the OS drops when this JVM exits, so a reservation cannot outlive the build that took it and
     * the files left behind mean nothing on their own.
     *
     * @return {@code false} if another JVM holds the port, and the caller should move on to the next candidate
     */
    private static boolean reserve(int port) {
        FileChannel channel = null;
        try {
            Files.createDirectories(RESERVATIONS_DIRECTORY);
            channel = FileChannel.open(RESERVATIONS_DIRECTORY.resolve(port + ".lock"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            FileLock lock = channel.tryLock();
            if (lock == null) {
                channel.close();
                return false;
            }
            return true;
        } catch (OverlappingFileLockException alreadyHeldByThisJvm) {
            closeQuietly(channel);
            return false;
        } catch (IOException noReservationPossible) {
            // nowhere to write them, or a directory another user owns: fall back on the probe alone rather than
            // failing a build over a port that is very probably free
            closeQuietly(channel);
            return true;
        }
    }

    private static void closeQuietly(FileChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
                // nothing useful to do about it while handing out a port
            }
        }
    }
}
