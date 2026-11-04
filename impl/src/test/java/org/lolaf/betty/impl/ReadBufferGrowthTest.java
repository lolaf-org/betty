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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.ServerBuilder;
import org.lolaf.betty.api.io.IOEventsListener;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.ReadBufferFullException;
import org.lolaf.betty.api.io.ReadBufferGrowthStrategy;
import org.lolaf.betty.api.settings.IOSettings;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

class ReadBufferGrowthTest extends AbstractTest {

    private static final int READ_BUFFER_SIZE = IOSettings.builder().build().getReadBufferSize();

    /**
     * Calls {@link #getTestParams()} once per buffer kind: each builder holds a port and a listener mock of its own.
     */
    static Stream<Arguments> getTestParamsWithDirectAndHeapBuffers() {
        return Stream.of(true, false).flatMap(direct -> getTestParams()
                .map(transport -> Arguments.of(transport.get()[0], transport.get()[1], direct)));
    }

    @ParameterizedTest(name = "[{index}] direct={2}")
    @MethodSource("getTestParamsWithDirectAndHeapBuffers")
    void aMessageLargerThanTheReadBufferIsReceivedWhole(ClientBuilder clientBuilder, ServerBuilder serverBuilder,
                                                        boolean direct) {
        setupTestEnvAndWaitForConnections(clientBuilder, serverBuilder.toBuilder()
                .ioSettings(IOSettings.builder().readDirectBuffer(direct).build()).build());
        Received received = trapWholeMessages(serverIoEventsListener);
        AtomicReference<Throwable> error = trapError(serverIoEventsListener);
        byte[] payload = payload(3 * READ_BUFFER_SIZE);

        clientIOsession.send(lengthPrefixed(payload));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(error).hasValue(null);
            assertThat(received.messages).singleElement().isEqualTo(payload);
        });
        assertThat(received.bufferDirectness).containsOnly(direct);
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void severalLargeMessagesInARowAreReceivedWhole(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        setupTestEnvAndWaitForConnections(clientBuilder, serverBuilder);
        Received received = trapWholeMessages(serverIoEventsListener);
        byte[] small = payload(100);
        byte[] large = payload(5 * READ_BUFFER_SIZE);

        for (byte[] message : List.of(large, small, large, large)) {
            clientIOsession.send(lengthPrefixed(message));
        }

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(received.messages).containsExactly(large, small, large, large));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void messagesThatFitNeverGrowTheBuffer(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        AtomicInteger growths = new AtomicInteger();
        ReadBufferGrowthStrategy counting = (unconsumed, capacity, max) -> {
            growths.incrementAndGet();
            return ReadBufferGrowthStrategy.DOUBLING.grow(unconsumed, capacity, max);
        };
        setupTestEnvAndWaitForConnections(clientBuilder, serverBuilder.toBuilder()
                .ioSettings(IOSettings.builder().readBufferGrowthStrategy(counting).build()).build());
        Received received = trapWholeMessages(serverIoEventsListener);

        for (int i = 0; i < 1000; i++) {
            clientIOsession.send(lengthPrefixed(payload(100)));
        }

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received.messages).hasSize(1000));
        assertThat(growths).hasValue(0);
    }

    /**
     * The maximum is above the TLS packet size, which is where the decoded buffer of a secure session starts.
     */
    @ParameterizedTest
    @MethodSource("getTestParams")
    void aMessagePastTheMaximumEndsTheSessionWithReadBufferFullException(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        ReadBufferFullException error = sessionEndOnReadBufferFull(clientBuilder, serverBuilder,
                IOSettings.builder().readBufferSize(4096).maxReadBufferSize(32 * 1024).build(), 32 * 1024);

        assertThat(error.getMaxReadBufferSize()).isEqualTo(32 * 1024);
    }

    /**
     * Without growth a secure session still holds a whole TLS packet, as its decoded buffer always did.
     */
    @ParameterizedTest
    @MethodSource("getTestParams")
    void withoutGrowthAMessageLargerThanTheReadBufferEndsTheSessionWithReadBufferFullException(
            ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        ReadBufferFullException error = sessionEndOnReadBufferFull(clientBuilder, serverBuilder,
                IOSettings.builder().readBufferSize(4096).maxReadBufferSize(4096).build(), 32 * 1024);

        assertThat(error.getMaxReadBufferSize()).isEqualTo(serverBuilder.getServerSSLSettings() == null
                ? 4096
                : getServerSSLContext().createSSLEngine().getSession().getPacketBufferSize());
    }

    @Test
    void aStrategyKnowingTheFramingGrowsTheBufferOnceForTheWholeMessage() {
        AtomicInteger growths = new AtomicInteger();
        ReadBufferGrowthStrategy exact = (unconsumed, capacity, max) -> {
            growths.incrementAndGet();
            return Integer.BYTES + unconsumed.getInt(0);
        };
        setupTestEnvAndWaitForConnections(getTestClientBuilder(), getTestServerBuilder().toBuilder()
                .ioSettings(IOSettings.builder().readBufferGrowthStrategy(exact).build()).build());
        Received received = trapWholeMessages(serverIoEventsListener);
        byte[] payload = payload(10 * READ_BUFFER_SIZE);

        clientIOsession.send(lengthPrefixed(payload));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received.messages).singleElement().isEqualTo(payload));
        assertThat(growths).hasValue(1);
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void aStrategyAskingTooLittleIsRaisedToWhatTheNextReadNeeds(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        setupTestEnvAndWaitForConnections(clientBuilder, serverBuilder.toBuilder()
                .ioSettings(IOSettings.builder().readBufferGrowthStrategy((unconsumed, capacity, max) -> capacity).build())
                .build());
        Received received = trapWholeMessages(serverIoEventsListener);
        byte[] payload = payload(3 * READ_BUFFER_SIZE);

        clientIOsession.send(lengthPrefixed(payload));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received.messages).singleElement().isEqualTo(payload));
    }

    private ReadBufferFullException sessionEndOnReadBufferFull(ClientBuilder clientBuilder, ServerBuilder serverBuilder,
                                                               IOSettings serverIOSettings, int messageSize) {
        setupTestEnvAndWaitForConnections(clientBuilder, serverBuilder.toBuilder().ioSettings(serverIOSettings).build());
        trapWholeMessages(serverIoEventsListener);
        AtomicReference<Throwable> error = trapError(serverIoEventsListener);

        clientIOsession.send(lengthPrefixed(payload(messageSize)));

        verify(serverIoEventsListener, timeout(5_000)).onDisconnected(serverClientIOsession);
        assertThat(error.get()).isInstanceOf(ReadBufferFullException.class)
                .hasMessageContaining(serverClientIOsession.getId());
        return (ReadBufferFullException) error.get();
    }

    @Test
    void theReadBufferGrowsUpToOneMebibyteByDefault() {
        assertThat(IOSettings.builder().build().getMaxReadBufferSize()).isEqualTo(1024 * 1024);
    }

    @Test
    void aMaximumBelowTheReadBufferSizeIsRejected() {
        IOSettings invalid = IOSettings.builder().readBufferSize(4096).maxReadBufferSize(4095).build();

        assertThatIllegalArgumentException()
                .isThrownBy(() -> getTestServerBuilder().toBuilder().ioSettings(invalid).build().newInstance())
                .withMessage("maxReadBufferSize 4095 is smaller than readBufferSize 4096");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> getTestClientBuilder().toBuilder().ioSettings(invalid).build().newInstance());
    }

    @Test
    void aMaximumEqualToTheReadBufferSizeIsAccepted() {
        IOSettings noGrowth = IOSettings.builder().readBufferSize(4096).maxReadBufferSize(4096).build();

        setupTestEnvAndWaitForConnections(getTestClientBuilder().toBuilder().ioSettings(noGrowth).build(),
                getTestServerBuilder().toBuilder().ioSettings(noGrowth).build());
    }

    /**
     * Consumes whole length-prefixed messages only, leaving an incomplete one in the buffer as a framing listener does.
     */
    private static Received trapWholeMessages(IOEventsListener listener) {
        Received received = new Received();
        doAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(1);
            received.bufferDirectness.add(buffer.isDirect());
            while (buffer.remaining() >= Integer.BYTES
                    && buffer.remaining() - Integer.BYTES >= buffer.getInt(buffer.position())) {
                byte[] message = new byte[buffer.getInt()];
                buffer.get(message);
                received.messages.add(message);
            }
            return null;
        }).when(listener).onRead(any(IOSession.class), any(ByteBuffer.class), anyLong());
        return received;
    }

    private static final class Received {
        final List<byte[]> messages = new CopyOnWriteArrayList<>();
        final Set<Boolean> bufferDirectness = ConcurrentHashMap.newKeySet();
    }

    private static AtomicReference<Throwable> trapError(IOEventsListener listener) {
        AtomicReference<Throwable> error = new AtomicReference<>();
        doAnswer(invocation -> {
            error.set(invocation.getArgument(1));
            return null;
        }).when(listener).onError(any(IOSession.class), any());
        return error;
    }

    private static byte[] payload(int size) {
        byte[] payload = new byte[size];
        for (int i = 0; i < size; i++) {
            payload[i] = (byte) (i % 251);
        }
        return payload;
    }

    private static byte[] lengthPrefixed(byte[] payload) {
        return ByteBuffer.allocate(Integer.BYTES + payload.length).putInt(payload.length).put(payload).array();
    }
}
