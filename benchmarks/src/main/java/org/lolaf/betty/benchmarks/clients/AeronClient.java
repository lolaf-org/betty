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

import io.aeron.*;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.lolaf.betty.benchmarks.impl.AbstractClientBenchmark;
import org.lolaf.betty.benchmarks.impl.ClientSelectorSettings;
import org.lolaf.betty.benchmarks.impl.ClientSettings;
import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import static io.aeron.CommonContext.generateRandomDirName;

@Slf4j
public class AeronClient extends AbstractClientBenchmark {

    private static final String REQUEST_ENDPOINT = "localhost:9000";
    private static final int REQUEST_STREAM_ID = 9000;
    private static final String RESPONSE_CONTROL = "localhost:9001";
    private static final int RESPONSE_STREAM_ID = 9001;

    MediaDriver serverDriver;
    MediaDriver clientDriver;
    Aeron aeronClient;
    Aeron aeronServer;
    Client client;
    Server server;
    AgentRunner serverRunner;
    AgentRunner clientRunner;
    Server.ClientSession clientSession;
    UnsafeBuffer serverBuffer = new UnsafeBuffer(new byte[16]);
    UnsafeBuffer serverBigBuffer = new UnsafeBuffer(new byte[16 * 1024]);
    UnsafeBuffer clientBuffer = new UnsafeBuffer(new byte[16]);
    long sequence;
    AtomicLong clientAckReceived = new AtomicLong();
    MessagePassingQueue.Supplier<IdleStrategy> idleStrategyProvider;

    @Override
    public boolean enableBenchmarkThreadCoreAffinity() {
        // if benchmark thread has affinity set, the throughput gets absolutely destroyed, no clue why..
        return false;
    }

    @Override
    public void setup() {
        if (getClientSelectorSettings().equals(ClientSelectorSettings.JDK_EPOLL) && getClientSettings().equals(ClientSettings.STOCK)) {
            throw new IllegalStateException("Not supported");
        }
        if (getClientSelectorSettings().equals(ClientSelectorSettings.JDK_EPOLL) && getClientSettings().equals(ClientSettings.LOW_LATENCY)) {
            throw new IllegalStateException("Not supported");
        }
        if (getClientSettings().equals(ClientSettings.STOCK)) {
            idleStrategyProvider = BackoffIdleStrategy::new;
        }
        if (getClientSettings().equals(ClientSettings.LOW_LATENCY)) {
            idleStrategyProvider = BusySpinIdleStrategy::new;
        }
        AtomicInteger ioCoreAffinityStart = new AtomicInteger(IO_THREAD_CORE_AFFINITY);
        final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(generateRandomDirName())
                .publicationTermBufferLength(512 * 1024 * 1024)
                .senderIdleStrategy(idleStrategyProvider.get())
                .receiverIdleStrategy(idleStrategyProvider.get())
                .threadingMode(ThreadingMode.DEDICATED)
                .receiverThreadFactory(r -> new Thread(() -> {
                    int nextCore = ioCoreAffinityStart.getAndDecrement();
                    log.info("Setting receiver IO core affinity to core " + nextCore);
                    Affinity.setAffinity(nextCore);
                    r.run();
                }))
                .senderThreadFactory(r -> new Thread(() -> {
                    int nextCore = ioCoreAffinityStart.getAndDecrement();
                    log.info("Setting sender IO core affinity to core " + nextCore);
                    Affinity.setAffinity(nextCore);
                    r.run();
                }))
                .enableExperimentalFeatures(true);
        serverDriver = MediaDriver.launch(
                context.clone().aeronDirectoryName(context.aeronDirectoryName() + "-server"));
        clientDriver = MediaDriver.launch(
                context.clone().aeronDirectoryName(context.aeronDirectoryName() + "-client"));
        super.setup();
    }

    @Override
    public void setupServer() {
        aeronServer = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(serverDriver.aeronDirectoryName())
                .errorHandler(t -> log.warn("Aeron server error after shutdown: {}", t.getMessage())));
        Server.ResponseHandler responseHandler = (buffer, offset, length, header, responsePublication) -> {
            long systemTime = buffer.getLong(offset);
            long sequence = buffer.getLong(offset + 8);
            clientAckReceived.set(sequence);
            return true;
        };

        server = new Server(aeronServer, image -> responseHandler, REQUEST_ENDPOINT, REQUEST_STREAM_ID,
                RESPONSE_CONTROL, RESPONSE_STREAM_ID, null, null);
        serverRunner = new AgentRunner(idleStrategyProvider.get(), new RethrowingErrorHandler(), null, server);
        AgentRunner.startOnThread(serverRunner);
    }

    @Override
    public void setupClient(ClientSettings clientSettings) {
        aeronClient = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(clientDriver.aeronDirectoryName())
                .errorHandler(t -> log.warn("Aeron client error after shutdown: {}", t.getMessage())));

        final ControlledFragmentHandler recvHandler = new ControlledFragmentAssembler((buffer, offset, length, header) -> {
            for (int i = offset; i < offset + length; i += 16) {
                clientBuffer.putLong(0, buffer.getLong(i));
                clientBuffer.putLong(8, buffer.getLong(i + 8));
            }
            // commit only the last receive message in the buffer
            client.offer(clientBuffer);
            return ControlledFragmentHandler.Action.COMMIT;
        });
        client = new Client(aeronClient, recvHandler, REQUEST_ENDPOINT, REQUEST_STREAM_ID, RESPONSE_CONTROL, RESPONSE_STREAM_ID);

        clientRunner = new AgentRunner(idleStrategyProvider.get(), new RethrowingErrorHandler(), null, client);
        AgentRunner.startOnThread(clientRunner);
    }

    @Override
    public void waitForClientConnection() {
        while (server.sessionCount() <= 0 && !client.isConnected()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        clientSession = server.clientToPublicationMap.values().iterator().next();
    }

    @Override
    public void serverSendMessageAndWaitsForAck() {
        clientAckReceived.set(-1);
        serverBuffer.putLong(0, System.nanoTime());
        serverBuffer.putLong(8, ++sequence);
        long sent;
        do {
            sent = clientSession.offer(serverBuffer);
        }
        while (sent < 0);
        while (clientAckReceived.get() != sequence) {
            Thread.onSpinWait();
        }
    }

    @Override
    public void serverSendMessagesAndWaitsForAck(int messagesCount) {
        clientAckReceived.set(-1);
        for (int i = 0; i < messagesCount * 16; i += 16) {
            serverBigBuffer.putLong(i, System.nanoTime());
            serverBigBuffer.putLong(i + 8, ++sequence);
        }
        long sent;
        do {
            sent = clientSession.offer(serverBigBuffer);
        }
        while (sent < 0);
        while (clientAckReceived.get() != sequence) {
            Thread.onSpinWait();
        }
    }

    @Override
    public boolean isSupported(ClientSettings settings) {
        return true;
    }

    @Override
    public void shutdownServer() throws IOException {
        serverRunner.close();
        server.close();
        // the Aeron client owns a conductor thread of its own: close it before the driver it talks to, or the
        // conductor sees the driver vanish and the default error handler kills the JMH fork
        CloseHelper.quietClose(aeronServer);
        serverDriver.close();
        deleteDir(new File(serverDriver.aeronDirectoryName()));
    }


    @Override
    public void shutdownClient() throws IOException {
        clientRunner.close();
        client.close();
        CloseHelper.quietClose(aeronClient);
        clientDriver.close();
        deleteDir(new File(clientDriver.aeronDirectoryName()));
    }

    void deleteDir(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                deleteDir(c);
        }
        if (!f.delete()) {
            throw new FileNotFoundException("Failed to delete file: " + f);
        }
    }

    private static class Client implements AutoCloseable, Agent {
        private final Aeron aeron;
        private final ControlledFragmentHandler handler;
        private final int requestStreamId;
        private final int responseStreamId;
        private final ChannelUriStringBuilder requestUriBuilder;
        private final ChannelUriStringBuilder responseUriBuilder;
        private Publication publication;
        private Subscription subscription;

        public Client(
                final Aeron aeron,
                final ControlledFragmentHandler handler,
                final String requestEndpoint,
                final int requestStreamId,
                final String responseControl,
                final int responseStreamId,
                final String requestChannel,
                final String responseChannel) {
            this.aeron = aeron;
            this.handler = handler;
            this.requestStreamId = requestStreamId;
            this.responseStreamId = responseStreamId;

            requestUriBuilder = null != requestChannel ?
                    new ChannelUriStringBuilder(requestChannel) : new ChannelUriStringBuilder();
            requestUriBuilder
                    .media("udp")
                    .endpoint(requestEndpoint);
            responseUriBuilder = null != responseChannel ?
                    new ChannelUriStringBuilder(responseChannel) : new ChannelUriStringBuilder();
            responseUriBuilder
                    .media("udp")
                    .controlMode("response")
                    .controlEndpoint(responseControl);
        }

        public Client(
                final Aeron aeron,
                final ControlledFragmentHandler handler,
                final String requestEndpoint,
                final int requestStreamId,
                final String responseControl,
                final int responseStreamId) {
            this(aeron, handler, requestEndpoint, requestStreamId, responseControl, responseStreamId, null, null);
        }

        @Override
        public int doWork() {
            if (null == subscription) {
                subscription = aeron.addSubscription(responseUriBuilder.build(), responseStreamId);
            }
            if (null == publication) {
                publication = aeron.addPublication(
                        requestUriBuilder.responseCorrelationId(subscription.registrationId()).build(),
                        requestStreamId);
            }
            return subscription.controlledPoll(handler, 10);
        }

        @Override
        public String roleName() {
            return "Client";
        }

        @Override
        public void close() {
            subscription.close();
            publication.close();
        }

        public boolean isConnected() {
            return null != subscription && subscription.isConnected() && null != publication && publication.isConnected();
        }

        public long offer(final DirectBuffer message) {
            return publication.offer(message);
        }
    }

    private static class Server implements AutoCloseable, Agent {
        private final Aeron aeron;
        private final Long2ObjectHashMap<ClientSession> clientToPublicationMap = new Long2ObjectHashMap<>();
        private final OneToOneConcurrentArrayQueue<Image> availableImages = new OneToOneConcurrentArrayQueue<>(8);
        private final OneToOneConcurrentArrayQueue<Image> unavailableImages = new OneToOneConcurrentArrayQueue<>(8);
        private final Function<Image, ResponseHandler> handlerFactory;
        private final int requestStreamId;
        private final int responseStreamId;
        private final ChannelUriStringBuilder requestUriBuilder;
        private final ChannelUriStringBuilder responseUriBuilder;
        private final ControlledFragmentAssembler requestAssembler = new ControlledFragmentAssembler(this::onControlledRequestMessage);
        private Subscription serverSubscription;

        public Server(
                final Aeron aeron,
                final Function<Image, ResponseHandler> handlerFactory,
                final String requestEndpoint,
                final int requestStreamId,
                final String responseControl,
                final int responseStreamId,
                final String requestChannel,
                final String responseChannel) {
            this.aeron = aeron;
            this.handlerFactory = handlerFactory;
            this.requestStreamId = requestStreamId;
            this.responseStreamId = responseStreamId;

            requestUriBuilder = null == requestChannel ?
                    new ChannelUriStringBuilder() : new ChannelUriStringBuilder(requestChannel);
            requestUriBuilder
                    .media("udp")
                    .endpoint(requestEndpoint)
                    .responseEndpoint(responseControl);
            responseUriBuilder = null == responseChannel ?
                    new ChannelUriStringBuilder() : new ChannelUriStringBuilder(responseChannel);
            responseUriBuilder
                    .media("udp")
                    .controlMode("response")
                    .controlEndpoint(responseControl);
        }

        @Override
        public int doWork() {
            if (null == serverSubscription) {
                serverSubscription = aeron.addSubscription(
                        requestUriBuilder.build(),
                        requestStreamId,
                        this::enqueueAvailableImage,
                        this::enqueueUnavailableImage);
            }

            Image image;
            while (null != (image = availableImages.poll())) {
                getOrCreateSession(image);
            }

            while (null != (image = unavailableImages.poll())) {
                removeSession(image);
            }
            return serverSubscription.controlledPoll(requestAssembler, 10);
        }


        public int sessionCount() {
            return clientToPublicationMap.size();
        }

        @Override
        public void close() {
            CloseHelper.quietClose(serverSubscription);
            clientToPublicationMap.values().forEach(CloseHelper::quietClose);
        }

        @Override
        public String roleName() {
            return "Server";
        }

        private void enqueueAvailableImage(final Image image) {
            if (!availableImages.offer(image)) {
                throw new IllegalStateException("Unable to enqueue new image");
            }
        }

        private void enqueueUnavailableImage(final Image image) {
            if (!unavailableImages.offer(image)) {
                throw new IllegalStateException("Unable to enqueue removed image");
            }
        }

        private ControlledFragmentHandler.Action onControlledRequestMessage(
                final DirectBuffer buffer, final int offset, final int length, final Header header) {
            final ClientSession session = getOrCreateSession((Image) header.context());
            final boolean processed = session.process(buffer, offset, length, header);

            return processed ? ControlledFragmentHandler.Action.CONTINUE : ControlledFragmentHandler.Action.ABORT;
        }

        private ClientSession getOrCreateSession(final Image image) {
            ClientSession session = clientToPublicationMap.get(image.correlationId());
            if (null == session) {
                Publication responsePublication = aeron.addPublication(
                        responseUriBuilder.responseCorrelationId(image.correlationId()).build(),
                        responseStreamId);
                ResponseHandler handler = handlerFactory.apply(image);
                session = new ClientSession(responsePublication, handler);
                clientToPublicationMap.put(image.correlationId(), session);
            }

            return session;
        }

        private void removeSession(final Image image) {
            requestAssembler.freeSessionBuffer(image.sessionId());
            final ClientSession session = clientToPublicationMap.remove(image.correlationId());
            CloseHelper.quietClose(session);
        }


        public interface ResponseHandler {

            boolean onMessage(DirectBuffer buffer, int offset, int length, Header header, Publication responsePublication);
        }

        private static final class ClientSession implements AutoCloseable {
            private final Publication publication;
            private final ResponseHandler handler;

            ClientSession(final Publication publication, final ResponseHandler handler) {
                this.publication = publication;
                this.handler = handler;
            }

            public boolean process(final DirectBuffer buffer, final int offset, final int length, final Header header) {
                return handler.onMessage(buffer, offset, length, header, publication);
            }

            public long offer(DirectBuffer directBuffer) {
                return publication.offer(directBuffer);
            }

            public long offer(DirectBuffer directBuffer, int offset, int length) {
                return publication.offer(directBuffer, offset, length);
            }


            /**
             * {@inheritDoc}
             */
            public void close() {
                CloseHelper.close(publication);
            }
        }
    }
}
