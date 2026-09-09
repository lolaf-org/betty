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
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@Slf4j
@State(Scope.Benchmark)
public class NettyClient extends AbstractClientBenchmark {
    private ChannelFuture channelFuture;
    private EventLoopGroup eventLoopGroup;

    @Override
    public boolean isSupported(ClientSettings settings) {
        // netty officially state that busy spin selector is not supported in their NIO impl
        return ClientSettings.STOCK.equals(settings);
    }

    @Override
    public void setupClient(ClientSettings clientSettings) {
        eventLoopGroup = new MultiThreadIoEventLoopGroup(1, r -> {
            return new Thread(() -> {
                log.info("Setting IO core affinity to core " + IO_THREAD_CORE_AFFINITY);
                Affinity.setAffinity(AbstractClientBenchmark.IO_THREAD_CORE_AFFINITY);
                r.run();
            });
        }, NioIoHandler.newFactory());
        Bootstrap bootstrap = new Bootstrap().group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                // fixed receive read buffer to be fair with others, default is using a received byte allocator that can increase its buffer size
                .option(ChannelOption.RECVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(READ_BUFFER_SIZE))
                .handler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(io.netty.channel.Channel ch) {
                        ch.pipeline().addLast(
                                new SimpleChannelInboundHandler<ByteBuf>() {

                                    private ByteBufAllocator allocator;

                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                        Channel channel = ctx.channel();
                                        ByteBuf out = allocator.directBuffer(msg.readableBytes());
                                        while (msg.readableBytes() >= BenchmarkServer.PACKET_SIZE) {
                                            out.writeLong(msg.readLong()).writeLong(msg.readLong());
                                        }
                                        channel.writeAndFlush(out);
                                    }

                                    @Override
                                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                        allocator = ctx.channel().alloc();
                                        super.channelActive(ctx);
                                    }

                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                        log.info("Exception caught processing message", cause);
                                        ctx.channel().close();
                                    }
                                });
                    }
                });
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        channelFuture = bootstrap.connect(BenchmarkServer.CONNECT_ADDRESS);
    }

    @Override
    public void shutdownClient() {
        channelFuture.channel().close();
        eventLoopGroup.shutdownGracefully();
    }
}