package org.apache.spark.network.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.NettyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ThreadFactory;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/13
 * Time: 20:30
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public class TransportServer implements Closeable {
    private final Logger logger = LoggerFactory.getLogger(TransportServer.class);
    private ServerBootstrap bootstrap;
    public void close() throws IOException {

    }
    public void testLog(){
        logger.info("testtsssssssssssssssssssss");
    }
    private void init(String hostToBind, int portToBind) {
        String mode = "NIO";
        IOMode ioMode = IOMode.valueOf(mode);
        int numThreads = 1;
        ThreadFactory threadFactory = NettyUtils.createThreadFactory("shuffle-server");
        EventLoopGroup bossGroup = null ;
        switch (ioMode) {
            case NIO:
                bossGroup =  new NioEventLoopGroup(numThreads, threadFactory);
                break;
            case EPOLL:
                bossGroup =  new EpollEventLoopGroup(numThreads, threadFactory);
                break;
            default:
                throw new IllegalArgumentException("Unknown io mode: " + mode);
        }
        EventLoopGroup workerGroup = bossGroup;
        PooledByteBufAllocator allocator = NettyUtils.createPooledByteBufAllocator(
                true, true /* allowCache */, 0);
        bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NettyUtils.getServerChannelClass(ioMode))
                .option(ChannelOption.ALLOCATOR, allocator)
                .childOption(ChannelOption.ALLOCATOR, allocator);

    }
}
