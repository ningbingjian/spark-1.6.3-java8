package org.apache.spark.network.server;

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
    public void close() throws IOException {

    }
    public void testLog(){
        logger.info("testtsssssssssssssssssssss");
    }
    private void init(String hostToBind, int portToBind) {
        IOMode mode = IOMode.valueOf("NIO");
        int numThreads = 1;
        ThreadFactory threadFactory = NettyUtils.createThreadFactory("shuffle-server");
        EventLoopGroup bossGroup = null ;
        switch (mode) {
            case NIO:
                bossGroup =  new NioEventLoopGroup(numThreads, threadFactory);
            case EPOLL:
                bossGroup =  new EpollEventLoopGroup(numThreads, threadFactory);
            default:
                throw new IllegalArgumentException("Unknown io mode: " + mode);
        }
        EventLoopGroup workerGroup = bossGroup;

    }
}
