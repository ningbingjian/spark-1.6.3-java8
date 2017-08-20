package org.apache.spark.network.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/17
 * Time: 21:07
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public class TransportServer  implements Closeable {
    private final Logger logger = LoggerFactory.getLogger(TransportServer.class);

    private final TransportContext context;
    private final TransportConf conf;
    private final RpcHandler appRpcHandler;
    private final List<TransportServerBootstrap> bootstraps;

    private ServerBootstrap bootstrap;
    private ChannelFuture channelFuture;
    private int port = -1;
    /**
     * Creates a TransportServer that binds to the given host and the given port, or to any available
     * if 0. If you don't want to bind to any special host, set "hostToBind" to null.
     * */
    public TransportServer(
            TransportContext context,
            String hostToBind,
            int portToBind,
            RpcHandler appRpcHandler,
            List<TransportServerBootstrap> bootstraps) {
        this.context = context;
        this.conf = context.getConf();
        this.appRpcHandler = appRpcHandler;
        this.bootstraps = Lists.newArrayList(Preconditions.checkNotNull(bootstraps));

        try {
            init(hostToBind, portToBind);
        } catch (RuntimeException e) {
            JavaUtils.closeQuietly(this);
            throw e;
        }

    }
    public int getPort() {
        if (port == -1) {
            throw new IllegalStateException("Server not initialized");
        }
        return port;
    }
    private void init(String hostToBind, int portToBind) {
        IOMode ioMode = IOMode.valueOf(conf.ioMode());
        EventLoopGroup bossGroup =
                NettyUtils.createEventLoop(ioMode, 0, "shuffle-server");
        EventLoopGroup workerGroup = bossGroup;
        PooledByteBufAllocator allocator = NettyUtils.createPooledByteBufAllocator(
                conf.preferDirectBufs(), true /* allowCache */, conf.serverThreads());


        bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NettyUtils.getServerChannelClass(ioMode))
                .option(ChannelOption.ALLOCATOR, allocator)
                .childOption(ChannelOption.ALLOCATOR, allocator);
        if (conf.backLog() > 0) {
            bootstrap.option(ChannelOption.SO_BACKLOG, conf.backLog());
        }
        if (conf.receiveBuf() > 0) {
            bootstrap.childOption(ChannelOption.SO_RCVBUF, conf.receiveBuf());
        }

        if (conf.sendBuf() > 0) {
            bootstrap.childOption(ChannelOption.SO_SNDBUF, conf.sendBuf());
        }
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                RpcHandler rpcHandler = appRpcHandler;
                for (TransportServerBootstrap bootstrap : bootstraps) {
                    rpcHandler = bootstrap.doBootstrap(ch, rpcHandler);
                }
                context.initializePipeline(ch, rpcHandler);
            }
        });
        InetSocketAddress address = hostToBind == null ?
                new InetSocketAddress(portToBind): new InetSocketAddress(hostToBind, portToBind);
        channelFuture = bootstrap.bind(address);
        channelFuture.syncUninterruptibly();

        port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        logger.debug("Shuffle server started on port :" + port);


    }
    @Override
    public void close() {
        if (channelFuture != null) {
            // close is a local operation and should finish within milliseconds; timeout just to be safe
            channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS);
            channelFuture = null;
        }
        if (bootstrap != null && bootstrap.group() != null) {
            bootstrap.group().shutdownGracefully();
        }
        if (bootstrap != null && bootstrap.childGroup() != null) {
            bootstrap.childGroup().shutdownGracefully();
        }
        bootstrap = null;
    }

}
