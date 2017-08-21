package org.apache.spark.network.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.internal.PlatformDependent;

import java.lang.reflect.Field;
import java.util.concurrent.ThreadFactory;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/13
 * Time: 20:56
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public class NettyUtils {
    /** Creates a new ThreadFactory which prefixes each thread with the given name. */
    public static ThreadFactory createThreadFactory(String threadPoolPrefix) {
        return new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(threadPoolPrefix + "-%d")
                .build();
    }
    /** Creates a Netty EventLoopGroup based on the IOMode. */
    public static EventLoopGroup createEventLoop(IOMode mode, int numThreads, String threadPrefix) {
        ThreadFactory threadFactory = createThreadFactory(threadPrefix);

        switch (mode) {
            case NIO:
                return new NioEventLoopGroup(numThreads, threadFactory);
            case EPOLL:
                return new EpollEventLoopGroup(numThreads, threadFactory);
            default:
                throw new IllegalArgumentException("Unknown io mode: " + mode);
        }
    }

    /** Returns the correct (client) SocketChannel class based on IOMode. */
    public static Class<? extends Channel> getClientChannelClass(IOMode mode) {
        switch (mode) {
            case NIO:
                return NioSocketChannel.class;
            case EPOLL:
                return EpollSocketChannel.class;
            default:
                throw new IllegalArgumentException("Unknown io mode: " + mode);
        }
    }


    /**
     * Create a pooled ByteBuf allocator but disables the thread-local cache. Thread-local caches
     * are disabled for TransportClients because the ByteBufs are allocated by the event loop thread,
     * but released by the executor thread rather than the event loop thread. Those thread-local
     * caches actually delay the recycling of buffers, leading to larger memory usage.
     *
     *
     */
    public static PooledByteBufAllocator createPooledByteBufAllocator(
            boolean allowDirectBufs,
            boolean allowCache,
            int numCores) {
        if (numCores == 0) {
            numCores = Runtime.getRuntime().availableProcessors();
        }
        return new PooledByteBufAllocator(
                allowDirectBufs && PlatformDependent.directBufferPreferred(),//堆内还是堆外
                Math.min(getPrivateStaticField("DEFAULT_NUM_HEAP_ARENA"), numCores),//使用核心数
                Math.min(getPrivateStaticField("DEFAULT_NUM_DIRECT_ARENA"), allowDirectBufs ? numCores : 0),
                getPrivateStaticField("DEFAULT_PAGE_SIZE"),//PAGE SIZE
                getPrivateStaticField("DEFAULT_MAX_ORDER"),//MAX ORDER
                allowCache ? getPrivateStaticField("DEFAULT_TINY_CACHE_SIZE") : 0,
                allowCache ? getPrivateStaticField("DEFAULT_SMALL_CACHE_SIZE") : 0,
                allowCache ? getPrivateStaticField("DEFAULT_NORMAL_CACHE_SIZE") : 0
        );
    }
    /**
     * Creates a LengthFieldBasedFrameDecoder where the first 8 bytes are the length of the frame.
     * This is used before all decoders.
     */
    public static TransportFrameDecoder createFrameDecoder() {
        return new TransportFrameDecoder();
    }
    /**
     * Used to get defaults from Netty's private static fields.
     * 从Netty的PooledByteBufAllocator私有静态变量中获取默认值
     * */
    private static int getPrivateStaticField(String name) {
        try {
            Field f = PooledByteBufAllocator.DEFAULT.getClass().getDeclaredField(name);
            f.setAccessible(true);
            return f.getInt(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    /** Returns the correct ServerSocketChannel class based on IOMode. */
    public static Class<? extends ServerChannel> getServerChannelClass(IOMode mode) {
        switch (mode) {
            case NIO:
                return NioServerSocketChannel.class;
            case EPOLL:
                return EpollServerSocketChannel.class;
            default:
                throw new IllegalArgumentException("Unknown io mode: " + mode);
        }
    }
    /** Returns the remote address on the channel or "&lt;unknown remote&gt;" if none exists. */
    public static String getRemoteAddress(Channel channel) {
        if (channel != null && channel.remoteAddress() != null) {
            return channel.remoteAddress().toString();
        }
        return "<unknown remote>";
    }
}
