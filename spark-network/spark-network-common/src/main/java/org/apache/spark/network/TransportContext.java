package org.apache.spark.network;

import com.google.common.collect.Lists;
import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.MessageDecoder;
import org.apache.spark.network.protocol.MessageEncoder;
import org.apache.spark.network.server.*;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.TransportFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/17
 * Time: 21:08
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public class TransportContext {
    private final Logger logger = LoggerFactory.getLogger(TransportContext.class);

    private final TransportConf conf;
    private final RpcHandler rpcHandler;
    private final boolean closeIdleConnections;

    private final MessageEncoder encoder;
    private final MessageDecoder decoder;

    public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
        this(conf, rpcHandler, false);
    }

    public TransportContext(
            TransportConf conf,
            RpcHandler rpcHandler,
            boolean closeIdleConnections) {
        this.conf = conf;
        this.rpcHandler = rpcHandler;
        this.encoder = new MessageEncoder();
        this.decoder = new MessageDecoder();
        this.closeIdleConnections = closeIdleConnections;
    }
    public TransportConf getConf() { return conf; }
    /** Create a server which will attempt to bind to a specific port. */
    public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
        return new TransportServer(this, null, port, rpcHandler, bootstraps);
    }
    /** Create a server which will attempt to bind to a specific host and port. */
    public TransportServer createServer(
            String host, int port, List<TransportServerBootstrap> bootstraps) {
        return new TransportServer(this, host, port, rpcHandler, bootstraps);
    }

    /** Creates a new server, binding to any available ephemeral port. */
    public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
        return createServer(0, bootstraps);
    }
    public TransportServer createServer() {
        return createServer(0, Lists.<TransportServerBootstrap>newArrayList());
    }

    public TransportChannelHandler initializePipeline(SocketChannel channel) {
        return initializePipeline(channel, rpcHandler);
    }
    /**
     * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
     * has a {@link org.apache.spark.network.server.TransportChannelHandler} to handle request or
     * response messages.
     *
     * @param channel The channel to initialize.
     * @param channelRpcHandler The RPC handler to use for the channel.
     *
     * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
     * be used to communicate on this channel. The TransportClient is directly associated with a
     * ChannelHandler to ensure all users of the same channel get the same TransportClient object.
     */
    public TransportChannelHandler initializePipeline(
            SocketChannel channel,
            RpcHandler channelRpcHandler) {
        try {
            TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
            channel.pipeline()
                    .addLast("encoder", encoder)
                    .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
                    .addLast("decoder", decoder)
                    .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
                    // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
                    // would require more logic to guarantee if this were not part of the same event loop.
                    .addLast("handler", channelHandler);
            return channelHandler;
        } catch (RuntimeException e) {
            logger.error("Error while initializing Netty pipeline", e);
            throw e;
        }
    }
    /**
     * Creates the server- and client-side handler which is used to handle both RequestMessages and
     * ResponseMessages. The channel is expected to have been successfully created, though certain
     * properties (such as the remoteAddress()) may not be available yet.
     */
    private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
        TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
        TransportClient client = new TransportClient(channel, responseHandler);
        TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client,
                rpcHandler);
        return new TransportChannelHandler(client, responseHandler, requestHandler,
                conf.connectionTimeoutMs(), closeIdleConnections);
    }


}
