package org.apache.spark.network;

import com.google.common.primitives.Ints;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.spark.network.protocol.*;
import org.apache.spark.network.util.ByteArrayWritableChannel;
import org.apache.spark.network.util.NettyUtils;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/22
 * Time: 21:53
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public class ProtocolSuite {
    private void testServerToClient(Message msg) {
        EmbeddedChannel serverChannel = new EmbeddedChannel(new FileRegionEncoder(),
                new MessageEncoder());
        serverChannel.writeOutbound(msg);
        EmbeddedChannel clientChannel = new EmbeddedChannel(
                NettyUtils.createFrameDecoder(), new MessageDecoder());

        while (!serverChannel.outboundMessages().isEmpty()) {
            clientChannel.writeInbound(serverChannel.readOutbound());
        }
        assertEquals(1, clientChannel.inboundMessages().size());
        assertEquals(msg, clientChannel.readInbound());
    }

    private void testClientToServer(Message msg) {
        EmbeddedChannel clientChannel = new EmbeddedChannel(new FileRegionEncoder(),
                new MessageEncoder());
        clientChannel.writeOutbound(msg);
        EmbeddedChannel serverChannel = new EmbeddedChannel(
                NettyUtils.createFrameDecoder(), new MessageDecoder());
        while (!clientChannel.outboundMessages().isEmpty()) {
            serverChannel.writeInbound(clientChannel.readOutbound());
        }
        assertEquals(1, serverChannel.inboundMessages().size());
        assertEquals(msg, serverChannel.readInbound());
    }
    @Test
    public void requests() {
        testClientToServer(new ChunkFetchRequest(new StreamChunkId(1, 2)));
        testClientToServer(new RpcRequest(12345, new TestManagedBuffer(0)));
        testClientToServer(new StreamRequest("abcde"));
        testClientToServer(new OneWayMessage(new TestManagedBuffer(10)));
    }
    @Test
    public void responses() {
        testServerToClient(new ChunkFetchSuccess(new StreamChunkId(1, 2), new TestManagedBuffer(10)));
        testServerToClient(new ChunkFetchSuccess(new StreamChunkId(1, 2), new TestManagedBuffer(0)));
        testServerToClient(new ChunkFetchFailure(new StreamChunkId(1, 2), "this is an error"));
        testServerToClient(new ChunkFetchFailure(new StreamChunkId(1, 2), ""));
        testServerToClient(new RpcResponse(12345, new TestManagedBuffer(0)));
        testServerToClient(new RpcResponse(12345, new TestManagedBuffer(100)));
        testServerToClient(new RpcFailure(0, "this is an error"));
        testServerToClient(new RpcFailure(0, ""));
        // Note: buffer size must be "0" since StreamResponse's buffer is written differently to the
        // channel and cannot be tested like this.
        testServerToClient(new StreamResponse("anId", 12345L, new TestManagedBuffer(0)));
        testServerToClient(new StreamFailure("anId", "this is an error"));
    }

    /**
     * Handler to transform a FileRegion into a byte buffer. EmbeddedChannel doesn't actually transfer
     * bytes, but messages, so this is needed so that the frame decoder on the receiving side can
     * understand what MessageWithHeader actually contains.
     */
    private static class FileRegionEncoder extends MessageToMessageEncoder<FileRegion> {

        @Override
        public void encode(ChannelHandlerContext ctx, FileRegion in, List<Object> out)
                throws Exception {

            ByteArrayWritableChannel channel = new ByteArrayWritableChannel(Ints.checkedCast(in.count()));
            while (in.transfered() < in.count()) {
                in.transferTo(channel, in.transfered());
            }
            out.add(Unpooled.wrappedBuffer(channel.getData()));
        }

    }
}
