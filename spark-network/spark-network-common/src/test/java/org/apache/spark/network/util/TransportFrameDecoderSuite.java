package org.apache.spark.network.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/20
 * Time: 21:47
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public class TransportFrameDecoderSuite {
    private static Random RND = new Random();
    @AfterClass
    public static void cleanup() {
        RND = null;
    }
    @Test
    public void testFrameDecoding() throws Exception {
        TransportFrameDecoder decoder = new TransportFrameDecoder();
        ChannelHandlerContext ctx = mockChannelHandlerContext();
        ByteBuf data = createAndFeedFrames(100, decoder, ctx);
        verifyAndCloseDecoder(decoder, ctx, data);
    }
    @Test
    public void testInterception() throws Exception {
        final int interceptedReads = 3;
        TransportFrameDecoder decoder = new TransportFrameDecoder();
        TransportFrameDecoder.Interceptor interceptor = spy(new MockInterceptor(interceptedReads));
        ChannelHandlerContext ctx = mockChannelHandlerContext();
        byte[] data = new byte[8];
        ByteBuf len = Unpooled.copyLong(8 + data.length);
        ByteBuf dataBuf = Unpooled.wrappedBuffer(data);
        try {
            decoder.setInterceptor(interceptor);
            for (int i = 0; i < interceptedReads; i++) {
                decoder.channelRead(ctx, dataBuf);
                assertEquals(0, dataBuf.refCnt());
                dataBuf = Unpooled.wrappedBuffer(data);
            }
            decoder.channelRead(ctx, len);
            decoder.channelRead(ctx, dataBuf);
            verify(interceptor, times(interceptedReads)).handle(any(ByteBuf.class));
            verify(ctx).fireChannelRead(any(ByteBuffer.class));
            assertEquals(0, len.refCnt());
            assertEquals(0, dataBuf.refCnt());
        }finally {
            release(len);
            release(dataBuf);
        }
    }

    private void verifyAndCloseDecoder(
            TransportFrameDecoder decoder,
            ChannelHandlerContext ctx,
            ByteBuf data) throws Exception {
        try {
            decoder.channelInactive(ctx);
            assertTrue("There shouldn't be dangling references to the data.", data.release());
        } finally {
            release(data);
        }
    }
    private ChannelHandlerContext mockChannelHandlerContext() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.fireChannelRead(any())).thenAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock in) {
                ByteBuf buf = (ByteBuf) in.getArguments()[0];
                buf.release();
                return null;
            }
        });
        return ctx;
    }
    /**
     * Creates a number of randomly sized frames and feed them to the given decoder, verifying
     * that the frames were read.
     */
    private ByteBuf createAndFeedFrames(
            int frameCount,
            TransportFrameDecoder decoder,
            ChannelHandlerContext ctx) throws Exception {
        ByteBuf data = Unpooled.buffer();
        for (int i = 0; i < frameCount; i++) {
            byte[] frame = new byte[1024 * (RND.nextInt(31) + 1)];
            data.writeLong(frame.length + 8);
            data.writeBytes(frame);
        }
        try {
            while (data.isReadable()) {
                int size = RND.nextInt(4 * 1024) + 256;
                decoder.channelRead(ctx, data.readSlice(Math.min(data.readableBytes(), size)).retain());
            }

            verify(ctx, times(frameCount)).fireChannelRead(any(ByteBuf.class));
        } catch (Exception e) {
            release(data);
            throw e;
        }
        return data;
    }
    private void release(ByteBuf buf) {
        if (buf.refCnt() > 0) {
            buf.release(buf.refCnt());
        }
    }

    private static class MockInterceptor implements TransportFrameDecoder.Interceptor {

        private int remainingReads;

        MockInterceptor(int readCount) {
            this.remainingReads = readCount;
        }

        @Override
        public boolean handle(ByteBuf data) throws Exception {
            data.readerIndex(data.readerIndex() + data.readableBytes());
            assertFalse(data.isReadable());
            remainingReads -= 1;
            return remainingReads != 0;
        }

        @Override
        public void exceptionCaught(Throwable cause) throws Exception {

        }

        @Override
        public void channelInactive() throws Exception {

        }

    }

}
