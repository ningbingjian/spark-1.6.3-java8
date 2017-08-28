package org.apache.spark.network;

import com.google.common.base.Preconditions;
import io.netty.buffer.Unpooled;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/24
 * Time: 20:52
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public class TestManagedBuffer  extends ManagedBuffer {
    private final int len;
    private NettyManagedBuffer underlying;
    public TestManagedBuffer(int len) {
        Preconditions.checkArgument(len <= Byte.MAX_VALUE);
        this.len = len;
        byte[] byteArray = new byte[len];
        for (int i = 0; i < len; i ++) {
            byteArray[i] = (byte) i;
        }
        this.underlying = new NettyManagedBuffer(Unpooled.wrappedBuffer(byteArray));
    }
    @Override
    public long size() {
        return underlying.size();
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        return underlying.nioByteBuffer();
    }
    @Override
    public InputStream createInputStream() throws IOException {
        return underlying.createInputStream();
    }
    @Override
    public ManagedBuffer retain() {
        underlying.retain();
        return this;
    }
    @Override
    public ManagedBuffer release() {
        underlying.release();
        return this;
    }

    @Override
    public Object convertToNetty() throws IOException {
        return underlying.convertToNetty();
    }
    @Override
    public int hashCode() {
        return underlying.hashCode();
    }
    @Override
    public boolean equals(Object other) {
        if (other instanceof ManagedBuffer) {
            try {
                ByteBuffer nioBuf = ((ManagedBuffer) other).nioByteBuffer();
                if (nioBuf.remaining() != len) {
                    return false;
                } else {
                    for (int i = 0; i < len; i ++) {
                        if (nioBuf.get() != i) {
                            return false;
                        }
                    }
                    return true;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }
}
