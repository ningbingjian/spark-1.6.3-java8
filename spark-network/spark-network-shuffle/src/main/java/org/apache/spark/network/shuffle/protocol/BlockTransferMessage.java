package org.apache.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.shuffle.protocol.mesos.RegisterDriver;

import java.nio.ByteBuffer;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/31
 * Time: 22:10
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public abstract class BlockTransferMessage implements Encodable {
    protected abstract Type type();
    /** Preceding every serialized message is its type, which allows us to deserialize it. */
    public static enum Type {
        OPEN_BLOCKS(0), UPLOAD_BLOCK(1), REGISTER_EXECUTOR(2), STREAM_HANDLE(3), REGISTER_DRIVER(4);

        private final byte id;

        private Type(int id) {
            assert id < 128 : "Cannot have more than 128 message types";
            this.id = (byte) id;
        }

        public byte id() { return id; }
    }
    /** Serializes the 'type' byte followed by the message itself. */
    public ByteBuffer toByteBuffer() {
        // Allow room for encoded message, plus the type byte
        ByteBuf buf = Unpooled.buffer(encodedLength() + 1);
        buf.writeByte(type().id);
        encode(buf);
        assert buf.writableBytes() == 0 : "Writable bytes remain: " + buf.writableBytes();
        return buf.nioBuffer();
    }
    // NB: Java does not support static methods in interfaces, so we must put this in a static class.
    public static class Decoder {
        /** Deserializes the 'type' byte followed by the message itself. */
        public static BlockTransferMessage fromByteBuffer(ByteBuffer msg) {
            ByteBuf buf = Unpooled.wrappedBuffer(msg);
            byte type = buf.readByte();
            switch (type) {
                case 0: return OpenBlocks.decode(buf);
                case 1: return UploadBlock.decode(buf);
                case 2: return RegisterExecutor.decode(buf);
                case 3: return StreamHandle.decode(buf);
                case 4: return RegisterDriver.decode(buf);
                default: throw new IllegalArgumentException("Unknown message type: " + type);
            }
        }
    }
}
