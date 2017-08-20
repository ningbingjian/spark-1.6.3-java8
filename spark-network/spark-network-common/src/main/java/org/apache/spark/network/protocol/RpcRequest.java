package org.apache.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/17
 * Time: 22:01
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public class RpcRequest extends AbstractMessage implements RequestMessage {
    /** Used to link an RPC request with its response. */
    public final long requestId;
    public RpcRequest(long requestId, ManagedBuffer message) {
        super(message, true);
        this.requestId = requestId;
    }
    public Type type() { return Type.RpcRequest; }
    public int encodedLength() {
        // The integer (a.k.a. the body size) is not really used, since that information is already
        // encoded in the frame length. But this maintains backwards compatibility with versions of
        // RpcRequest that use Encoders.ByteArrays.

        // id --8个字节

        return 8 + 4;
    }
    public void encode(ByteBuf buf) {
        //写id
        buf.writeLong(requestId);
        // See comment in encodedLength().
        //主体和框架在一起 所以长度就是body的长度
        buf.writeInt((int) body().size());
    }
    public static RpcRequest decode(ByteBuf buf) {
        long requestId = buf.readLong();
        // See comment in encodedLength().
        buf.readInt();
        return new RpcRequest(requestId, new NettyManagedBuffer(buf.retain()));
    }

}
