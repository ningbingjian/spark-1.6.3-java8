package org.apache.spark.network.protocol;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/19
 * Time: 12:31
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;

/**
 * A RPC that does not expect a reply, which is handled by a remote
 * {@link org.apache.spark.network.server.RpcHandler}.
 */
public class OneWayMessage extends AbstractMessage implements RequestMessage {
    public OneWayMessage(ManagedBuffer body) {
        super(body, true);
    }

    @Override
    public Type type() { return Type.OneWayMessage; }
    @Override
    public int encodedLength() {
        // The integer (a.k.a. the body size) is not really used, since that information is already
        // encoded in the frame length. But this maintains backwards compatibility with versions of
        // RpcRequest that use Encoders.ByteArrays.
        return 4;
    }
    @Override
    public void encode(ByteBuf buf) {
        // See comment in encodedLength().
        buf.writeInt((int) body().size());
    }
    public static OneWayMessage decode(ByteBuf buf) {
        // See comment in encodedLength().
        buf.readInt();
        return new OneWayMessage(new NettyManagedBuffer(buf.retain()));
    }
    @Override
    public int hashCode() {
        return Objects.hashCode(body());
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof OneWayMessage) {
            OneWayMessage o = (OneWayMessage) other;
            return super.equals(o);
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("body", body())
                .toString();
    }

}
