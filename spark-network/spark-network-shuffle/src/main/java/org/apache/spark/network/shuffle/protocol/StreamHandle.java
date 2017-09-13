package org.apache.spark.network.shuffle.protocol;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/9/4
 * Time: 21:40
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * Identifier for a fixed number of chunks to read from a
 * stream created by an "open blocks"
 * message. This is used by
 * {@link org.apache.spark.network.shuffle.OneForOneBlockFetcher}.
 */
public class StreamHandle extends BlockTransferMessage  {
    public final long streamId;
    public final int numChunks;

    public StreamHandle(long streamId, int numChunks) {
        this.streamId = streamId;
        this.numChunks = numChunks;
    }
    @Override
    protected Type type() { return Type.STREAM_HANDLE; }

    @Override
    public int hashCode() {
        return Objects.hashCode(streamId, numChunks);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("streamId", streamId)
                .add("numChunks", numChunks)
                .toString();
    }
    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof StreamHandle) {
            StreamHandle o = (StreamHandle) other;
            return Objects.equal(streamId, o.streamId)
                    && Objects.equal(numChunks, o.numChunks);
        }
        return false;
    }
    @Override
    public int encodedLength() {
        return 8 + 4;
    }
    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(streamId);
        buf.writeInt(numChunks);
    }

    public static StreamHandle decode(ByteBuf buf) {
        long streamId = buf.readLong();
        int numChunks = buf.readInt();
        return new StreamHandle(streamId, numChunks);
    }

}
