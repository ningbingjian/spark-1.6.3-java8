package org.apache.spark.network.shuffle.protocol;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/9/4
 * Time: 21:24
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;

/** Request to read a set of blocks. Returns {@link StreamHandle}. */
public class OpenBlocks extends BlockTransferMessage {
    public final String appId;
    public final String execId;
    public final String[] blockIds;
    public OpenBlocks(String appId, String execId, String[] blockIds) {
        this.appId = appId;
        this.execId = execId;
        this.blockIds = blockIds;
    }

    @Override
    protected Type type() { return Type.OPEN_BLOCKS; }
    @Override
    public int hashCode() {
        return Objects.hashCode(appId, execId) * 41
                + Arrays.hashCode(blockIds);
    }
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("appId", appId)
                .add("execId", execId)
                .add("blockIds", Arrays.toString(blockIds))
                .toString();
    }
    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof OpenBlocks) {
            OpenBlocks o = (OpenBlocks) other;
            return Objects.equal(appId, o.appId)
                    && Objects.equal(execId, o.execId)
                    && Arrays.equals(blockIds, o.blockIds);
        }
        return false;
    }
    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId)
                + Encoders.Strings.encodedLength(execId)
                + Encoders.StringArrays.encodedLength(blockIds);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        Encoders.Strings.encode(buf, execId);
        Encoders.StringArrays.encode(buf, blockIds);
    }
    public static OpenBlocks decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        String[] blockIds = Encoders.StringArrays.decode(buf);
        return new OpenBlocks(appId, execId, blockIds);
    }


}
