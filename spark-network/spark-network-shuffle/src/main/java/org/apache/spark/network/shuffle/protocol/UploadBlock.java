package org.apache.spark.network.shuffle.protocol;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/9/4
 * Time: 21:33
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;

/** Request to upload a block with a certain StorageLevel.
 * Returns nothing (empty byte array). */
public class UploadBlock extends BlockTransferMessage  {
    public final String appId;
    public final String execId;
    public final String blockId;
    // TODO: StorageLevel is serialized separately
    // in here because StorageLevel is not available in
    // this package. We should avoid this hack.
    public final byte[] metadata;
    public final byte[] blockData;
    /**
     * @param metadata Meta-information about block, typically StorageLevel.
     * @param blockData The actual block's bytes.
     */
    public UploadBlock(
            String appId,
            String execId,
            String blockId,
            byte[] metadata,
            byte[] blockData) {
        this.appId = appId;
        this.execId = execId;
        this.blockId = blockId;
        this.metadata = metadata;
        this.blockData = blockData;
    }

    @Override
    protected Type type() { return Type.UPLOAD_BLOCK; }
    @Override
    public int hashCode() {
        int objectsHashCode = Objects.hashCode(appId, execId, blockId);
        return (objectsHashCode * 41 + Arrays.hashCode(metadata)) * 41 + Arrays.hashCode(blockData);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("appId", appId)
                .add("execId", execId)
                .add("blockId", blockId)
                .add("metadata size", metadata.length)
                .add("block size", blockData.length)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof UploadBlock) {
            UploadBlock o = (UploadBlock) other;
            return Objects.equal(appId, o.appId)
                    && Objects.equal(execId, o.execId)
                    && Objects.equal(blockId, o.blockId)
                    && Arrays.equals(metadata, o.metadata)
                    && Arrays.equals(blockData, o.blockData);
        }
        return false;
    }
    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId)
                + Encoders.Strings.encodedLength(execId)
                + Encoders.Strings.encodedLength(blockId)
                + Encoders.ByteArrays.encodedLength(metadata)
                + Encoders.ByteArrays.encodedLength(blockData);
    }
    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        Encoders.Strings.encode(buf, execId);
        Encoders.Strings.encode(buf, blockId);
        Encoders.ByteArrays.encode(buf, metadata);
        Encoders.ByteArrays.encode(buf, blockData);
    }
    public static UploadBlock decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        String blockId = Encoders.Strings.decode(buf);
        byte[] metadata = Encoders.ByteArrays.decode(buf);
        byte[] blockData = Encoders.ByteArrays.decode(buf);
        return new UploadBlock(appId, execId, blockId, metadata, blockData);
    }

}
