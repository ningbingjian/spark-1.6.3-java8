package org.apache.spark.network.shuffle.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/9/4
 * Time: 20:52
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
/** Contains all configuration necessary for locating
 * the shuffle files of an executor. */
public class ExecutorShuffleInfo implements Encodable {

    /** The base set of local directories that the
     * executor stores its shuffle files in. */
    public final String[] localDirs;
    /** Number of subdirectories created within each localDir. */
    public final int subDirsPerLocalDir;
    /** Shuffle manager (SortShuffleManager or HashShuffleManager)
     * that the executor is using. */
    public final String shuffleManager;
    @JsonCreator
    public ExecutorShuffleInfo(
            @JsonProperty("localDirs") String[] localDirs,
            @JsonProperty("subDirsPerLocalDir") int subDirsPerLocalDir,
            @JsonProperty("shuffleManager") String shuffleManager) {
        this.localDirs = localDirs;
        this.subDirsPerLocalDir = subDirsPerLocalDir;
        this.shuffleManager = shuffleManager;
    }
    @Override
    public int hashCode() {
        return Objects.hashCode(subDirsPerLocalDir, shuffleManager) * 41 + Arrays.hashCode(localDirs);
    }
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("localDirs", Arrays.toString(localDirs))
                .add("subDirsPerLocalDir", subDirsPerLocalDir)
                .add("shuffleManager", shuffleManager)
                .toString();
    }
    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof ExecutorShuffleInfo) {
            ExecutorShuffleInfo o = (ExecutorShuffleInfo) other;
            return Arrays.equals(localDirs, o.localDirs)
                    && Objects.equal(subDirsPerLocalDir, o.subDirsPerLocalDir)
                    && Objects.equal(shuffleManager, o.shuffleManager);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return Encoders.StringArrays.encodedLength(localDirs)
                + 4 // int
                + Encoders.Strings.encodedLength(shuffleManager);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.StringArrays.encode(buf, localDirs);
        buf.writeInt(subDirsPerLocalDir);
        Encoders.Strings.encode(buf, shuffleManager);
    }

    public static ExecutorShuffleInfo decode(ByteBuf buf) {
        String[] localDirs = Encoders.StringArrays.decode(buf);
        int subDirsPerLocalDir = buf.readInt();
        String shuffleManager = Encoders.Strings.decode(buf);
        return new ExecutorShuffleInfo(localDirs, subDirsPerLocalDir, shuffleManager);
    }



}
