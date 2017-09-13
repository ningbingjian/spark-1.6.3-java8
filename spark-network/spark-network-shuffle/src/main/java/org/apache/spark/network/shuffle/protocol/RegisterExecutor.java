package org.apache.spark.network.shuffle.protocol;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/9/4
 * Time: 21:28
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;

/**
 * Initial registration message between an executor and its local shuffle server.
 * Returns nothing (empty byte array).
 */
public class RegisterExecutor extends BlockTransferMessage {
    public final String appId;
    public final String execId;
    public final ExecutorShuffleInfo executorInfo;
    public RegisterExecutor(
            String appId,
            String execId,
            ExecutorShuffleInfo executorInfo) {
        this.appId = appId;
        this.execId = execId;
        this.executorInfo = executorInfo;
    }

    @Override
    protected Type type() { return Type.REGISTER_EXECUTOR; }

    @Override
    public int hashCode() {
        return Objects.hashCode(appId, execId, executorInfo);
    }
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("appId", appId)
                .add("execId", execId)
                .add("executorInfo", executorInfo)
                .toString();
    }
    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof RegisterExecutor) {
            RegisterExecutor o = (RegisterExecutor) other;
            return Objects.equal(appId, o.appId)
                    && Objects.equal(execId, o.execId)
                    && Objects.equal(executorInfo, o.executorInfo);
        }
        return false;
    }
    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId)
                + Encoders.Strings.encodedLength(execId)
                + executorInfo.encodedLength();
    }
    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        Encoders.Strings.encode(buf, execId);
        executorInfo.encode(buf);
    }
    public static RegisterExecutor decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        ExecutorShuffleInfo executorShuffleInfo = ExecutorShuffleInfo.decode(buf);
        return new RegisterExecutor(appId, execId, executorShuffleInfo);
    }
}
