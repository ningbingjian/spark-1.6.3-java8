package org.apache.spark.network.protocol;

import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/19
 * Time: 12:00
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public abstract class AbstractResponseMessage extends AbstractMessage implements ResponseMessage {
    protected AbstractResponseMessage(ManagedBuffer body, boolean isBodyInFrame) {
        super(body, isBodyInFrame);
    }

    public abstract ResponseMessage createFailureResponse(String error);
}
