package org.apache.spark.network.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ThreadFactory;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/13
 * Time: 20:56
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public class NettyUtils {
    /** Creates a new ThreadFactory which prefixes each thread with the given name. */
    public static ThreadFactory createThreadFactory(String threadPoolPrefix) {
        return new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(threadPoolPrefix + "-%d")
                .build();
    }
}
