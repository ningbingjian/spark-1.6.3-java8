package org.apache.spark.network;

import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;

import java.io.File;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/21
 * Time: 21:34
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */

public class ChunkFetchIntegrationSuite {
    static final long STREAM_ID = 1;
    static final int BUFFER_CHUNK_INDEX = 0;
    static final int FILE_CHUNK_INDEX = 1;
    static TransportServer server;
    static TransportClientFactory clientFactory;
    static StreamManager streamManager;
    static File testFile;
}
