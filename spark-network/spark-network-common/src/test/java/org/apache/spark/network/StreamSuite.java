package org.apache.spark.network;

import com.google.common.io.Files;
import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.util.SystemPropertyConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.junit.BeforeClass;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/28
 * Time: 22:08
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public class StreamSuite {
    private static final String[] STREAMS = { "largeBuffer", "smallBuffer", "emptyBuffer", "file" };

    private static TransportServer server;
    private static TransportClientFactory clientFactory;
    private static File testFile;
    private static File tempDir;

    private static ByteBuffer emptyBuffer;
    private static ByteBuffer smallBuffer;
    private static ByteBuffer largeBuffer;
    private static ByteBuffer createBuffer(int bufSize) {
        ByteBuffer buf = ByteBuffer.allocate(bufSize);
        for (int i = 0; i < bufSize; i ++) {
            buf.put((byte) i);
        }
        buf.flip();
        return buf;
    }
    @BeforeClass
    public static void setUp() throws Exception {
        tempDir = Files.createTempDir();
        emptyBuffer = createBuffer(0);
        smallBuffer = createBuffer(100);
        largeBuffer = createBuffer(100000);
        testFile = File.createTempFile("stream-test-file", "txt", tempDir);
        FileOutputStream fp = new FileOutputStream(testFile);
        try {
            Random rnd = new Random();
            for (int i = 0; i < 512; i++) {
                byte[] fileContent = new byte[1024];
                rnd.nextBytes(fileContent);
                fp.write(fileContent);
            }
        } finally {
            fp.close();
        }
        final TransportConf conf = new TransportConf("shuffle", new SystemPropertyConfigProvider());
        final StreamManager streamManager = new StreamManager() {
            @Override
            public ManagedBuffer getChunk(long streamId, int chunkIndex) {
                throw new UnsupportedOperationException();
            }
            @Override
            public ManagedBuffer openStream(String streamId) {
                switch (streamId) {
                    case "largeBuffer":
                        return new NioManagedBuffer(largeBuffer);
                    case "smallBuffer":
                        return new NioManagedBuffer(smallBuffer);
                    case "emptyBuffer":
                        return new NioManagedBuffer(emptyBuffer);
                    case "file":
                        return new FileSegmentManagedBuffer(conf, testFile, 0, testFile.length());
                    default:
                        throw new IllegalArgumentException("Invalid stream: " + streamId);
                }
            }


        };
        RpcHandler handler = new RpcHandler() {
            @Override
            public void receive(
                    TransportClient client,
                    ByteBuffer message,
                    RpcResponseCallback callback) {
                throw new UnsupportedOperationException();
            }

            @Override
            public StreamManager getStreamManager() {
                return streamManager;
            }
        };
        TransportContext context = new TransportContext(conf, handler);
        server = context.createServer();
        clientFactory = context.createClientFactory();

    }
}
