package org.apache.spark.network;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.util.SystemPropertyConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

    static ManagedBuffer bufferChunk;
    static ManagedBuffer fileChunk;
    @BeforeClass
    public static void setUp() throws Exception {
        int bufSize = 100000;
        final ByteBuffer buf = ByteBuffer.allocate(bufSize);
        for (int i = 0; i < bufSize; i ++) {
            buf.put((byte) i);
        }
        buf.flip();
        bufferChunk = new NioManagedBuffer(buf);
        testFile = File.createTempFile("shuffle-test-file", "txt");
        testFile.deleteOnExit();
        RandomAccessFile fp = new RandomAccessFile(testFile, "rw");
        boolean shouldSuppressIOException = true;
        try {
            byte[] fileContent = new byte[1024];
            new Random().nextBytes(fileContent);
            fp.write(fileContent);
            shouldSuppressIOException = false;
        }finally {
            Closeables.close(fp, shouldSuppressIOException);
        }
        final TransportConf conf = new TransportConf("shuffle",
                new SystemPropertyConfigProvider());
        fileChunk = new FileSegmentManagedBuffer(conf, testFile, 10, testFile.length() - 25);
        streamManager = new StreamManager() {
            @Override
            public ManagedBuffer getChunk(long streamId, int chunkIndex) {
                assertEquals(STREAM_ID, streamId);
                if (chunkIndex == BUFFER_CHUNK_INDEX) {
                    return new NioManagedBuffer(buf);
                } else if (chunkIndex == FILE_CHUNK_INDEX) {
                    return new FileSegmentManagedBuffer(conf, testFile, 10, testFile.length() - 25);
                } else {
                    throw new IllegalArgumentException("Invalid chunk index: " + chunkIndex);
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
    @AfterClass
    public static void tearDown() {
        bufferChunk.release();
        server.close();
        clientFactory.close();
        testFile.delete();
    }
    class FetchResult {
        public Set<Integer> successChunks;
        public Set<Integer> failedChunks;
        public List<ManagedBuffer> buffers;

        public void releaseBuffers() {
            for (ManagedBuffer buffer : buffers) {
                buffer.release();
            }
        }
    }
    private FetchResult fetchChunks(List<Integer> chunkIndices) throws Exception {
        TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
        final Semaphore sem = new Semaphore(0);
        final FetchResult res = new FetchResult();
        res.successChunks = Collections.synchronizedSet(new HashSet<Integer>());
        res.failedChunks = Collections.synchronizedSet(new HashSet<Integer>());
        res.buffers = Collections.synchronizedList(new LinkedList<ManagedBuffer>());
        ChunkReceivedCallback callback = new ChunkReceivedCallback() {
            @Override
            public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
                buffer.retain();
                res.successChunks.add(chunkIndex);
                res.buffers.add(buffer);
                sem.release();
            }
            @Override
            public void onFailure(int chunkIndex, Throwable e) {
                res.failedChunks.add(chunkIndex);
                sem.release();
            }
        };

        for (int chunkIndex : chunkIndices) {
            client.fetchChunk(STREAM_ID, chunkIndex, callback);
        }
        if (!sem.tryAcquire(chunkIndices.size(), 5, TimeUnit.SECONDS)) {
            fail("Timeout getting response from the server");
        }
        client.close();
        return res;
    }
    @Test
    public void fetchBufferChunk() throws Exception {
        FetchResult res = fetchChunks(Lists.newArrayList(BUFFER_CHUNK_INDEX));
        assertEquals(res.successChunks, Sets.newHashSet(BUFFER_CHUNK_INDEX));
        assertTrue(res.failedChunks.isEmpty());
        assertBufferListsEqual(res.buffers, Lists.newArrayList(bufferChunk));
        res.releaseBuffers();

    }
    @Test
    public void fetchBothChunks() throws Exception {
        FetchResult res = fetchChunks(Lists.newArrayList(BUFFER_CHUNK_INDEX, FILE_CHUNK_INDEX));
        assertEquals(res.successChunks, Sets.newHashSet(BUFFER_CHUNK_INDEX, FILE_CHUNK_INDEX));
        assertTrue(res.failedChunks.isEmpty());
        assertBufferListsEqual(res.buffers, Lists.newArrayList(bufferChunk, fileChunk));
        res.releaseBuffers();
    }
    @Test
    public void fetchNonExistentChunk() throws Exception {
        FetchResult res = fetchChunks(Lists.newArrayList(12345));
        assertTrue(res.successChunks.isEmpty());
        assertEquals(res.failedChunks, Sets.newHashSet(12345));
        assertTrue(res.buffers.isEmpty());
    }

    private void assertBufferListsEqual(List<ManagedBuffer> list0, List<ManagedBuffer> list1)
            throws Exception {
        assertEquals(list0.size(), list1.size());
        for (int i = 0; i < list0.size(); i ++) {
            assertBuffersEqual(list0.get(i), list1.get(i));
        }
    }
    private void assertBuffersEqual(ManagedBuffer buffer0, ManagedBuffer buffer1) throws Exception {
        ByteBuffer nio0 = buffer0.nioByteBuffer();
        ByteBuffer nio1 = buffer1.nioByteBuffer();

        int len = nio0.remaining();
        assertEquals(nio0.remaining(), nio1.remaining());
        for (int i = 0; i < len; i ++) {
            assertEquals(nio0.get(), nio1.get());
        }
    }
}
