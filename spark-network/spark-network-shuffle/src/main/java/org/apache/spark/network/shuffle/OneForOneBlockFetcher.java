package org.apache.spark.network.shuffle;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/9/12
 * Time: 22:04
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.StreamHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Simple wrapper on top of a TransportClient which interprets each chunk as a whole block, and
 * invokes the BlockFetchingListener appropriately. This class is agnostic to the actual RPC
 * handler, as long as there is a single "open blocks" message which returns a ShuffleStreamHandle,
 * and Java serialization is used.
 *
 * Note that this typically corresponds to a
 * {@link org.apache.spark.network.server.OneForOneStreamManager} on the server side.
 */
public class OneForOneBlockFetcher {
    private final Logger logger = LoggerFactory.getLogger(OneForOneBlockFetcher.class);

    private final TransportClient client;
    private final OpenBlocks openMessage;
    private final String[] blockIds;
    private final BlockFetchingListener listener;
    private final ChunkReceivedCallback chunkCallback;

    private StreamHandle streamHandle = null;
    public OneForOneBlockFetcher(
            TransportClient client,
            String appId,
            String execId,
            String[] blockIds,
            BlockFetchingListener listener) {
        this.client = client;
        this.openMessage = new OpenBlocks(appId, execId, blockIds);
        this.blockIds = blockIds;
        this.listener = listener;
        this.chunkCallback = new ChunkCallback();
    }
    /** Callback invoked on receipt of each chunk. We equate a single chunk to a single block. */
    private class ChunkCallback implements ChunkReceivedCallback {
        @Override
        public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
            // On receipt of a chunk, pass it upwards as a block.
            listener.onBlockFetchSuccess(blockIds[chunkIndex], buffer);
        }

        @Override
        public void onFailure(int chunkIndex, Throwable e) {
            // On receipt of a failure, fail every block from chunkIndex onwards.
            String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
            failRemainingBlocks(remainingBlockIds, e);
        }
    }

    /**
     * Begins the fetching process, calling the listener with every block fetched.
     * The given message will be serialized with the Java serializer, and the RPC must return a
     * {@link StreamHandle}. We will send all fetch requests immediately, without throttling.
     */
    public void start() {
        if (blockIds.length == 0) {
            throw new IllegalArgumentException("Zero-sized blockIds array");
        }
        client.sendRpc(openMessage.toByteBuffer(), new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                try {
                    streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
                    logger.trace("Successfully opened blocks {}, preparing to fetch chunks.", streamHandle);

                    // Immediately request all chunks -- we expect that the total size of the request is
                    // reasonable due to higher level chunking in [[ShuffleBlockFetcherIterator]].
                    for (int i = 0; i < streamHandle.numChunks; i++) {
                        client.fetchChunk(streamHandle.streamId, i, chunkCallback);
                    }
                } catch (Exception e) {
                    logger.error("Failed while starting block fetches after success", e);
                    failRemainingBlocks(blockIds, e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error("Failed while starting block fetches", e);
                failRemainingBlocks(blockIds, e);
            }
        });
    }
    /** Invokes the "onBlockFetchFailure" callback for every listed block id. */
    private void failRemainingBlocks(String[] failedBlockIds, Throwable e) {
        for (String blockId : failedBlockIds) {
            try {
                listener.onBlockFetchFailure(blockId, e);
            } catch (Exception e2) {
                logger.error("Error in block fetch failure callback", e2);
            }
        }
    }
}
