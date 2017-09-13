package org.apache.spark.network.shuffle;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.sasl.SaslClientBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.RegisterExecutor;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/9/11
 * Time: 22:18
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public class ExternalShuffleClient extends ShuffleClient{
    private final Logger logger = LoggerFactory.getLogger(ExternalShuffleClient.class);
    private final TransportConf conf;
    private final boolean saslEnabled;
    private final boolean saslEncryptionEnabled;
    private final SecretKeyHolder secretKeyHolder;
    protected TransportClientFactory clientFactory;
    protected String appId;

    /**
     * Creates an external shuffle client, with SASL optionally enabled. If SASL is not enabled,
     * then secretKeyHolder may be null.
     */
    public ExternalShuffleClient(
            TransportConf conf,
            SecretKeyHolder secretKeyHolder,
            boolean saslEnabled,
            boolean saslEncryptionEnabled) {
        Preconditions.checkArgument(
                !saslEncryptionEnabled || saslEnabled,
                "SASL encryption can only be enabled if SASL is also enabled.");
        this.conf = conf;
        this.secretKeyHolder = secretKeyHolder;
        this.saslEnabled = saslEnabled;
        this.saslEncryptionEnabled = saslEncryptionEnabled;
    }
    @Override
    public void init(String appId) {
        this.appId = appId;
        TransportContext context = new TransportContext(conf, new NoOpRpcHandler(), true);
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        if (saslEnabled) {
            bootstraps.add(new SaslClientBootstrap(conf, appId, secretKeyHolder, saslEncryptionEnabled));
        }
        clientFactory = context.createClientFactory(bootstraps);
    }
    @Override
    public void fetchBlocks(
            final String host,
            final int port,
            final String execId,
            String[] blockIds,
            BlockFetchingListener listener) {
        checkInit();
        try {
            RetryingBlockFetcher.BlockFetchStarter blockFetchStarter =
                    new RetryingBlockFetcher.BlockFetchStarter() {
                        @Override
                        public void createAndStart(String[] blockIds, BlockFetchingListener listener)
                                throws IOException {
                            TransportClient client = clientFactory.createClient(host, port);
                            new OneForOneBlockFetcher(client, appId, execId, blockIds, listener).start();
                        }
                    };

            int maxRetries = conf.maxIORetries();
            if (maxRetries > 0) {
                // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
                // a bug in this code. We should remove the if statement once we're sure of the stability.
                new RetryingBlockFetcher(conf, blockFetchStarter, blockIds, listener).start();
            } else {
                blockFetchStarter.createAndStart(blockIds, listener);
            }
        } catch (Exception e) {
            logger.error("Exception while beginning fetchBlocks", e);
            for (String blockId : blockIds) {
                listener.onBlockFetchFailure(blockId, e);
            }
        }

    }
    protected void checkInit() {
        assert appId != null : "Called before init()";
    }
    /**
     * Registers this executor with an external shuffle server. This registration is required to
     * inform the shuffle server about where and how we store our shuffle files.
     *
     * @param host Host of shuffle server.
     * @param port Port of shuffle server.
     * @param execId This Executor's id.
     * @param executorInfo Contains all info necessary for the service to find our shuffle files.
     */
    public void registerWithShuffleServer(
            String host,
            int port,
            String execId,
            ExecutorShuffleInfo executorInfo) throws IOException {
        checkInit();
        TransportClient client = clientFactory.createUnmanagedClient(host, port);
        try {
            ByteBuffer registerMessage = new RegisterExecutor(appId, execId, executorInfo).toByteBuffer();
            client.sendRpcSync(registerMessage, 5000 /* timeoutMs */);
        } finally {
            client.close();
        }
    }
    @Override
    public void close() {
        clientFactory.close();
    }
}
