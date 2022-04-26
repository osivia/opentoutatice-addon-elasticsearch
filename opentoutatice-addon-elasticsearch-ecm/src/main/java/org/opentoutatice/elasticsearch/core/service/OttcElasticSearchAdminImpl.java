/**
 *
 */
package org.opentoutatice.elasticsearch.core.service;

import static org.nuxeo.elasticsearch.ElasticSearchConstants.ALL_FIELDS;
import static org.nuxeo.elasticsearch.ElasticSearchConstants.DOC_TYPE;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.nuxeo.elasticsearch.config.ElasticSearchIndexConfig;
import org.nuxeo.elasticsearch.config.ElasticSearchLocalConfig;
import org.nuxeo.elasticsearch.config.ElasticSearchRemoteConfig;
import org.nuxeo.runtime.api.Framework;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchAdmin;
import org.opentoutatice.elasticsearch.config.OttcElasticSearchIndexOrAliasConfig;
import org.opentoutatice.elasticsearch.config.exception.AliasConfigurationException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.IndexNAliasManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.test.EsNodeTestInitializer;
import org.opentoutatice.elasticsearch.core.reindexing.docs.transitory.TransitoryIndexUse;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
// Extends ElasticSearchAdminImpl to respect other services signatures (ElasticSearchIndexing, AlasticSearchService).
// Eg, cf ElasticSearchCompoent#applicationStarted()
public class OttcElasticSearchAdminImpl /* extends ElasticSearchAdminImpl */ implements OttcElasticSearchAdmin {

    private static final Log log = LogFactory.getLog(OttcElasticSearchAdminImpl.class);

    private static final String TIMEOUT_WAIT_FOR_CLUSTER = "30s";

    final AtomicInteger totalCommandProcessed = new AtomicInteger(0);

    private final Map<String, String> indexNames = new HashMap<>();

    private final Map<String, String> repoNames = new HashMap<>();

    private final Map<String, ElasticSearchIndexConfig> indexConfig;

    private Node localNode;

    private Client client;

    private boolean indexInitDone = false;

    private final ElasticSearchLocalConfig localConfig;

    private final ElasticSearchRemoteConfig remoteConfig;

    private String[] includeSourceFields;

    private String[] excludeSourceFields;

    private boolean embedded = true;

    private List<String> repositoryInitialized = new ArrayList<>();

    /**
     * Init the admin service, remote configuration if not null will take precedence
     * over local embedded configuration.
     *
     * @throws RealIndexNameException
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws IndexExistenceException
     */
    public OttcElasticSearchAdminImpl(ElasticSearchLocalConfig localConfig, ElasticSearchRemoteConfig remoteConfig,
            Map<String, ElasticSearchIndexConfig> indexConfig) throws InterruptedException, ExecutionException {
        this.remoteConfig = remoteConfig;
        this.localConfig = localConfig;
        this.indexConfig = indexConfig;

        this.connect();
        // Zero down time re-indexing FORK ============
        IndexNAliasManager.init(this.client.admin());
        this.initializeIndexes();
    }

    private void connect() {
        if (this.client != null) {
            return;
        }
        if (this.remoteConfig != null) {
            this.client = this.connectToRemote(this.remoteConfig);
            this.embedded = false;
        } else {
            this.localNode = this.createEmbeddedNode(this.localConfig);
            this.client = this.connectToEmbedded();
            this.embedded = true;

            // Zero down time Re-indexing FORK ==================
            if (Framework.isTestModeSet()) {
                EsNodeTestInitializer.initializeEsNodeInTestMode(this.client, log);
            }
        }
        this.checkClusterHealth();
        log.info("ES Connected");
    }

    public void disconnect() {
        
        // Test mode
        if (Framework.isTestModeSet()) {
            EsNodeTestInitializer.cleanIndices(this.client, log);
        }

        if (this.client != null) {
            this.client.close();
            this.client = null;
            this.indexInitDone = false;
            log.info("ES Disconnected");
        }
        if (this.localNode != null) {
            this.localNode.close();
            this.localNode = null;

            log.info("ES embedded Node Stopped");
        }
        
    }

    private Node createEmbeddedNode(ElasticSearchLocalConfig conf) {
        log.info("ES embedded Node Initializing (local in JVM)");
        if (conf == null) {
            throw new IllegalStateException("No embedded configuration defined");
        }
        if (!Framework.isTestModeSet()) {
            log.warn("Elasticsearch embedded configuration is ONLY for testing" + " purpose. You need to create a dedicated Elasticsearch"
                    + " cluster for production.");
        }
        Builder sBuilder = ImmutableSettings.settingsBuilder();
        sBuilder.put("http.enabled", conf.httpEnabled()).put("path.data", conf.getDataPath()).put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0).put("cluster.name", conf.getClusterName()).put("node.name", conf.getNodeName())
                .put("http.netty.worker_count", 4).put("cluster.routing.allocation.disk.threshold_enabled", false);
        if (conf.getIndexStorageType() != null) {
            sBuilder.put("index.store.type", conf.getIndexStorageType());
            if (conf.getIndexStorageType().equals("memory")) {
                sBuilder.put("gateway.type", "none");
            }
        }
        Settings settings = sBuilder.build();
        log.debug("Using settings: " + settings.toDelimitedString(','));
        Node ret = NodeBuilder.nodeBuilder().local(true).settings(settings).node();
        assert ret != null : "Can not create an embedded ES Node";
        return ret;
    }

    private Client connectToEmbedded() {
        log.info("Connecting to embedded ES");
        Client ret = this.localNode.start().client();
        assert ret != null : "Can not connect to embedded ES Node";
        return ret;
    }

    private Client connectToRemote(ElasticSearchRemoteConfig config) {
        log.info("Connecting to remote ES cluster: " + config);
        Builder builder = ImmutableSettings.settingsBuilder().put("cluster.name", config.getClusterName())
                .put("client.transport.nodes_sampler_interval", config.getSamplerInterval()).put("client.transport.ping_timeout", config.getPingTimeout())
                .put("client.transport.ignore_cluster_name", config.isIgnoreClusterName()).put("client.transport.sniff", config.isClusterSniff());
        Settings settings = builder.build();
        if (log.isDebugEnabled()) {
            log.debug("Using settings: " + settings.toDelimitedString(','));
        }
        TransportClient ret = new TransportClient(settings);
        String[] addresses = config.getAddresses();
        if (addresses == null) {
            log.error("You need to provide an addressList to join a cluster");
        } else {
            for (String item : config.getAddresses()) {
                String[] address = item.split(":");
                log.debug("Add transport address: " + item);
                try {
                    InetAddress inet = InetAddress.getByName(address[0]);
                    ret.addTransportAddress(new InetSocketTransportAddress(inet, Integer.parseInt(address[1])));
                } catch (UnknownHostException e) {
                    log.error("Unable to resolve host " + address[0], e);
                }
            }
        }
        assert ret != null : "Unable to create a remote client";
        return ret;
    }

    // TMP: public for JUnit tests
    public void checkClusterHealth(String... indexNames) {
        if (this.client == null) {
            throw new IllegalStateException("No es client available");
        }
        String errorMessage = null;
        try {
            log.debug("Waiting for cluster yellow health status, indexes: " + Arrays.toString(indexNames));
            ClusterHealthResponse ret = this.client.admin().cluster().prepareHealth(indexNames).setTimeout(TIMEOUT_WAIT_FOR_CLUSTER).setWaitForYellowStatus()
                    .get();
            if (ret.isTimedOut()) {
                errorMessage = "ES Cluster health status not Yellow after " + TIMEOUT_WAIT_FOR_CLUSTER + " give up: " + ret;
            } else {
                if ((indexNames.length > 0) && (ret.getStatus() != ClusterHealthStatus.GREEN)) {
                    log.warn("Es Cluster ready but not GREEN: " + ret);
                } else {
                    log.info("ES Cluster ready: " + ret);
                }
            }
        } catch (NoNodeAvailableException e) {
            errorMessage = "Failed to connect to elasticsearch, check addressList and clusterName: " + e.getMessage();
        }
        if (errorMessage != null) {
            log.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }
    }

    private void initializeIndexes() {
        for (ElasticSearchIndexConfig conf : this.indexConfig.values()) {
            if (DOC_TYPE.equals(conf.getType())) {
                log.info("Associate index " + conf.getName() + " with repository: " + conf.getRepositoryName());
                this.indexNames.put(conf.getRepositoryName(), conf.getName());
                this.repoNames.put(conf.getName(), conf.getRepositoryName());
                Set<String> set = new LinkedHashSet<>();
                if (this.includeSourceFields != null) {
                    set.addAll(Arrays.asList(this.includeSourceFields));
                }
                set.addAll(Arrays.asList(conf.getIncludes()));
                if (set.contains(ALL_FIELDS)) {
                    set.clear();
                    set.add(ALL_FIELDS);
                }
                this.includeSourceFields = set.toArray(new String[set.size()]);
                set.clear();
                if (this.excludeSourceFields != null) {
                    set.addAll(Arrays.asList(this.excludeSourceFields));
                }
                set.addAll(Arrays.asList(conf.getExcludes()));
                this.excludeSourceFields = set.toArray(new String[set.size()]);
            }

        }
        this.initIndexes(false);
    }

    // Admin Impl =============================================================
    @Override
    public void refreshRepositoryIndex(String repositoryName) {
        if (log.isDebugEnabled()) {
            log.debug("Refreshing index associated with repo: " + repositoryName);
        }
        this.getClient().admin().indices().prepareRefresh(this.getIndexNameForRepository(repositoryName)).execute().actionGet();
        if (log.isDebugEnabled()) {
            log.debug("Refreshing index done");
        }
    }

    @Override
    public void flushRepositoryIndex(String repositoryName) {
        log.warn("Flushing index associated with repo: " + repositoryName);
        this.getClient().admin().indices().prepareFlush(this.getIndexNameForRepository(repositoryName)).execute().actionGet();
        log.info("Flushing index done");
    }

    @Override
    public void refresh() {
        for (String repositoryName : this.indexNames.keySet()) {
            this.refreshRepositoryIndex(repositoryName);
        }
    }

    @Override
    public void flush() {
        for (String repositoryName : this.indexNames.keySet()) {
            this.flushRepositoryIndex(repositoryName);
        }
    }

    @Override
    public void optimizeIndex(String indexName) {
        log.warn("Optimizing index: " + indexName);
        for (ElasticSearchIndexConfig conf : this.indexConfig.values()) {
            if (conf.getName().equals(indexName)) {
                this.getClient().admin().indices().prepareOptimize(indexName).get();
            }
        }
        log.info("Optimize done");
    }

    @Override
    public void optimizeRepositoryIndex(String repositoryName) {
        this.optimizeIndex(this.getIndexNameForRepository(repositoryName));
    }

    @Override
    public void optimize() {
        for (ElasticSearchIndexConfig conf : this.indexConfig.values()) {
            this.optimizeIndex(conf.getName());
        }
    }

    @Override
    public Client getClient() {
        return this.client;
    }

    @Override
    public void initIndexes(boolean dropIfExists) {
        this.indexInitDone = false;
        for (ElasticSearchIndexConfig conf : this.indexConfig.values()) {
            this.initIndex(conf, dropIfExists);
        }
        log.info("ES Service ready");
        this.indexInitDone = true;
    }

    @Override
    public void dropAndInitIndex(String indexName) {
        log.info("Drop and init index: " + indexName);
        this.indexInitDone = false;
        for (ElasticSearchIndexConfig conf : this.indexConfig.values()) {
            if (conf.getName().equals(indexName)) {
                this.initIndex(conf, true);
            }
        }
        this.indexInitDone = true;
    }

    @Override
    public void dropAndInitRepositoryIndex(String repositoryName) {
        log.info("Drop and init index of repository: " + repositoryName);
        this.indexInitDone = false;
        for (ElasticSearchIndexConfig conf : this.indexConfig.values()) {
            if (DOC_TYPE.equals(conf.getType()) && repositoryName.equals(conf.getRepositoryName())) {
                this.initIndex(conf, true);
            }
        }
        this.indexInitDone = true;
    }

    @Override
    public List<String> getRepositoryNames() {
        return Collections.unmodifiableList(new ArrayList<>(this.indexNames.keySet()));
    }

    // Zero downtime Re-indexing FORK ======================
    public void initIndex(ElasticSearchIndexConfig conf, boolean dropIfExists) {
        if (!conf.mustCreate()) {

            // Nx configured with alias: must exist
            if (this.aliasConfigured(conf.getRepositoryName())) {
                if (!IndexNAliasManager.get().aliasExists(conf.getName())) {
                    // Update confs
                    this.getIndexNames().put(conf.getRepositoryName(), null);
                    this.getRepoNames().remove(conf.getName());

                    log.fatal(new AliasConfigurationException(
                            String.format("Alias [%s] does not exist: you must create it before use alias mode.", conf.getName())));
                }
            }

            return;
        }
        log.info(String.format("Initialize index: %s, type: %s", conf.getName(), conf.getType()));
        boolean mappingExists = false;
        boolean indexExists = this.getClient().admin().indices().prepareExists(conf.getName()).execute().actionGet().isExists();
        if (indexExists) {
            if (!dropIfExists) {
                log.debug("Index " + conf.getName() + " already exists");
                mappingExists = this.getClient().admin().indices().prepareGetMappings(conf.getName()).execute().actionGet().getMappings().get(conf.getName())
                        .containsKey(DOC_TYPE);
            } else {
                if (!Framework.isTestModeSet()) {
                    log.warn(String.format("Initializing index: %s, type: %s with " + "dropIfExists flag, deleting an existing index", conf.getName(),
                            conf.getType()));
                }
                this.getClient().admin().indices().delete(new DeleteIndexRequest(conf.getName())).actionGet();
                indexExists = false;
            }
        }
        if (!indexExists) {
            log.info(String.format("Creating index: %s", conf.getName()));
            if (log.isDebugEnabled()) {
                log.debug("Using settings: " + conf.getSettings());
            }
            this.getClient().admin().indices().prepareCreate(conf.getName()).setSettings(conf.getSettings()).execute().actionGet();
        }
        if (!mappingExists) {
            log.info(String.format("Creating mapping type: %s on index: %s", conf.getType(), conf.getName()));
            if (log.isDebugEnabled()) {
                log.debug("Using mapping: " + conf.getMapping());
            }
            this.getClient().admin().indices().preparePutMapping(conf.getName()).setType(conf.getType()).setSource(conf.getMapping()).execute().actionGet();
            if (!dropIfExists && (conf.getRepositoryName() != null)) {
                this.repositoryInitialized.add(conf.getRepositoryName());
            }
        }
        // make sure the index is ready before returning
        this.checkClusterHealth(conf.getName());
    }

    @Override
    public int getPendingWorkerCount() {
        // impl of scheduling is left to the ESService
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getRunningWorkerCount() {
        // impl of scheduling is left to the ESService
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getTotalCommandProcessed() {
        return this.totalCommandProcessed.get();
    }

    @Override
    public boolean isEmbedded() {
        return this.embedded;
    }

    @Override
    public boolean useExternalVersion() {
        if (this.isEmbedded()) {
            return this.localConfig.useExternalVersion();
        }
        return this.remoteConfig.useExternalVersion();
    }

    @Override
    public boolean isIndexingInProgress() {
        // impl of scheduling is left to the ESService
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ListenableFuture<Boolean> prepareWaitForIndexing() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /*
     * ========================================================= Fork part for
     * Re-indexing with zero down time
     * =========================================================
     */

    /**
     * Get the elastic search indexes for searches
     *
     * @throws InterruptedException
     */
    // For reading use
    public String[] getSearchIndexes(List<String> searchRepositories) {
        // String[] ret = null;

        if (searchRepositories.isEmpty()) {
            // FIXME!!!
            Collection<String> values = this.indexNames.values();
            return values.toArray(new String[values.size()]);
        }
        // ret = new String[searchRepositories.size()];
        // int i = 0;
        List<String> indices = new ArrayList<String>(searchRepositories.size());
        for (String repo : searchRepositories) {
            try {
                if (ReIndexingRunnerManager.get().isReIndexingInProgress(repo)) {
                    for (String idx : this.getReadIndicesForReIndexingRepository(repo)) {
                        // ret[i++] = idx;
                        indices.add(idx);
                    }
                } else {
                    // ret[i++] = getConfiguredIndexOrAliasNameForRepository(repo);
                    indices.add(this.getConfiguredIndexOrAliasNameForRepository(repo));
                }
            } catch (InterruptedException e) {
                // TODO: throw blocking exception if too many InterruptedException
                if (log.isErrorEnabled()) {
                    log.error(e);
                }
            }
        }
        return indices.toArray(new String[indices.size()]);
    }

    public String[] getReadIndicesForReIndexingRepository(String repositoryName) {
        String[] res = null;

        List<String> transientReadIndices = IndexNAliasManager.get().getIndicesOfAlias(TransitoryIndexUse.Read.getAlias());
        if (transientReadIndices != null) {
            res = new String[0];
            res = transientReadIndices.toArray(res);
        }

        return res;
    }

    // For writing use
    @Override
    public String getIndexNameForRepository(String repositoryName) {
        String writeIndexOrAlias = null;
        try {
            if (ReIndexingRunnerManager.get().isReIndexingInProgress(repositoryName)) {
                writeIndexOrAlias = TransitoryIndexUse.Write.getAlias();
            } else {
                writeIndexOrAlias = this.getConfiguredIndexOrAliasNameForRepository(repositoryName);
            }

            if (log.isTraceEnabled()) {
                log.trace(String.format("Write alias: [%s]", writeIndexOrAlias));
            }
        } catch (InterruptedException e) {
            // TODO: throw blocking exception if too many InterruptedException
            if (log.isErrorEnabled()) {
                log.error(e);
            }
        }

        return writeIndexOrAlias;
    }

    @Override
    public String getConfiguredIndexOrAliasNameForRepository(String repositoryName) {
        String ret = this.indexNames.get(repositoryName);
        if (ret == null) {
            throw new NoSuchElementException("No index or alias defined for repository: " + repositoryName);
        }
        return ret;
    }
    
    @Override
    public boolean aliasConfigured(String repositoryName) {
        ElasticSearchIndexConfig esCfg = this.getIndexConfig().get(this.getIndexNames().get(repositoryName));
        return (esCfg instanceof OttcElasticSearchIndexOrAliasConfig) && (((OttcElasticSearchIndexOrAliasConfig) esCfg).aliasConfigured());
    }

    @Override
    public boolean isZeroDownTimeReIndexingInProgress(String repository) throws InterruptedException {
        return ReIndexingRunnerManager.get().isReIndexingInProgress(repository);
    }

    public Map<String, ElasticSearchIndexConfig> getIndexConfig() {
        return this.indexConfig;
    }

    @Override
    public Map<String, String> getIndexNames() {
        return this.indexNames;
    }

    @Override
    public Map<String, String> getRepoNames() {
        return this.repoNames;
    }

    /* ============================================================= */

    public boolean isReady() {
        return this.indexInitDone;
    }

    String[] getIncludeSourceFields() {
        return this.includeSourceFields;
    }

    String[] getExcludeSourceFields() {
        return this.excludeSourceFields;
    }

    Map<String, String> getRepositoryMap() {
        return this.repoNames;
    }

    /**
     * Get the list of repository names that have their index created.
     */
    public List<String> getInitializedRepositories() {
        return this.repositoryInitialized;
    }

}
