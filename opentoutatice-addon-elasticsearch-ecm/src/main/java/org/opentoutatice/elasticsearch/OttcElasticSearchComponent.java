/**
 *
 */
package org.opentoutatice.elasticsearch;

import static org.nuxeo.elasticsearch.ElasticSearchConstants.ES_ENABLED_PROPERTY;
import static org.nuxeo.elasticsearch.ElasticSearchConstants.INDEXING_QUEUE_ID;
import static org.nuxeo.elasticsearch.ElasticSearchConstants.REINDEX_ON_STARTUP_PROPERTY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.Transaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.nuxeo.ecm.automation.jaxrs.io.documents.JsonESDocumentWriter;
import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.api.SortInfo;
import org.nuxeo.ecm.core.repository.RepositoryService;
import org.nuxeo.ecm.core.work.api.Work;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.elasticsearch.api.EsResult;
import org.nuxeo.elasticsearch.commands.IndexingCommand;
import org.nuxeo.elasticsearch.config.ElasticSearchDocWriterDescriptor;
import org.nuxeo.elasticsearch.config.ElasticSearchIndexConfig;
import org.nuxeo.elasticsearch.config.ElasticSearchLocalConfig;
import org.nuxeo.elasticsearch.config.ElasticSearchRemoteConfig;
import org.nuxeo.elasticsearch.query.NxQueryBuilder;
import org.nuxeo.elasticsearch.work.IndexingWorker;
import org.nuxeo.elasticsearch.work.ScrollingIndexingWorker;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.model.ComponentContext;
import org.nuxeo.runtime.model.ComponentInstance;
import org.nuxeo.runtime.model.DefaultComponent;
import org.nuxeo.runtime.transaction.TransactionHelper;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchAdmin;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchService;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception.ReIndexingStateException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception.ReIndexingStatusException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.runner.works.ScrollingReIndexingWorker;
import org.opentoutatice.elasticsearch.core.service.OttcElasticSearchAdminImpl;
import org.opentoutatice.elasticsearch.core.service.OttcElasticSearchIndexingImpl;
import org.opentoutatice.elasticsearch.core.service.OttcElasticSearchServiceImpl;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
public class OttcElasticSearchComponent extends DefaultComponent implements OttcElasticSearchAdmin, OttcElasticSearchIndexing, OttcElasticSearchService {

    private static final Log log = LogFactory.getLog(OttcElasticSearchComponent.class);

    private static final String EP_REMOTE = "elasticSearchRemote";

    private static final String EP_LOCAL = "elasticSearchLocal";

    public static final String EP_INDEX = "elasticSearchIndex";

    private static final String EP_DOC_WRITER = "elasticSearchDocWriter";

    private static final long REINDEX_TIMEOUT = 20;

    // Indexing commands that where received before the index initialization
    private final List<IndexingCommand> stackedCommands = Collections.synchronizedList(new ArrayList<IndexingCommand>());

    private final Map<String, ElasticSearchIndexConfig> indexConfig = new HashMap<>();

    private ElasticSearchLocalConfig localConfig;

    private ElasticSearchRemoteConfig remoteConfig;

    private OttcElasticSearchAdminImpl esa;

    private OttcElasticSearchIndexingImpl esi;

    private OttcElasticSearchServiceImpl ess;

    protected JsonESDocumentWriter jsonESDocumentWriter;

    private ListeningExecutorService waiterExecutorService;

    private final AtomicInteger runIndexingWorkerCount = new AtomicInteger(0);

    // Nuxeo Component impl ====================================================
    @Override
    public void registerContribution(Object contribution, String extensionPoint, ComponentInstance contributor) {
        switch (extensionPoint) {
            case EP_LOCAL:
                ElasticSearchLocalConfig localContrib = (ElasticSearchLocalConfig) contribution;
                if (localContrib.isEnabled()) {
                    this.localConfig = localContrib;
                    this.remoteConfig = null;
                    log.info("Registering local embedded configuration: " + this.localConfig + ", loaded from " + contributor.getName());
                } else if (this.localConfig != null) {
                    log.info("Disabling previous local embedded configuration, deactivated by " + contributor.getName());
                    this.localConfig = null;
                }
                break;
            case EP_REMOTE:
                ElasticSearchRemoteConfig remoteContribution = (ElasticSearchRemoteConfig) contribution;
                if (remoteContribution.isEnabled()) {
                    this.remoteConfig = remoteContribution;
                    this.localConfig = null;
                    log.info("Registering remote configuration: " + this.remoteConfig + ", loaded from " + contributor.getName());
                } else if (this.remoteConfig != null) {
                    log.info("Disabling previous remote configuration, deactivated by " + contributor.getName());
                    this.remoteConfig = null;
                }
                break;
            case EP_INDEX:
                ElasticSearchIndexConfig idx = (ElasticSearchIndexConfig) contribution;
                ElasticSearchIndexConfig previous = this.indexConfig.get(idx.getName());
                if (idx.isEnabled()) {
                    idx.merge(previous);
                    this.indexConfig.put(idx.getName(), idx);
                    log.info("Registering index configuration: " + idx + ", loaded from " + contributor.getName());
                } else if (previous != null) {
                    log.info("Disabling index configuration: " + previous + ", deactivated by " + contributor.getName());
                    this.indexConfig.remove(idx.getName());
                }
                break;
            case EP_DOC_WRITER:
                ElasticSearchDocWriterDescriptor writerDescriptor = (ElasticSearchDocWriterDescriptor) contribution;
                try {
                    this.jsonESDocumentWriter = writerDescriptor.getKlass().newInstance();
                } catch (IllegalAccessException | InstantiationException e) {
                    log.error("Can not instantiate jsonESDocumentWriter from " + writerDescriptor.getKlass());
                    throw new RuntimeException(e);
                }
                break;
            default:
                throw new IllegalStateException("Invalid EP: " + extensionPoint);
        }
    }

    // @Override
    // public void unregisterContribution(Object contribution, String extensionPoint, ComponentInstance contributor) {
    // switch (extensionPoint) {
    // case EP_INDEX:
    // ElasticSearchIndexConfig idx = (ElasticSearchIndexConfig) contribution;
    //
    // log.info("Unregistering index configuration: " + idx);
    // indexConfig.remove(idx.getName());
    // break;
    // default:
    // throw new IllegalStateException("Invalid EP: " + extensionPoint);
    //
    // }
    // }

    @Override
    public void applicationStarted(ComponentContext context) throws InterruptedException, ExecutionException {
        if (!this.isElasticsearchEnabled()) {
            log.info("Elasticsearch service is disabled");
            return;
        }

        this.esa = new OttcElasticSearchAdminImpl(this.localConfig, this.remoteConfig, this.indexConfig);
        this.esi = new OttcElasticSearchIndexingImpl(this.esa, this.jsonESDocumentWriter);
        this.ess = new OttcElasticSearchServiceImpl(this.esa);
        this.initListenerThreadPool();
        this.processStackedCommands();
        this.reindexOnStartup();
    }

    private void reindexOnStartup() {
        boolean reindexOnStartup = Boolean.parseBoolean(Framework.getProperty(REINDEX_ON_STARTUP_PROPERTY, "false"));
        if (!reindexOnStartup) {
            return;
        }
        for (String repositoryName : this.esa.getInitializedRepositories()) {
            log.warn(String.format("Indexing repository: %s on startup", repositoryName));
            this.runReindexingWorker(repositoryName, "SELECT ecm:uuid FROM Document");
            try {
                this.prepareWaitForIndexing().get(REINDEX_TIMEOUT, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                log.error(e.getMessage(), e);
            } catch (TimeoutException e) {
                log.warn(String.format("Indexation of repository %s not finised after %d s, continuing in background", repositoryName, REINDEX_TIMEOUT));
            }
        }
    }

    protected boolean isElasticsearchEnabled() {
        return Boolean.parseBoolean(Framework.getProperty(ES_ENABLED_PROPERTY, "true"));
    }

    @Override
    public void deactivate(ComponentContext context) {
        if (this.esa != null) {
            this.esa.disconnect();
        }
    }

    @Override
    public int getApplicationStartedOrder() {
        RepositoryService component = (RepositoryService) Framework.getRuntime().getComponent("org.nuxeo.ecm.core.repository.RepositoryServiceComponent");
        return component.getApplicationStartedOrder() / 2;
    }

    void processStackedCommands() {
        if (!this.stackedCommands.isEmpty()) {
            log.info(String.format("Processing %d indexing commands stacked during startup", this.stackedCommands.size()));
            this.runIndexingWorker(this.stackedCommands);
            this.stackedCommands.clear();
            log.debug("Done");
        }
    }

    // Es Admin ================================================================

    public synchronized OttcElasticSearchAdminImpl getElasticSearchAdmin() {
        return this.esa;
    }

    @Override
    public Client getClient() {
        return this.esa.getClient();
    }

    // ===

    @Override
    public Map<String, String> getIndexNames() {
        return this.esa.getIndexNames();
    }

    @Override
    public Map<String, String> getRepoNames() {
        return this.esa.getRepoNames();
    }

    // ===

    @Override
    public void initIndexes(boolean dropIfExists) {
        this.esa.initIndexes(dropIfExists);
    }

    @Override
    public void dropAndInitIndex(String indexName) {
        this.esa.dropAndInitIndex(indexName);
    }

    @Override
    public void dropAndInitRepositoryIndex(String repositoryName) {
        this.esa.dropAndInitRepositoryIndex(repositoryName);
    }

    @Override
    public List<String> getRepositoryNames() {
        return this.esa.getRepositoryNames();
    }

    @Override
    public String getIndexNameForRepository(String repositoryName) {
        return this.esa.getIndexNameForRepository(repositoryName);
    }

    @Override
    public int getPendingWorkerCount() {
        WorkManager wm = Framework.getLocalService(WorkManager.class);
        return wm.getQueueSize(INDEXING_QUEUE_ID, Work.State.SCHEDULED);
    }

    @Override
    public int getRunningWorkerCount() {
        WorkManager wm = Framework.getLocalService(WorkManager.class);
        return this.runIndexingWorkerCount.get() + wm.getQueueSize(INDEXING_QUEUE_ID, Work.State.RUNNING);
    }

    @Override
    public int getTotalCommandProcessed() {
        return this.esa.getTotalCommandProcessed();
    }

    @Override
    public boolean isEmbedded() {
        return this.esa.isEmbedded();
    }

    @Override
    public boolean useExternalVersion() {
        return this.esa.useExternalVersion();
    }

    // Zero Down Time Fork =============
    @Override
    public boolean isIndexingInProgress() {
        boolean is = (this.runIndexingWorkerCount.get() > 0) || (this.getPendingWorkerCount() > 0) || (this.getRunningWorkerCount() > 0);
        try {
            is = is || ReIndexingRunnerManager.get().isReIndexingInProgress();
        } catch (InterruptedException e) {
            // Do not block
            if (log.isErrorEnabled()) {
                log.error("Can not check Zero Down Time Re-Indexing existance: ", e);
            }
        }
        return is;
    }

    @Override
    public ListenableFuture<Boolean> prepareWaitForIndexing() {
        return this.waiterExecutorService.submit(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                WorkManager wm = Framework.getLocalService(WorkManager.class);
                wm.awaitCompletion(INDEXING_QUEUE_ID, 300, TimeUnit.SECONDS);
                return true;
            }
        });
    }

    private static class NamedThreadFactory implements ThreadFactory {

        @SuppressWarnings("NullableProblems")
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "waitForEsIndexing");
        }
    }

    protected void initListenerThreadPool() {
        this.waiterExecutorService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool(new NamedThreadFactory()));
    }

    @Override
    public void refresh() {
        this.esa.refresh();
    }

    @Override
    public void refreshRepositoryIndex(String repositoryName) {
        this.esa.refreshRepositoryIndex(repositoryName);
    }

    @Override
    public void flush() {
        this.esa.flush();
    }

    @Override
    public void flushRepositoryIndex(String repositoryName) {
        this.esa.flushRepositoryIndex(repositoryName);
    }

    @Override
    public void optimize() {
        this.esa.optimize();
    }

    @Override
    public void optimizeRepositoryIndex(String repositoryName) {
        this.esa.optimizeRepositoryIndex(repositoryName);
    }

    @Override
    public void optimizeIndex(String indexName) {
        this.esa.optimizeIndex(indexName);
    }

    // ES Indexing =============================================================

    @Override
    public void indexNonRecursive(IndexingCommand cmd) throws ClientException {
        List<IndexingCommand> cmds = new ArrayList<>(1);
        cmds.add(cmd);
        this.indexNonRecursive(cmds);
    }

    @Override
    public void indexNonRecursive(List<IndexingCommand> cmds) throws ClientException {
        if (!this.isReady()) {
            this.stackCommands(cmds);
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("Process indexing commands: " + Arrays.toString(cmds.toArray()));
        }
        this.esi.indexNonRecursive(cmds);
    }

    protected void stackCommands(List<IndexingCommand> cmds) {
        if (log.isDebugEnabled()) {
            log.debug("Delaying indexing commands: Waiting for Index to be initialized." + Arrays.toString(cmds.toArray()));
        }
        this.stackedCommands.addAll(cmds);
    }

    @Override
    public void runIndexingWorker(List<IndexingCommand> cmds) {
        if (!this.isReady()) {
            this.stackCommands(cmds);
            return;
        }
        this.runIndexingWorkerCount.incrementAndGet();
        try {
            this.dispatchWork(cmds);
        } finally {
            this.runIndexingWorkerCount.decrementAndGet();
        }
    }

    /**
     * Dispatch jobs between sync and async worker
     */
    protected void dispatchWork(List<IndexingCommand> cmds) {
        Map<String, List<IndexingCommand>> syncCommands = new HashMap<>();
        Map<String, List<IndexingCommand>> asyncCommands = new HashMap<>();
        for (IndexingCommand cmd : cmds) {
            if (cmd.isSync()) {
                List<IndexingCommand> syncCmds = syncCommands.get(cmd.getRepositoryName());
                if (syncCmds == null) {
                    syncCmds = new ArrayList<>();
                }
                syncCmds.add(cmd);
                syncCommands.put(cmd.getRepositoryName(), syncCmds);
            } else {
                List<IndexingCommand> asyncCmds = asyncCommands.get(cmd.getRepositoryName());
                if (asyncCmds == null) {
                    asyncCmds = new ArrayList<>();
                }
                asyncCmds.add(cmd);
                asyncCommands.put(cmd.getRepositoryName(), asyncCmds);
            }
        }
        this.runIndexingSyncWorker(syncCommands);
        this.scheduleIndexingAsyncWorker(asyncCommands);
    }

    protected void scheduleIndexingAsyncWorker(Map<String, List<IndexingCommand>> asyncCommands) {
        if (asyncCommands.isEmpty()) {
            return;
        }
        WorkManager wm = Framework.getLocalService(WorkManager.class);
        for (String repositoryName : asyncCommands.keySet()) {
            IndexingWorker idxWork = new IndexingWorker(repositoryName, asyncCommands.get(repositoryName));
            // we are in afterCompletion don't wait for a commit
            wm.schedule(idxWork, false);
        }
    }

    protected void runIndexingSyncWorker(Map<String, List<IndexingCommand>> syncCommands) {
        if (syncCommands.isEmpty()) {
            return;
        }
        Transaction transaction = TransactionHelper.suspendTransaction();
        try {
            for (String repositoryName : syncCommands.keySet()) {
                IndexingWorker idxWork = new IndexingWorker(repositoryName, syncCommands.get(repositoryName));
                idxWork.run();
            }
        } finally {
            if (transaction != null) {
                TransactionHelper.resumeTransaction(transaction);
            }

        }
    }

    // @Override
    // public void runReindexingWorker(String repositoryName, String nxql) {
    // if ((nxql == null) || nxql.isEmpty()) {
    // throw new IllegalArgumentException("Expecting an NXQL query");
    // }
    // ScrollingIndexingWorker worker = new ScrollingIndexingWorker(repositoryName, nxql);
    // WorkManager wm = Framework.getLocalService(WorkManager.class);
    // wm.schedule(worker);
    // }

    /* ========== 'FORK' for zero down time Es re-indexing ========== */

    @Override
    public void runReindexingWorker(String repositoryName, String nxql) {
        runReindexingWorker(repositoryName, nxql, false);
    }

    public void runReindexingWorker(String repositoryName, String nxql, boolean zeroDownTime) {
        ScrollingIndexingWorker worker = null;
        WorkManager wm = Framework.getLocalService(WorkManager.class);

        if ((nxql == null) || nxql.isEmpty()) {
            throw new IllegalArgumentException("Expecting an NXQL query");
        }
        // To use different queue
        if (!zeroDownTime) {
            worker = new ScrollingIndexingWorker(repositoryName, nxql);
        } else {
            worker = new ScrollingReIndexingWorker(repositoryName, nxql);

        }

        wm.schedule(worker);
    }


    @Override
    public boolean reIndexAllDocumentsWithZeroDownTime(String repository) throws ReIndexingStatusException, ReIndexingStateException, ReIndexingException {
        return this.esi.reIndexAllDocumentsWithZeroDownTime(repository);
    }

    @Override
    public boolean isZeroDownTimeReIndexingInProgress(String repository) throws InterruptedException {
        return this.esa.isZeroDownTimeReIndexingInProgress(repository);
    }

    // FIXME: to remove when new service exposition will be ok (Framework.getService)
    public OttcElasticSearchServiceImpl getEsService() {
        return this.ess;
    }

    @Override
    public String getConfiguredIndexOrAliasNameForRepository(String repositoryName) {
        return this.esa.getConfiguredIndexOrAliasNameForRepository(repositoryName);
    }

    // ES Search ===============================================================
    @Override
    public DocumentModelList query(NxQueryBuilder queryBuilder) throws ClientException {
        return this.ess.query(queryBuilder);
    }

    @Override
    public EsResult queryAndAggregate(NxQueryBuilder queryBuilder) throws ClientException {
        return this.ess.queryAndAggregate(queryBuilder);
    }

    @Deprecated
    @Override
    public DocumentModelList query(CoreSession session, String nxql, int limit, int offset, SortInfo... sortInfos) throws ClientException {
        NxQueryBuilder query = new NxQueryBuilder(session).nxql(nxql).limit(limit).offset(offset).addSort(sortInfos);
        return this.query(query);
    }

    @Deprecated
    @Override
    public DocumentModelList query(CoreSession session, QueryBuilder queryBuilder, int limit, int offset, SortInfo... sortInfos) throws ClientException {
        NxQueryBuilder query = new NxQueryBuilder(session).esQuery(queryBuilder).limit(limit).offset(offset).addSort(sortInfos);
        return this.query(query);
    }

    // misc ====================================================================
    private boolean isReady() {
        return (this.esa != null) && this.esa.isReady();
    }

}
