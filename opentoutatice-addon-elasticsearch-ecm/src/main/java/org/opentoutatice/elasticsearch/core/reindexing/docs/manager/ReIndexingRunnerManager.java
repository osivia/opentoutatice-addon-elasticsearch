/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.manager;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.core.Response.Status.Family;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.helper.Validate;
import org.nuxeo.ecm.core.work.api.Work.State;
import org.nuxeo.ecm.core.work.WorkManagerImpl;
import org.nuxeo.ecm.core.work.api.Work;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.ecm.core.work.api.WorkManager.Scheduling;
import org.nuxeo.elasticsearch.api.ElasticSearchAdmin;
import org.nuxeo.elasticsearch.api.ElasticSearchIndexing;
import org.nuxeo.runtime.api.Framework;
import org.opentoutatice.elasticsearch.OttcElasticSearchComponent;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.config.OttcElasticSearchIndexOrAliasConfig;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsState;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsStateChecker;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception.ReIndexingStateException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception.ReIndexingStatusException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.index.IndexName;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.runner.ReIndexingWork;
import org.opentoutatice.elasticsearch.core.reindexing.docs.runner.step.ReIndexingRunnerStep;
import org.opentoutatice.elasticsearch.core.service.OttcElasticSearchAdminImpl;

/**
 * Singleton launching re-indexing works (one work by repository at most).
 *
 * @author dchevrier
 *
 */
public class ReIndexingRunnerManager {

    private static final Log log = LogFactory.getLog(ReIndexingRunnerManager.class);

    private static final String DOC_TYPE = "doc";

    private IndexNAliasManager indexManager;

    private WorkManager workManager;

    private OttcElasticSearchAdminImpl esAdmin;
    private OttcElasticSearchIndexing esIndexing;

    private String currentWorkId;

    // For logs infos
    private Map<String, ReIndexingRunnerStep> runnerStepByWork = new HashMap<String, ReIndexingRunnerStep>(1);
    private Map<String, Long> startTimeByWork = new HashMap<String, Long>(1);
    private Map<String, Long> endTimeByWork = new HashMap<String, Long>(1);
    private Map<String, IndexName> newIndexByWork = new HashMap<String, IndexName>(1);
    private Map<String, Long> initialNbDocsInBdd = new HashMap<String, Long>(1);

    private static ReIndexingRunnerManager instance;

    private ReIndexingRunnerManager() {
        super();

        this.setIndexManager(IndexNAliasManager.get());

        WorkManager workManager = Framework.getService(WorkManager.class);
        Validate.notNull(workManager);
        this.setWorkManager(workManager);

        OttcElasticSearchAdminImpl esAdmin = ((OttcElasticSearchComponent) Framework.getService(ElasticSearchAdmin.class)).getElasticSearchAdmin();
        Validate.notNull(esAdmin);
        this.setEsAdmin(esAdmin);

        OttcElasticSearchIndexing esIndexing = (OttcElasticSearchIndexing) Framework.getService(ElasticSearchIndexing.class);
        Validate.notNull(esAdmin);
        this.setEsIndexing(esIndexing);
    }

    public static synchronized ReIndexingRunnerManager get() {
        if (instance == null) {
            instance = new ReIndexingRunnerManager();
        }
        return instance;
    }
    
    // For tests only
    public static void reset() {
        instance = null;
    }

    /**
     * Launches Es re-indexing with zero down time on given repository.
     *
     * @param repository @return ReIndexingSteps: state and status of re-indexing
     *            step in progress when calling method @throws
     */
    public boolean reIndexWithZeroDownTime(String repository) throws ReIndexingStatusException, ReIndexingStateException, ReIndexingException {
        // Launched status
        boolean launchedStatus = false;

        synchronized (this) {
            OttcElasticSearchIndexOrAliasConfig aliasConfig = this.checkConfig(repository);

            if (this.isEsInInitialAllowedState(aliasConfig)) {
                // Launch re-indexing
                this.launchReIndexingRunner(aliasConfig);
                
                launchedStatus = true;
            }
        }

        return launchedStatus;
    }

    private OttcElasticSearchIndexOrAliasConfig checkConfig(String repository) throws ReIndexingException {
        // Find Es configuration associated with repository
        OttcElasticSearchIndexOrAliasConfig aliasCfg = null;
        try {
            String indexOrAliasName = this.getEsAdmin().getConfiguredIndexOrAliasNameForRepository(repository);
            aliasCfg = (OttcElasticSearchIndexOrAliasConfig) this.getEsAdmin().getIndexConfig().get(indexOrAliasName);
        } catch (Exception e) {
            throw new ReIndexingException(e);
        }

        if (aliasCfg == null) {
            throw new ReIndexingException(String.format("No Elasticsearch configuration for [%s] repository.", repository));
        } else if (!DOC_TYPE.equals(aliasCfg.getType())) {
            throw new ReIndexingException(String.format("Elasticsearch configuration for [%s] repository is of [%s] type: configuration must be of 'doc' type.",
                    repository, aliasCfg.getType()));
        }

        return aliasCfg;
    }

    private boolean isEsInInitialAllowedState(OttcElasticSearchIndexOrAliasConfig aliasConfig)
            throws ReIndexingStatusException, ReIndexingStateException, ReIndexingException {
        boolean allowedState = false;

        // Re-indexing process yet running
        allowedState = !this.isReIndexingRunnerInProgressOn(aliasConfig.getRepositoryName());

        if (!allowedState) {
            throw new ReIndexingStatusException(String.format(
                    "Re-indexing process is yet running for repository [%s]: you can not launch other re-indexing processes on a repository while other is running",
                    aliasConfig.getRepositoryName()));
        }

        // Allowed initail state:
        // - only one alias xxx-alias on only one index
        // - zero or one former alias former-xxx-alias; if one, must points on index
        // different of xxx-alias's one
        try {
            allowedState &= EsStateChecker.get().aliasExistsWithOnlyOneIndex(aliasConfig.getAliasName()) && EsStateChecker.get().transientAliasesNotExist()
                    && EsStateChecker.get().mayFormerAliasExists(aliasConfig.getAliasName());
        } catch (ReIndexingStateException se) {
            ReIndexingStateException rse = new ReIndexingStateException(
                    String.format("You can not launch a re-indexing process on [%s] repository: ", aliasConfig.getRepositoryName()));
            rse.initCause(se);
            throw rse;
        }

        return allowedState;
    }

    // Just to catch and build ReIndexing Exception
    private boolean isReIndexingRunnerInProgressOn(String repository) throws ReIndexingException {
        boolean isInProgress = false;

        try {
            isInProgress = this.isReIndexingInProgress(repository);
        } catch (Exception e) {
            throw new ReIndexingException(e);
        }

        return isInProgress;
    }

    protected void launchReIndexingRunner(OttcElasticSearchIndexOrAliasConfig aliasCfg) throws ReIndexingStateException {
        // Get initial Es State for possible recovery
        EsState initialEsState = null;
        try {
            initialEsState = EsStateChecker.get().getEsState();
        } catch (InterruptedException | ExecutionException e) {
            throw new ReIndexingStateException(e);
        }

        ReIndexingWork reIndexingWork = new ReIndexingWork(aliasCfg, this.getEsAdmin(), this.getEsIndexing(), initialEsState);
        this.setCurrentWorkId(reIndexingWork.getId());
        this.getWorkManager().schedule(reIndexingWork);
    }

    // FIXME: to keep signature with repo
    public boolean isReIndexingInProgress(String repositoryName) throws InterruptedException {
        return this.isReIndexingInProgress();
    }

    public boolean isReIndexingInProgress() throws InterruptedException {
        boolean inProgress = false;

        State workState = this.getWorkManager().getWorkState(this.getCurrentWorkId());
        if (workState != null) {
            inProgress = State.SCHEDULED.equals(workState) || State.RUNNING.equals(workState);
        }
        
        if (log.isTraceEnabled()) {
            log.trace(String.format("Zero down time re-indexing in progress: [%s]", String.valueOf(inProgress)));
        }

        return inProgress;
    }

    public void cleanLogsInfos() {
        this.runnerStepByWork.clear();
        this.startTimeByWork.clear();
        this.endTimeByWork.clear();
        this.newIndexByWork.clear();
        this.initialNbDocsInBdd.clear();
    }

    // Getters & Setters =====================

    public IndexNAliasManager getIndexManager() {
        return this.indexManager;
    }

    private void setIndexManager(IndexNAliasManager indexManager) {
        this.indexManager = indexManager;
    }

    public WorkManager getWorkManager() {
        return this.workManager;
    }

    private void setWorkManager(WorkManager workManager) {
        this.workManager = workManager;
    }

    public OttcElasticSearchAdminImpl getEsAdmin() {
        return this.esAdmin;
    }

    public void setEsAdmin(OttcElasticSearchAdminImpl esAdmin) {
        this.esAdmin = esAdmin;
    }

    public OttcElasticSearchIndexing getEsIndexing() {
        return this.esIndexing;
    }

    private void setEsIndexing(OttcElasticSearchIndexing esIndexing) {
        this.esIndexing = esIndexing;
    }

    public String getCurrentWorkId() {
        return this.currentWorkId;
    }

    private void setCurrentWorkId(String currentWorkId) {
        this.currentWorkId = currentWorkId;
    }

    public ReIndexingRunnerStep getRunnerStepFor(String work) {
        return this.runnerStepByWork.get(work);
    }

    public void setRunnerStepFor(String work, ReIndexingRunnerStep step) {
        this.runnerStepByWork.put(work, step);
    }

    public long getStartTimeFor(String work) {
        return this.startTimeByWork.get(work);
    }

    public void setStartTimeFor(String work) {
        this.startTimeByWork.put(work, System.currentTimeMillis());
    }

    public long getEndTimeFor(String work) {
        return this.endTimeByWork.get(work);
    }

    public void setEndTimeFor(String work) {
        this.endTimeByWork.put(work, System.currentTimeMillis());
    }

    public IndexName getNewIndexFor(String work) {
        return this.newIndexByWork.get(work);
    }

    public void setNewIndexFor(String work, IndexName index) {
        this.newIndexByWork.put(work, index);
    }

    public Long getInitialNbDocsInBddFor(String work) {
        return this.initialNbDocsInBdd.get(work);
    }

    public void setInitialNbDocsInBddFor(String work, Long nbDocs) {
        this.initialNbDocsInBdd.put(work, nbDocs);
    }

}
