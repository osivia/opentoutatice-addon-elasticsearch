/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.runner;

import java.text.DecimalFormat;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.runtime.api.Framework;
import org.opentoutatice.elasticsearch.OttcElasticSearchComponent;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.config.OttcElasticSearchIndexOrAliasConfig;
import org.opentoutatice.elasticsearch.core.reindexing.docs.constant.ReIndexingConstants;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsState;
import org.opentoutatice.elasticsearch.core.reindexing.docs.exception.IndexException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.index.IndexName;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.IndexNAliasManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.cfg.ReIndexingConfig;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.RecoveringReIndexingException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.runner.step.ReIndexingRunnerStep;
import org.opentoutatice.elasticsearch.core.reindexing.docs.runner.step.ReIndexingRunnerStepState;
import org.opentoutatice.elasticsearch.core.reindexing.docs.runner.step.ReIndexingRunnerStepStateStatus;
import org.opentoutatice.elasticsearch.core.reindexing.docs.test.constant.ReIndexingTestConstants;
import org.opentoutatice.elasticsearch.core.service.OttcElasticSearchAdminImpl;

/**
 * @author david
 *
 */
public class ReIndexingRunner {

    private static final Log log = LogFactory.getLog(ReIndexingRunner.class);

    private String workId;

    private ReIndexingConfig reIndexingConfig;

    private ReIndexingRunnerStep runnerStep;

    private OttcElasticSearchAdminImpl esAdmin;

    private OttcElasticSearchIndexing esIndexing;

    private EsState initialEsState;

    private static final String REINDEX_REPOSITORY_QUERY = "select ecm:uuid from Document";

    private static final DecimalFormat decimalFormat = new DecimalFormat("##.###");

    public ReIndexingRunner(String id, OttcElasticSearchIndexOrAliasConfig nxAliasCfg, OttcElasticSearchAdminImpl esAdmin, OttcElasticSearchIndexing esIndexing,
            EsState initialEsState) {
        super();
        this.workId = id;
        this.setReIndexingConfig(new ReIndexingConfig(nxAliasCfg));
        this.esAdmin(esAdmin).esIndexing(esIndexing);
        this.setInitialEsState(initialEsState);
    }

    public void run() throws ReIndexingException {

        final String repository = this.getRepository();

        try {
            ReIndexingRunnerManager.get().setStartTimeFor(this.getWorkId());
            this.setRunnerStep(ReIndexingRunnerStep.initialization);

            final OttcElasticSearchIndexOrAliasConfig nxAliasCfg = this.getNxAliasCfg(repository);
            final IndexName newIndex = this.getNewIndex(repository);
            final IndexName initialIndex = this.getInitialIndex(repository);

            OttcElasticSearchIndexOrAliasConfig newNxAliasCfg = null;

            try {
                this.setRunnerStep(ReIndexingRunnerStep.initialization
                        .stepState(ReIndexingRunnerStepState.started.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));

                // Check interrupt status
                this.checkInterrupt();

                newNxAliasCfg = this.createNewEsIndex(newIndex, nxAliasCfg);
                this.getEsAdmin().initIndexIf(newNxAliasCfg.getName(), newNxAliasCfg.getType(), newNxAliasCfg.getSettings(), false);

                this.createNSwitchOnTransientAliases(initialIndex, newIndex);

                // For test only
                if (Framework.isTestModeSet()) {
                    this.mayFireExceptionInTestMode(this.getRunnerStep());
                }

                this.setRunnerStep(
                        ReIndexingRunnerStep.initialization.stepState(ReIndexingRunnerStepState.done.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));
            } catch (Exception e) {
                this.manageReIndexingError(this.getWorkId(), nxAliasCfg.getAliasName(), initialIndex, newIndex, this.getRunnerStep(), this.getInitialEsState(),
                        e);
            }

            try {
                this.setRunnerStep(
                        ReIndexingRunnerStep.indexing.stepState(ReIndexingRunnerStepState.started.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));

                // Check interrupt status
                this.checkInterrupt();

                this.reIndex(repository, newIndex);

                // For test only
                if (Framework.isTestModeSet()) {
                    this.mayFireExceptionInTestMode(this.getRunnerStep());
                }

                this.setRunnerStep(
                        ReIndexingRunnerStep.indexing.stepState(ReIndexingRunnerStepState.done.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));
            } catch (Exception e) {
                this.manageReIndexingError(this.getWorkId(), nxAliasCfg.getAliasName(), initialIndex, newIndex, this.getRunnerStep(), this.getInitialEsState(),
                        e);
            }

            try {
                this.setRunnerStep(
                        ReIndexingRunnerStep.switching.stepState(ReIndexingRunnerStepState.started.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));

                // Check interrupt status
                this.checkInterrupt();

                this.updateEsAlias(nxAliasCfg.getAliasName(), initialIndex, newIndex);

                this.updateEsFormerAlias(nxAliasCfg.getAliasName(), initialIndex);
                this.deleteTransientAliases(initialIndex, newIndex);

                ReIndexingRunnerManager.get().setNewIndexFor(this.getWorkId(), newIndex);

                // For test only
                if (Framework.isTestModeSet()) {
                    this.mayFireExceptionInTestMode(this.getRunnerStep());
                }

                // Check interrupt status
                this.checkInterrupt();

                this.setRunnerStep(
                        ReIndexingRunnerStep.switching.stepState(ReIndexingRunnerStepState.done.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));
            } catch (Exception e) {
                this.manageReIndexingError(this.getWorkId(), nxAliasCfg.getAliasName(), initialIndex, newIndex, this.getRunnerStep(), this.getInitialEsState(),
                        e);
            }

        } finally {
            ReIndexingRunnerManager.get().setEndTimeFor(this.getWorkId());
        }

    }

    /**
     * @param e
     * @throws ReIndexingException
     */
    private void manageReIndexingError(String workId, String currentAlias, IndexName initialIndex, IndexName newIndex, ReIndexingRunnerStep step,
            EsState initialEsState, Exception e) throws ReIndexingException {
        // Exception to be thrown
        ReIndexingException reIndexingException = new ReIndexingException(String.format("[Re-indexing process INTERRUPTED during [%s] step]: ", step.name()),
                e);

        // Set step status
        step.getStepState().stepStatus(ReIndexingRunnerStepStateStatus.inError.error(reIndexingException));

        // Try recovering Es state (can throw RecoveringReIndexingException)
        try {
            ReIndexingErrorsHandler.get().restoreInitialEsState(workId, step, initialEsState, initialIndex, newIndex, currentAlias);
        } catch (RecoveringReIndexingException re) {
            // Set reindexing exception as cause for meaningfull stack trace
            Throwable cause = re.getCause();
            cause.initCause(reIndexingException);
            throw re;
        }

        throw reIndexingException;
    }

    /**
     * Checks if current Threaad is interrupted.
     *
     * @throws InterruptedException
     */
    private void checkInterrupt() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException(String.format("Zero Down Time work [%s] INTERUPTED.", this.getWorkId()));
        }
    }

    // Initialization phase ============================

    protected OttcElasticSearchIndexOrAliasConfig createNewEsIndex(IndexName newIndexName, OttcElasticSearchIndexOrAliasConfig nxIndexNAliasCfg)
            throws ReIndexingException {
        try {
            return IndexNAliasManager.get().createNewIndex(newIndexName, nxIndexNAliasCfg);
        } catch (IndexException | InterruptedException | ExecutionException e) {
            throw new ReIndexingException(e);
        }
    }

    protected void createNSwitchOnTransientAliases(IndexName initialIndex, IndexName newIndex) throws ReIndexingException {
        IndexNAliasManager.get().createTransientAliases(initialIndex, newIndex);
    }

    // Re-indexing phase ============================

    protected void reIndex(String repository, IndexName newIndex) throws ReIndexingException {
        try {
            if (log.isDebugEnabled()) {
                log.debug("About to launch Re-indexing...");
            }

            // Launch asynchronous indexing
            // FIXME: are async exceptions throwned in calling thread??
            ((OttcElasticSearchComponent) this.getEsIndexing()).runReindexingWorker(repository, REINDEX_REPOSITORY_QUERY, true);

            if (log.isInfoEnabled()) {
                log.info("Re-indexing launched.");
            }

            // Status
            this.setRunnerStep(
                    ReIndexingRunnerStep.indexing.stepState(ReIndexingRunnerStepState.inProgress.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));

            // Wait for asynchronous indexing
            this.waitReIndexing();

            // Refresh Es caches
            if (log.isInfoEnabled()) {
                log.info(String.format("Refreshing new index [%s]", newIndex.toString()));
            }
            this.getEsAdmin().refreshRepositoryIndex(repository);

        } catch (RuntimeException | InterruptedException e) {
            throw new ReIndexingException(e);
        }
    }

    protected void waitReIndexing() throws InterruptedException {
        // await timeout in ms
        final long timeOut = 100;
        final long loopWaitTime = Long.valueOf(Framework.getProperty(ReIndexingConstants.REINDEXING_WAIT_LOOP_TIME, "30")).longValue();

        long startTime = System.currentTimeMillis();
        if (log.isInfoEnabled()) {
            log.info(String.format("Starting waiting for re-indexing every [%s %s]", String.valueOf(loopWaitTime),
                    StringUtils.lowerCase(String.valueOf(TimeUnit.SECONDS.toString()))));
        }

        WorkManager workManager = Framework.getService(WorkManager.class);

        if (log.isInfoEnabled()) {
            log.info("...");
        }
        boolean awaitCompletion = false;

        do {
            // s -> ms
            Thread.sleep(loopWaitTime * 1000);


            if (log.isTraceEnabled()) {
                log.trace(String.format("Await completed: [%s]", String.valueOf(awaitCompletion)));
            }

        } while (!(awaitCompletion = workManager.awaitCompletion(ReIndexingConstants.REINDEXING_QUEUE_ID, timeOut, TimeUnit.MILLISECONDS)));

        Validate.isTrue(awaitCompletion);

        // Status
        this.setRunnerStep(ReIndexingRunnerStep.indexing.stepState(ReIndexingRunnerStepState.done.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));

        if (log.isInfoEnabled()) {
            float duration = (float) (System.currentTimeMillis() - startTime) / 1000;

            log.info(String.format("End waiting: re-indexing done in [%s] s", decimalFormat.format(duration)));
        }
    }

    // Switching phase ===========================

    private void updateEsAlias(String aliasName, IndexName initialIndex, IndexName newIndex) throws ReIndexingException {
        IndexNAliasManager.get().updateEsAlias(aliasName, initialIndex, newIndex);
    }

    private void updateEsFormerAlias(String aliasName, IndexName initialIndex) throws ReIndexingException {
        IndexNAliasManager.get().updateEsFormerAlias(aliasName, initialIndex);
    }

    private void deleteTransientAliases(IndexName initialIndex, IndexName newIndex) throws ReIndexingException {
        IndexNAliasManager.get().deleteTransientAliases(initialIndex, newIndex);
    }

    // Utility =======================
    private OttcElasticSearchIndexOrAliasConfig getNxAliasCfg(String repository) {
        return this.getReIndexingConfig().getNxAliasCfg();
    }

    private IndexName getNewIndex(String repository) {
        return this.getReIndexingConfig().getNewIndex();
    }

    private IndexName getInitialIndex(String repository) {
        return this.getReIndexingConfig().getInitialIndex();
    }

    // Getters and Setters ========================

    private String getRepository() {
        Validate.notNull(this.getReIndexingConfig());
        Validate.notNull(this.getReIndexingConfig().getNxAliasCfg());
        return this.getReIndexingConfig().getNxAliasCfg().getRepositoryName();
    }

    public String getWorkId() {
        return this.workId;
    }

    public void setWorkId(String workId) {
        this.workId = workId;
    }

    public ReIndexingConfig getReIndexingConfig() {
        return this.reIndexingConfig;
    }

    private void setReIndexingConfig(ReIndexingConfig reIndexingCfg) {
        this.reIndexingConfig = reIndexingCfg;
    }

    public ReIndexingRunnerStep getRunnerStep() {
        return this.runnerStep;
    }

    private void setRunnerStep(ReIndexingRunnerStep runnerStep) {
        this.runnerStep = runnerStep;
        ReIndexingRunnerManager.get().setRunnerStepFor(this.getWorkId(), runnerStep);
    }

    public OttcElasticSearchAdminImpl getEsAdmin() {
        return this.esAdmin;
    }

    public ReIndexingRunner esAdmin(OttcElasticSearchAdminImpl esAdmin) {
        Validate.notNull(esAdmin);
        this.esAdmin = esAdmin;
        return this;
    }

    public OttcElasticSearchIndexing getEsIndexing() {
        return this.esIndexing;
    }

    public ReIndexingRunner esIndexing(OttcElasticSearchIndexing esIndexing) {
        Validate.notNull(esIndexing);
        this.esIndexing = esIndexing;
        return this;
    }

    public EsState getInitialEsState() {
        return this.initialEsState;
    }

    private void setInitialEsState(EsState initialEsState) {
        this.initialEsState = initialEsState;
    }

    // Only for tests
    private void mayFireExceptionInTestMode(ReIndexingRunnerStep step) throws Exception {
        if (StringUtils.equals(step.name(), Framework.getProperty(ReIndexingTestConstants.FIRE_TEST_ERRORS_ON_STEP_PROP))) {
            throw new Exception(String.format("[ERROR TEST] during: %s", step.name()));
        }
    }

}
