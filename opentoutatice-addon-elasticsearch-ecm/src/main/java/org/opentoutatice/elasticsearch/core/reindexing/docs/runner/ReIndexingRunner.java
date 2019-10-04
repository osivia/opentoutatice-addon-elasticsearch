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
import org.nuxeo.elasticsearch.ElasticSearchConstants;
import org.nuxeo.runtime.api.Framework;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.config.OttcElasticSearchIndexOrAliasConfig;
import org.opentoutatice.elasticsearch.core.reindexing.docs.constant.ReIndexingConstants;
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

    private ReIndexingConfig reIndexingConfig;

    private ReIndexingRunnerStep runnerStep;

    private OttcElasticSearchAdminImpl esAdmin;

    private OttcElasticSearchIndexing esIndexing;

    private static final String REINDEX_REPOSITORY_QUERY = "select ecm:uuid from Document";

    private static final DecimalFormat decimalFormat = new DecimalFormat("##.###");

    public ReIndexingRunner(OttcElasticSearchIndexOrAliasConfig nxAliasCfg, OttcElasticSearchAdminImpl esAdmin, OttcElasticSearchIndexing esIndexing) {
        super();
        this.setReIndexingConfig(new ReIndexingConfig(nxAliasCfg));
        this.esAdmin(esAdmin).esIndexing(esIndexing);
    }

    public void run() throws ReIndexingException {

        final String repository = this.getRepository();

        try {
            ReIndexingRunnerManager.get().setStartTimeFor(repository);
            this.setRunnerStep(ReIndexingRunnerStep.initialization);

            final OttcElasticSearchIndexOrAliasConfig nxAliasCfg = this.getNxAliasCfg(repository);
            final IndexName newIndex = this.getNewIndex(repository);
            final IndexName initialIndex = this.getInitialIndex(repository);

            OttcElasticSearchIndexOrAliasConfig newNxAliasCfg = null;

            try {
                this.setRunnerStep(ReIndexingRunnerStep.initialization
                        .stepState(ReIndexingRunnerStepState.started.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));

                newNxAliasCfg = this.createNewEsIndex(newIndex, nxAliasCfg);
                this.getEsAdmin().initIndex(newNxAliasCfg, false);

                this.createNSwitchOnTransientAliases(initialIndex, newIndex);

                // For test only
                if (Framework.isTestModeSet()) {
                    this.mayFireExceptionInTestMode(this.getRunnerStep());
                }

                this.setRunnerStep(
                        ReIndexingRunnerStep.initialization.stepState(ReIndexingRunnerStepState.done.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));
            } catch (Exception e) {
                this.manageReIndexingError(repository, initialIndex, newIndex, this.getRunnerStep(), e);
            }

            try {
                this.setRunnerStep(
                        ReIndexingRunnerStep.indexing.stepState(ReIndexingRunnerStepState.started.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));

                this.reIndex(repository, newIndex);

                // For test only
                if (Framework.isTestModeSet()) {
                    this.mayFireExceptionInTestMode(this.getRunnerStep());
                }

                this.setRunnerStep(
                        ReIndexingRunnerStep.indexing.stepState(ReIndexingRunnerStepState.done.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));
            } catch (Exception e) {
                this.manageReIndexingError(repository, initialIndex, newIndex, ReIndexingRunnerStep.indexing, e);
            }

            try {
                this.setRunnerStep(
                        ReIndexingRunnerStep.switching.stepState(ReIndexingRunnerStepState.started.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));

                this.updateEsAlias(nxAliasCfg.getAliasName(), initialIndex, newIndex);

                this.updateEsFormerAlias(nxAliasCfg.getAliasName(), initialIndex);
                this.deleteTransientAliases(initialIndex, newIndex);
                
                ReIndexingRunnerManager.get().setLastIndexFor(repository, newIndex);

                // For test only
                if (Framework.isTestModeSet()) {
                    this.mayFireExceptionInTestMode(this.getRunnerStep());
                }

                this.setRunnerStep(
                        ReIndexingRunnerStep.switching.stepState(ReIndexingRunnerStepState.done.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));
            } catch (Exception e) {
                this.manageReIndexingError(repository, initialIndex, newIndex, ReIndexingRunnerStep.switching, e);
            }

        } finally {
            ReIndexingRunnerManager.get().setEndTimeFor(repository);
        }

    }

    /**
     * @param e
     * @throws ReIndexingException
     */
    private void manageReIndexingError(String repository, IndexName initialIndex, IndexName newIndex, ReIndexingRunnerStep step, Exception e) throws ReIndexingException {
        // Exception to be thrown
        ReIndexingException reIndexingException = new ReIndexingException(String.format("[Re-indexing process INTERRUPTED during [%s] step]: ", step.name()),
                e);

        // Set step status
        step.getStepState().stepStatus(ReIndexingRunnerStepStateStatus.inError.error(reIndexingException));

        // Try recovering Es state (can throw RecoveringReIndexingException)
        try {
            ReIndexingErrorsHandler.get().restoreInitialEsState(repository, step, initialIndex, newIndex);
        } catch (RecoveringReIndexingException re) {
            // Set reindexing exception as cause for meaningfull stack trace
            Throwable cause = re.getCause();
            cause.initCause(reIndexingException);
            throw re;
        }

        throw reIndexingException;
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
            this.getEsIndexing().runReindexingWorker(repository, REINDEX_REPOSITORY_QUERY);

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
        long timeOut = 100;

        long loopWaitTime = Long.valueOf(Framework.getProperty(ReIndexingConstants.REINDEXING_WAIT_LOOP_TIME, "30")).longValue();
        TimeUnit unit = TimeUnit.SECONDS;

        long startTime = System.currentTimeMillis();
        if (log.isInfoEnabled()) {
            log.info(String.format("Starting waiting for re-indexing every [%s %s]", String.valueOf(loopWaitTime),
                    StringUtils.lowerCase(String.valueOf(unit.toString()))));
        }

        WorkManager workManager = Framework.getService(WorkManager.class);
        boolean awaitCompletion = workManager.awaitCompletion(ElasticSearchConstants.INDEXING_QUEUE_ID, timeOut, unit);

        final String waitIndicatorForLogs = "...";
        while (!awaitCompletion) {

            // s -> ms
            Thread.sleep(loopWaitTime * 1000);

            awaitCompletion = workManager.awaitCompletion(ElasticSearchConstants.INDEXING_QUEUE_ID, timeOut, unit);

            if (log.isTraceEnabled()) {
                log.trace(String.format("Await completed: [%s]", String.valueOf(awaitCompletion)));
            }

            if (log.isInfoEnabled()) {
                log.info(waitIndicatorForLogs);
            }
        }

        Validate.isTrue(awaitCompletion);

        // Status
        this.setRunnerStep(ReIndexingRunnerStep.indexing.stepState(ReIndexingRunnerStepState.done.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));

        if (log.isInfoEnabled()) {
            float duration = (float) (System.currentTimeMillis() - startTime) / 1000;

            log.info(String.format("End waiting: re-indexing done in [%s] %s", decimalFormat.format(duration),
                    StringUtils.lowerCase(String.valueOf(unit.toString()))));
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
        ReIndexingRunnerManager.get().setRunnerStepFor(this.getRepository(), runnerStep);
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

    // Only for tests
    private void mayFireExceptionInTestMode(ReIndexingRunnerStep step) throws Exception {
        if (StringUtils.equals(step.name(), Framework.getProperty(ReIndexingTestConstants.FIRE_TEST_ERRORS_ON_STEP_PROP))) {
            throw new Exception(String.format("[ERROR TEST] during: %s", step.name()));
        }
    }

}
