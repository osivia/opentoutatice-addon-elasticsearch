/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.runner;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.runtime.api.Framework;
import org.opentoutatice.elasticsearch.core.reindexing.docs.index.IndexName;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.IndexNAliasManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.RecoveringReIndexingException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.runner.step.ReIndexingRunnerStep;
import org.opentoutatice.elasticsearch.core.reindexing.docs.test.constant.ReIndexingTestConstants;

/**
 * @author david
 *
 */
public class ReIndexingErrorsHandler {

    private static final Log log = LogFactory.getLog(ReIndexingErrorsHandler.class);

    private static ReIndexingErrorsHandler instance;

    private ReIndexingErrorsHandler() {
    };

    public static synchronized ReIndexingErrorsHandler get() {
        if (instance == null) {
            instance = new ReIndexingErrorsHandler();
        }
        return instance;
    }

    public void restoreInitialEsState(String repository, ReIndexingRunnerStep indexingStep, Object... params) throws RecoveringReIndexingException {

        try {
            switch (indexingStep) {
                case initialization:
                    if (log.isDebugEnabled()) {
                        log.debug("[Es State RECOVERY] About to delete transient aliases and new index...");
                    }

                    // For test only
                    if (Framework.isTestModeSet()) {
                        this.mayFireExceptionInTestMode(ReIndexingRunnerStep.initialization);
                    }

                    if (IndexNAliasManager.get().transientAliasesExist()) {
                        IndexNAliasManager.get().deleteTransientAliases((IndexName) params[0], (IndexName) params[1]);
                    }
                    // Delete new index
                    if (IndexNAliasManager.get().indexExists(((IndexName) params[1]).toString())) {
                        IndexNAliasManager.get().deleteIndex(((IndexName) params[1]).toString());
                    }

                    if (log.isInfoEnabled()) {
                        log.info("[Es State RECOVERY] [Transient aliases and new index deleted] ");
                    }
                    break;
                case indexing:
                case switching:
                    if (log.isDebugEnabled()) {
                        log.debug("[Es State RECOVERY] About to delete transient aliases...");
                    }
                    
                    ReIndexingRunnerManager.get().setLastIndexFor(repository, (IndexName) params[1]);

                    // For test only
                    if (Framework.isTestModeSet()) {
                        this.mayFireExceptionInTestMode(ReIndexingRunnerStep.indexing);
                    }

                    if (IndexNAliasManager.get().transientAliasesExist()) {
                        IndexNAliasManager.get().deleteTransientAliases((IndexName) params[0], (IndexName) params[1]);
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("[Es State RECOVERY] [Transient aliases deleted] ");
                    }
                    break;

                default:
                    break;
            }
        } catch (Exception e) {
            throw new RecoveringReIndexingException("[Es State Recovery FAILED]: ", e);
        }

    }

    // Only for tests
    private void mayFireExceptionInTestMode(ReIndexingRunnerStep step) throws Exception {
        if (StringUtils.equals(step.name(), Framework.getProperty(ReIndexingTestConstants.FIRE_TEST_ERRORS_ON_STEP_PROP))
                && StringUtils.equals(step.name(), Framework.getProperty(ReIndexingTestConstants.FIRE_TEST_ERRORS_ON_STEP_RECOVERY_PROP))) {
            throw new Exception(String.format("[RECOVERY ERROR TEST] during: %s", step.name()));
        }
    }

}
