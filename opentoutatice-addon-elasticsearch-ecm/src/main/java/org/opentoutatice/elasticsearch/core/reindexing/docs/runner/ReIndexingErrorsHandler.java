/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.runner;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.IndicesAdminClient;
import org.nuxeo.runtime.api.Framework;
import org.opentoutatice.elasticsearch.config.OttcElasticSearchIndexOrAliasConfig;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsState;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsStateChecker;
import org.opentoutatice.elasticsearch.core.reindexing.docs.index.IndexName;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.IndexNAliasManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.RecoveringReIndexingException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.runner.step.ReIndexingRunnerStep;
import org.opentoutatice.elasticsearch.core.reindexing.docs.test.constant.ReIndexingTestConstants;
import org.opentoutatice.elasticsearch.utils.MessageUtils;

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

    public void restoreInitialEsState(String workId, ReIndexingRunnerStep indexingStep, EsState initialEsState, Object... params)
            throws RecoveringReIndexingException {

        try {
            if (log.isDebugEnabled()) {
                log.debug("[Es State RECOVERY] About to restore initial Es State...");
            }

            switch (indexingStep) {
                case initialization:
                    // For test only
                    if (Framework.isTestModeSet()) {
                        this.mayFireExceptionInTestMode(ReIndexingRunnerStep.initialization);
                    }

                    this.restoreInitialEsState(initialEsState, (IndexName) params[0], (IndexName) params[1]);

                    // Delete new index
                    if (IndexNAliasManager.get().indexExists(((IndexName) params[1]).toString())) {
                        IndexNAliasManager.get().deleteIndex(((IndexName) params[1]).toString());
                    }
                    break;

                case indexing:
                    ReIndexingRunnerManager.get().setNewIndexFor(workId, (IndexName) params[1]);

                    // For test only
                    if (Framework.isTestModeSet()) {
                        this.mayFireExceptionInTestMode(ReIndexingRunnerStep.indexing);
                    }

                    this.restoreInitialEsState(initialEsState, (IndexName) params[0], (IndexName) params[1]);
                    break;

                case switching:
                    ReIndexingRunnerManager.get().setNewIndexFor(workId, (IndexName) params[1]);

                    // For test only
                    if (Framework.isTestModeSet()) {
                        this.mayFireExceptionInTestMode(ReIndexingRunnerStep.switching);
                    }

                    this.restoreInitialEsState(initialEsState, (IndexName) params[0], (IndexName) params[1]);
                    break;

                default:
                    break;
            }

            if (log.isInfoEnabled()) {
                log.info("[Es State RECOVERY] Initial Es State restored");
            }
        } catch (Exception e) {
            throw new RecoveringReIndexingException("[Es State Recovery FAILED]: ", e);
        }

    }

    protected void restoreInitialEsState(EsState initialEsState, IndexName initialIndex, IndexName newIndex)
            throws ReIndexingException, InterruptedException, ExecutionException {
        // try {
        // Delete transient aliases
        if (IndexNAliasManager.get().transientAliasesExist()) {
            IndexNAliasManager.get().deleteTransientAliases(initialIndex, newIndex);
        }

        // Current aliases
        Map<String, List<String>> currentAliases = EsStateChecker.get().getEsState().getAliases();
        // Initial aliases (we must have something to restore)
        Map<String, List<String>> initialAliases = initialEsState.getAliases();
        Validate.isTrue(initialAliases.keySet() != null ? initialAliases.keySet().size() >= 1 : false);

        IndicesAdminClient esClient = IndexNAliasManager.get().getAdminClient().indices();

        // Remove current Es state
        for (Entry<String, List<String>> currentAlias : currentAliases.entrySet()) {

            String currentAliasName = currentAlias.getKey();
            Validate.isTrue(StringUtils.endsWith(currentAliasName, OttcElasticSearchIndexOrAliasConfig.NX_ALIAS_SUFFIX));

            List<String> currentIndicesOfAlias = IndexNAliasManager.get().getIndicesOfAlias(currentAliasName);

            if (log.isInfoEnabled()) {
                log.info(String.format("Removing alias [%s] on indices: [%s]", currentAliasName, MessageUtils.listToString(currentIndicesOfAlias)));
            }

            String[] currentIndices = new String[currentIndicesOfAlias.size()];
            currentIndices = currentIndicesOfAlias.toArray(currentIndices);
            esClient.prepareAliases().removeAlias(currentIndices, currentAliasName).get();
        }

        // Restoring initial state
        for (Entry<String, List<String>> initialAlias : initialAliases.entrySet()) {

            String initialAliasName = initialAlias.getKey();
            Validate.isTrue(StringUtils.endsWith(initialAliasName, OttcElasticSearchIndexOrAliasConfig.NX_ALIAS_SUFFIX));

            List<String> initialIndicesOfAlias = initialAlias.getValue();

            if (log.isInfoEnabled()) {
                log.info(String.format("Restoring initial alias [%s] on indices: [%s]", initialAliasName, MessageUtils.listToString(initialIndicesOfAlias)));
            }
            String[] initialIndices = new String[initialAlias.getValue().size()];
            initialIndices = initialAlias.getValue().toArray(initialIndices);
            esClient.prepareAliases().addAlias(initialIndices, initialAliasName).get();
        }
        // } catch (Exception e) {
        // throw new RecoveringReIndexingException(e);
        // }
    }

    // Only for tests
    private void mayFireExceptionInTestMode(ReIndexingRunnerStep step) throws Exception {
        if (StringUtils.equals(step.name(), Framework.getProperty(ReIndexingTestConstants.FIRE_TEST_ERRORS_ON_STEP_PROP))
                && StringUtils.equals(step.name(), Framework.getProperty(ReIndexingTestConstants.FIRE_TEST_ERRORS_ON_STEP_RECOVERY_PROP))) {
            throw new Exception(String.format("[RECOVERY ERROR TEST] during: %s", step.name()));
        }
    }

}
