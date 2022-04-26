/**
 *
 */
package org.tst.opentoutatice.elasticsearch.reindexing.docs;

import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.nuxeo.ecm.automation.client.Session;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.elasticsearch.api.ElasticSearchAdmin;
import org.nuxeo.elasticsearch.api.ElasticSearchIndexing;
import org.nuxeo.elasticsearch.api.ElasticSearchService;
import org.nuxeo.elasticsearch.test.RepositoryElasticSearchFeature;
import org.nuxeo.runtime.test.runner.BlacklistComponent;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.Jetty;
import org.nuxeo.runtime.test.runner.LocalDeploy;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsState;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsStateChecker;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.IndexNAliasManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.runner.step.ReIndexingRunnerStep;
import org.opentoutatice.elasticsearch.core.reindexing.docs.test.constant.ReIndexingTestConstants;
import org.opentoutatice.elasticsearch.reindexing.docs.config.ZeroDownTimeErrorsConfigFeature;
import org.opentoutatice.elasticsearch.reindexing.docs.feature.EmbeddedAutomationServerFeatureWithOsvClient;

import com.google.inject.Inject;

/**
 *
 * @author david
 *
 */
@RunWith(FeaturesRunner.class)
@Features({ZeroDownTimeErrorsConfigFeature.class, RepositoryElasticSearchFeature.class, EmbeddedAutomationServerFeatureWithOsvClient.class})
@BlacklistComponent("org.nuxeo.elasticsearch.ElasticSearchComponent")
@Deploy({"org.nuxeo.ecm.automation.test", "org.nuxeo.elasticsearch.core.test"})
@LocalDeploy({"fr.toutatice.ecm.platform.elasticsearch", "fr.toutatice.ecm.platform.elasticsearch:elasticsearch-config-test.xml",
        "fr.toutatice.ecm.platform.elasticsearch:usermanger-test.xml", "fr.toutatice.ecm.platform.elasticsearch:log4j.xml"})
@Jetty(port = 18080)
@RepositoryConfig(cleanup = Granularity.CLASS)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ZeroDownTimeReIndexingErrorsCasesTst {

    protected static final Log log = LogFactory.getLog(ZeroDownTimeReIndexingErrorsCasesTst.class);

    static final String[][] users = {{"VirtualAdministrator", "secret"}, {"Administrator", "Administrator"}};
    static final int NB_DOCS = 5;

    @Inject
    protected ElasticSearchAdmin esAdmin;

    @Inject
    protected ElasticSearchIndexing esIndexing;

    @Inject
    protected ElasticSearchService esService;
    
    @Inject
    protected WorkManager wm;

    @Inject
    protected CoreSession session;

    @Inject
    protected Session automationSession;

    protected static int nbAliases = 1;
    protected static int nbIndices = 1;

    // Error is fired at end of initialization step
    @Test
    public void testA_ReIndexingWithErrorOnInitializationStep() throws Exception {
        // Es state = {nbIndices, nbAliases}
        int[] expectedInitialEsState = {1, 1};
        int[] expectedFinalEsState = {1, 1};
        this.testErrorRecovery(ReIndexingRunnerStep.initialization, expectedInitialEsState, expectedFinalEsState);
    }

    // Error is fired at end of indexing step
    @Test
    public void testB_ReIndexingWithErrorOnIndexingStep() throws Exception {
        // Es state = {nbIndices, nbAliases}
        int[] expectedInitialEsState = {1, 1};
        int[] expectedFinalEsState = {2, 1};
        this.testErrorRecovery(ReIndexingRunnerStep.indexing, expectedInitialEsState, expectedFinalEsState);
    }

    // Error is fired at end of switching step
    @Test
    public void testC_ReIndexingWithErrorOnSwitchingStep() throws Exception {
        // Es state = {nbIndices, nbAliases}
        int[] expectedInitialEsState = {2, 1};
        // New index creating
        int[] expectedFinalEsState = {3, 1};
        this.testErrorRecovery(ReIndexingRunnerStep.switching, expectedInitialEsState, expectedFinalEsState);
    }

    // Error is fired at end of initialization step
    // Recovery error is fired at beginning of recovery
    @Test
    public void testD_ReIndexingWithErrorOnInitializationStepAndRecovery() throws Exception {
        // Es state = {nbIndices, nbAliases}
        int[] expectedInitialEsState = {3, 1};
        // New index creating & read/write alias kept
        int[] expectedFinalEsState = {4, 3};
        System.setProperty(ReIndexingTestConstants.FIRE_TEST_ERRORS_ON_STEP_RECOVERY_PROP, ReIndexingRunnerStep.initialization.name());
        this.testErrorRecovery(ReIndexingRunnerStep.initialization, expectedInitialEsState, expectedFinalEsState);
    }

    // Error is fired at end of indexing step
    // Recovery error fired at beginning of recovery
    @Test
    public void testE_ReIndexingWithErrorOnIndexingStepAndRecovery() throws Exception {
        // Es state = {nbIndices, nbAliases}
        int[] expectedInitialEsState = {4, 3};
        // New index creating & read/write alias kept
        int[] expectedFinalEsState = {5, 3};
        System.setProperty(ReIndexingTestConstants.FIRE_TEST_ERRORS_ON_STEP_RECOVERY_PROP, ReIndexingRunnerStep.indexing.name());
        // TODO: re-indexing is not launched cause bad initial state ("c'est normal")
        Exception exc = null;
        try {
            this.testErrorRecovery(ReIndexingRunnerStep.indexing, expectedInitialEsState, expectedFinalEsState);
        } catch (Exception e) {
            exc = e;
        }
        Assert.assertNotNull(exc);
        
    }

    /**
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws Exception
     */
    protected void testErrorRecovery(ReIndexingRunnerStep step, int[] expectedInitialEsState, int[] expectedFinalEsState)
            throws InterruptedException, ExecutionException, Exception {
        String repoName = this.session.getRepositoryName();

        this.checkEsState(repoName, expectedInitialEsState[0], expectedInitialEsState[1]);

        // To Fire Exception at end of step
        System.setProperty(ReIndexingTestConstants.FIRE_TEST_ERRORS_ON_STEP_PROP, step.name());
        // Launch zero down time re-indexing
        ZeroDownTimeReIndexingTst.launchReIndexingFromAutomation(this.automationSession, users);

        // Waiting for ordering logs
        ZeroDownTimeReIndexingTst.waitReIndexing(repoName);

        // Asserts:
        this.checkEsState(repoName, expectedFinalEsState[0], expectedFinalEsState[1]);
        
    }

    private EsState checkEsState(String repoName, int expectedNbIndices, int expectedNbAliases) throws InterruptedException, ExecutionException {
        EsState esState = EsStateChecker.get().getEsState();

        // Long initialNbDocs = initialEsState.getNbDocsByIndicesOn(repoName,
        // false).get("nxutest-alias");
        Assert.assertEquals(expectedNbIndices, esState.getNbIndices());
        Assert.assertEquals(expectedNbAliases, esState.getNbAliases());

        return esState;
    }

    // No error on recovery Es State
    public void checkFinalEsStateWithError(String repoName, int nbAliasesExpected, int nbExpectedIndices) throws InterruptedException, ExecutionException {

        EsState finalEsState = EsStateChecker.get().getEsState();

        // Still one index
        Assert.assertEquals(nbExpectedIndices, finalEsState.getNbIndices());
        // Still one alias
        Assert.assertEquals(nbAliasesExpected, finalEsState.getNbAliases());
    }

}
