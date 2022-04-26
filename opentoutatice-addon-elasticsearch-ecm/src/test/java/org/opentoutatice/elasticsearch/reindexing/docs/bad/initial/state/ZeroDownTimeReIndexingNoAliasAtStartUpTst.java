/**
 *
 */
package org.opentoutatice.elasticsearch.reindexing.docs.bad.initial.state;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.automation.client.Session;
import org.nuxeo.ecm.automation.client.model.FileBlob;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
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
import org.opentoutatice.elasticsearch.core.reindexing.docs.automation.ReIndexZeroDownTimeES;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.test.EsNodeTestInitializer;
import org.opentoutatice.elasticsearch.reindexing.docs.config.ZeroDownTimeErrorStartUpConfigFeature;
import org.opentoutatice.elasticsearch.reindexing.docs.feature.EmbeddedAutomationServerFeatureWithOsvClient;
import org.tst.opentoutatice.elasticsearch.reindexing.docs.ZeroDownTimeReIndexingTst;

import com.google.inject.Inject;

/**
 * @author david
 *
 */
@RunWith(FeaturesRunner.class)
@Features({ZeroDownTimeErrorStartUpConfigFeature.class, RepositoryElasticSearchFeature.class, EmbeddedAutomationServerFeatureWithOsvClient.class})
@BlacklistComponent("org.nuxeo.elasticsearch.ElasticSearchComponent")
@Deploy({"org.nuxeo.ecm.automation.test", "org.nuxeo.elasticsearch.core.test",})
@LocalDeploy({"fr.toutatice.ecm.platform.elasticsearch", "fr.toutatice.ecm.platform.elasticsearch:elasticsearch-config-test.xml",
        "fr.toutatice.ecm.platform.elasticsearch:usermanger-test.xml", "fr.toutatice.ecm.platform.elasticsearch:log4j.xml"})
@Jetty(port = 18080)
@RepositoryConfig(cleanup = Granularity.METHOD)
public class ZeroDownTimeReIndexingNoAliasAtStartUpTst {

    protected static final Log log = LogFactory.getLog(ZeroDownTimeReIndexingTst.class);

    static final String[][] users = {{"VirtualAdministrator", "secret"}, {"Administrator", "Administrator"}};
    static final int NB_DOCS = 5;

    @Inject
    protected ElasticSearchAdmin esAdmin;

    @Inject
    protected ElasticSearchIndexing esIndexing;

    @Inject
    protected ElasticSearchService esService;

    @Inject
    protected CoreSession session;

    @Inject
    protected Session automationSession;

    @Test
    public void testReIndexingWithBadInitialEsState() {

        boolean error = false;
        try {
            this.zeroDownTimeReIndexingFromAutomation();
        } catch (Exception esc) {
            error = true;
        }

        Assert.assertEquals(true, error);
    }

    /**
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws Exception
     */
    private void zeroDownTimeReIndexingFromAutomation() throws InterruptedException, ExecutionException, Exception {
        String repoName = this.session.getRepositoryName();

        // Launch zero down time re-indexing
        this.launchReIndexingFromAutomation();

        // Waiting for log ordering
        this.waitReIndexing(repoName);

    }

    public String launchReIndexingFromAutomation() throws Exception {
        StringBuilder launchedStatus = new StringBuilder();

        Session session_ = null;
        try {
            session_ = this.automationSession.getClient().getSession(users[0][0], users[0][1]);
            FileBlob launchedStatusFile = (FileBlob) session_.newRequest(ReIndexZeroDownTimeES.ID).set("repository", "test").execute();


            try (BufferedReader br = Files.newBufferedReader(Paths.get(launchedStatusFile.getFile().getPath()))) {
                launchedStatus.append(br.readLine());
            }

            log.info(String.format("[========= Launched status: [%s] =========]", launchedStatus.toString()));
        } finally {
            if (session_ != null) {
                session_.close();
            }
        }

        return launchedStatus.toString();
    }

    /**
     * @param repoName
     * @throws InterruptedException
     */
    public void waitReIndexing(String repoName) throws InterruptedException {
        Thread.sleep(1000);
        boolean running = ReIndexingRunnerManager.get().isReIndexingInProgress(repoName);
        while (running) {
            Thread.sleep(500);
            running = ReIndexingRunnerManager.get().isReIndexingInProgress(repoName);
        }
    }
    
}
