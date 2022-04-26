/**
 *
 */
package org.tst.opentoutatice.elasticsearch.reindexing.docs;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonNode;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.nuxeo.ecm.automation.client.OperationRequest;
import org.nuxeo.ecm.automation.client.RemoteException;
import org.nuxeo.ecm.automation.client.Session;
import org.nuxeo.ecm.automation.client.model.Document;
import org.nuxeo.ecm.automation.client.model.Documents;
import org.nuxeo.ecm.automation.client.model.FileBlob;
import org.nuxeo.ecm.core.api.CoreInstance;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.api.PathRef;
import org.nuxeo.ecm.core.api.VersioningOption;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.elasticsearch.api.ElasticSearchAdmin;
import org.nuxeo.elasticsearch.api.ElasticSearchIndexing;
import org.nuxeo.elasticsearch.api.ElasticSearchService;
import org.nuxeo.elasticsearch.listener.ElasticSearchInlineListener;
import org.nuxeo.elasticsearch.query.NxQueryBuilder;
import org.nuxeo.elasticsearch.test.RepositoryElasticSearchFeature;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.BlacklistComponent;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.Jetty;
import org.nuxeo.runtime.test.runner.LocalDeploy;
import org.nuxeo.runtime.transaction.TransactionHelper;
import org.opentoutatice.elasticsearch.OttcElasticSearchComponent;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.core.reindexing.docs.automation.CleanESIndices;
import org.opentoutatice.elasticsearch.core.reindexing.docs.automation.ReIndexZeroDownTimeES;
import org.opentoutatice.elasticsearch.core.reindexing.docs.constant.ReIndexingConstants;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsState;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsStateChecker;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception.ReIndexingStateException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception.ReIndexingStatusException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.status.ReIndexingProcessStatusBuilder;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.IndexNAliasManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.query.filter.ReIndexingTransientAggregate;
import org.opentoutatice.elasticsearch.core.reindexing.docs.transitory.TransitoryIndexUse;
import org.opentoutatice.elasticsearch.reindexing.docs.config.ZeroDownTimeConfigFeature;
import org.opentoutatice.elasticsearch.reindexing.docs.feature.EmbeddedAutomationServerFeatureWithOsvClient;

import com.google.inject.Inject;

import fr.toutatice.ecm.elasticsearch.automation.QueryES;

/**
 * @author dchevrier
 *
 */
@RunWith(FeaturesRunner.class)
@Features({ZeroDownTimeConfigFeature.class, RepositoryElasticSearchFeature.class, EmbeddedAutomationServerFeatureWithOsvClient.class})
@BlacklistComponent("org.nuxeo.elasticsearch.ElasticSearchComponent")
@Deploy({"org.nuxeo.ecm.automation.test", "org.nuxeo.elasticsearch.core.test", "org.nuxeo.ecm.core.event"})
@LocalDeploy({"fr.toutatice.ecm.platform.elasticsearch", "fr.toutatice.ecm.platform.elasticsearch:elasticsearch-config-test.xml",
        "fr.toutatice.ecm.platform.elasticsearch:usermanger-test.xml", "fr.toutatice.ecm.platform.elasticsearch:log4j.xml"})
@Jetty(port = 18080)
@RepositoryConfig(cleanup = Granularity.CLASS)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ZeroDownTimeReIndexingTst {

    protected static final Log log = LogFactory.getLog(ZeroDownTimeReIndexingTst.class);

    static final String[][] users = {{"VirtualAdministrator", "secret"}, {"Administrator", "Administrator"}};
    static final int NB_DOCS = 50;

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

    @Inject
    protected WorkManager wm;

    // One index & alias at tests begining
    protected static int nbAliases = 1;
    protected static int nbIndices = 1;
    protected static int totalDocs;

    protected static boolean indexedOnce = false;

    /**
     * Create docs.
     *
     * @throws Exception
     *
     * @throws IndexExistenceException
     */
    @Before
    public void prepareRepository() throws Exception {
        // FIXME: can be done with CoreRepoistory Test class?
        log.debug("Starting populate repo...");

        boolean tx = false;
        try {
            if (TransactionHelper.isTransactionActive()) {
                TransactionHelper.commitOrRollbackTransaction();
                tx = TransactionHelper.startTransaction();
            }

            ElasticSearchInlineListener.useSyncIndexing.set(true);

            // Populate repo & index
            DocumentModel rootDocument = this.session.getRootDocument();
            // Create container
            DocumentModel modelToCreate = this.session.createDocumentModel(rootDocument.getPathAsString(), "ws_container", "Workspace");
            DocumentModel container = this.session.createDocument(modelToCreate);
            // Commit to fire indexing
            this.session.save();

            // Docs in container
            DocumentModel docToVersion = null;
            for (int nb = 0; nb < NB_DOCS; nb++) {
                String docSuffix = String.valueOf(nb + 1);
                DocumentModel docModelToCreate = this.session.createDocumentModel(container.getPathAsString(), "Note_".concat(docSuffix), "Note");
                docToVersion = this.session.createDocument(docModelToCreate);
                // Commit to fire indexing
            }
            this.session.save();

            // For test:Are versions indexed?:
            for (int nbVersion = 0; nbVersion < 3; nbVersion++) {
                this.session.checkIn(docToVersion.getRef(), VersioningOption.MAJOR, StringUtils.EMPTY);
                this.session.checkOut(docToVersion.getRef());
            }
            this.session.save();

        } catch (Exception e) {
            TransactionHelper.setTransactionRollbackOnly();
        } finally {
            if (tx) {
                TransactionHelper.commitOrRollbackTransaction();
                TransactionHelper.startTransaction();
            }
        }

        // Waiting Es indexation
        Thread.sleep(1000); // Commit time
        while (this.esAdmin.isIndexingInProgress()) {
            Thread.sleep(500);
        }
        this.esAdmin.refreshRepositoryIndex(this.session.getRepositoryName());

        // Test: are versions indexed?
        NxQueryBuilder qBuilder = new NxQueryBuilder(this.session);
        qBuilder.nxql("select * from Document");
        DocumentModelList allDocs = this.esService.query(qBuilder);
        log.debug("Docs in index from Core: ");
        for (DocumentModel doc : allDocs) {

            log.debug(String.format("%s | %s | %s | %s", doc.getName(), doc.getVersionLabel(), doc.isVersion(),
                    ReIndexingProcessStatusBuilder.dateFormat.format(((GregorianCalendar) doc.getPropertyValue("dc:created")).getTime())));
        }
        // Documents allDocsFromAutomation = this.esQueryFromAutomation("select * from Document");
        // log.debug("Docs in index from Automation: ");
        // for(Document doc : allDocsFromAutomation) {
        // log.debug(String.format("%s | %s | %s ", doc.getName(), doc.getVersionLabel(), doc.isVersion()));
        // }

        log.debug("Repo populated.");

    }

    @Test
    public void testA_ZeroDownTimeReIndeingFromCore()
            throws InterruptedException, ExecutionException, ReIndexingStatusException, ReIndexingStateException, ReIndexingException {
        String repoName = this.session.getRepositoryName();

        EsState initialEsState = checkInitialEsState(repoName);

        // Docs list
        DocumentModelList initialDocs = this.getAllDocs(repoName);
        
        // Launch zero down time re-indexing
        ((OttcElasticSearchIndexing) this.esIndexing).reIndexAllDocumentsWithZeroDownTime(repoName);
        
        // Waiting for re-indexing
        waitReIndexing(repoName);
       
        // Refresh index
        this.esAdmin.refreshRepositoryIndex(this.session.getRepositoryName());
        
        // Asserts:
        this.checkFinalEsState(repoName, initialEsState, initialDocs);
    }

    @Test
    public void testB_ZeroDownTimeReIndexingFromAutomation() throws Exception {
        this.zeroDownTimeReIndexingFromAutomation();
    }

    @Test
    public void testC_ZeroDownTimeReadWriteDuringReIndexingFromCore()
            throws ReIndexingStatusException, ReIndexingStateException, ReIndexingException, InterruptedException, ExecutionException {
        String repoName = this.session.getRepositoryName();

        Assert.assertEquals(Boolean.FALSE, ReIndexingRunnerManager.get().isReIndexingInProgress(repoName));

        // Existing docs
        DocumentModelList workspaces = this.getWorkspaces(repoName);

        // Launch zero down time re-indexing with slow loop wait (cf ZeroDownTimeReadWriteConfigFeature)
        // (~ "suspend" re-indexing to test read-write)
        ((OttcElasticSearchIndexing) this.esIndexing).reIndexAllDocumentsWithZeroDownTime(repoName);

        // Read - write test when re-indexing is suspending: =========
        Thread.sleep(100);
        Assert.assertEquals(Boolean.TRUE, ReIndexingRunnerManager.get().isReIndexingInProgress(repoName));

        // Write test -----------
        String indexOrAlias = ((OttcElasticSearchComponent) this.esAdmin).getIndexNameForRepository(repoName);
        Assert.assertEquals(TransitoryIndexUse.Write.getAlias(), indexOrAlias);
        
        // Wait initialization
        Thread.sleep(500);
        
        boolean tx = false;
        DocumentModel createdWs = null;
        try {
            if (TransactionHelper.isTransactionActive()) {
                TransactionHelper.commitOrRollbackTransaction();
                tx = TransactionHelper.startTransaction();
            }
            DocumentModel wsToCreate = this.session.createDocumentModel("Workspace");
            // Create 2 documents
            createdWs = this.session.createDocument(wsToCreate);
            this.session.createDocument(wsToCreate);
            log.debug(String.format("[========== Writing test launched for [%s] ==========]", createdWs.getId()));

            // To fire indexing (sync for ws)
            ElasticSearchInlineListener.useSyncIndexing.set(true);
            this.session.save();

            // Delete yet existing Note
            this.session.removeDocument(new PathRef("/ws_container/Note_1"));
            this.session.save();
        } catch (Exception e) {
            TransactionHelper.setTransactionRollbackOnly();
        } finally {
            if (tx) {
                TransactionHelper.commitOrRollbackTransaction();
                log.debug(String.format("Documents created (& one removed) at [%s]", ReIndexingProcessStatusBuilder.dateFormat.format(new Date())));
                TransactionHelper.startTransaction();
            }
        }

        // --------------------------

        // Read test -----------
        List<String> repos = new ArrayList<>();
        repos.add(repoName);
        String[] searchIndexes = ((OttcElasticSearchComponent) this.esAdmin).getElasticSearchAdmin().getSearchIndexes(repos);
        log.debug("Search indices: ");
        for (String sIdx : searchIndexes) {
            log.debug(sIdx);
        }

        List<String> readIndices = IndexNAliasManager.get().getIndicesOfAlias(TransitoryIndexUse.Read.getAlias());
        // There exist some empty or null values in list (?...)
        String[] indicesOfReadAlias = new String[readIndices.size()];
        log.debug(String.format("Indices of [%s] alias: ", TransitoryIndexUse.Read.getAlias()));
        int pos = 0;
        for (String idxOf : readIndices) {
            if (StringUtils.isNotBlank(idxOf)) {
                indicesOfReadAlias[pos] = idxOf;
                log.debug(idxOf);
                pos++;
            }
        }

        Assert.assertEquals(Boolean.TRUE, ArrayUtils.isEquals(searchIndexes, indicesOfReadAlias));

        // FIXME: cache?
        log.debug(String.format("[========== Reading test for [%s] ==========]", createdWs.getId()));
        DocumentModel reFoundWs = this.getDoc(repoName, createdWs.getId());
        log.debug("[========== =========== ==========]");
        Assert.assertNotNull(reFoundWs);

        DocumentModelList incrementedWorkspaces = this.getWorkspaces(repoName);
        Assert.assertEquals(incrementedWorkspaces.size(), workspaces.size() + 2);

        // ===========================================================

        // To be sure switch on new index is done
        waitReIndexing(repoName);
        Assert.assertEquals(Boolean.FALSE, ReIndexingRunnerManager.get().isReIndexingInProgress(repoName));

        // For global tests
        nbIndices += 1;
    }

    @Test
    public void testD_ZeroDownTimeReadDuplicateDuringReIndexingFromAutomation() throws Exception {
        
        String repoName = this.session.getRepositoryName();
        final String ALL_DOCS_QUERY = "select * from Document";

        Assert.assertEquals(Boolean.FALSE, ReIndexingRunnerManager.get().isReIndexingInProgress(repoName));

        // Existing docs
        Documents allDocsInfirstIndex = this.esQueryFromAutomation(ALL_DOCS_QUERY);

        // Launch zero down time re-indexing with slow loop wait (cf ZeroDownTimeReadWriteConfigFeature)
        // (~ "suspend" re-indexing to test read-write)
        launchReIndexingFromAutomation(this.automationSession, users);

        Assert.assertEquals(Boolean.TRUE, ReIndexingRunnerManager.get().isReIndexingInProgress(repoName));

        // Check duplicates before end of suspend time
        Long suspendTime = Long.valueOf(Framework.getProperty(ReIndexingConstants.REINDEXING_WAIT_LOOP_TIME));
        Thread.sleep((suspendTime.longValue() - 1) * 1000);

        // Query must have duplicates
        try (CoreSession session_ = CoreInstance.openCoreSessionSystem(repoName)) {
            NxQueryBuilder qBuilder = new NxQueryBuilder(session_);
            qBuilder.nxql(ALL_DOCS_QUERY);

            SearchResponse response = ((OttcElasticSearchComponent) this.esService).getEsService().search(qBuilder);
            StringTerms duplicateAggs = response.getAggregations().get(ReIndexingTransientAggregate.DUPLICATE_AGGREGATE_NAME);
            // Build duplicate list
            List<String> duplicateIds = new LinkedList<String>();

            for (org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket : duplicateAggs.getBuckets()) {
                if (bucket.getDocCount() > 1) {
                    duplicateIds.add(bucket.getKey());
                }
            }

            Assert.assertEquals(Boolean.TRUE, duplicateIds.size() > 0);
        }

        log.debug("------------------------------------------------------------------------------");
        long startTime = System.currentTimeMillis();

        Documents filteredDocs = this.esQueryFromAutomation("select * from Document");

        long duration = System.currentTimeMillis() - startTime;
        log.debug(String.format("====== Query from automation done: [%s] ms", String.valueOf(duration)));
        log.debug(String.format("Nb docs on one index: [%s] | Nb docs on twice: [%s]", allDocsInfirstIndex.size(), filteredDocs.size()));
        log.debug("------------------------------------------------------------------------------");

        Assert.assertEquals(allDocsInfirstIndex.size(), filteredDocs.size());


        waitReIndexing(repoName);
        Assert.assertEquals(Boolean.FALSE, ReIndexingRunnerManager.get().isReIndexingInProgress(repoName));

        // For global tests
        nbIndices += 1;
    }

    @Test
    public void testE_ZeroDownTimeReIndexingTwiceFromAutomation() throws Exception {
       
        String repoName = this.session.getRepositoryName();

        // First launch
        this.zeroDownTimeReIndexingFromAutomation();

        // Es state
        EsState intermediateEsState = EsStateChecker.get().getEsState();
        // Docs list
        DocumentModelList intermediateDocs = this.getAllDocs(repoName);

        // Launch zero down time re-indexing
        launchReIndexingFromAutomation(this.automationSession, users);
        // Waiting for re-indexing
        waitReIndexing(repoName);
        // Refresh index
        this.esAdmin.refreshRepositoryIndex(this.session.getRepositoryName());

        // Asserts:
        this.checkFinalEsState(repoName, intermediateEsState, intermediateDocs);
    }

    @Test
    public void testF_ZeroDownTimeReIndexingYetRunningFromAutomation() throws Exception {
        
        // Launch zero down time re-indexing
        launchReIndexingFromAutomation(this.automationSession, users);
        // Do not wait & launch concurrent call
        RemoteException exc = null;
        try {
            Thread.sleep(150);
            launchReIndexingFromAutomation(this.automationSession, users);
        } catch (RemoteException e) {
            exc = e;
        }

        Assert.assertNotNull(exc);
        JsonNode excAsJsonNode = ((org.nuxeo.ecm.automation.client.jaxrs.spi.JsonMarshalling.RemoteThrowable) exc.getRemoteCause()).getOtherNodes()
                .get("className");

        Assert.assertEquals(ReIndexingStatusException.class.getCanonicalName(), excAsJsonNode.getTextValue());
        
        waitReIndexing(this.session.getRepositoryName());
        
        nbIndices++;
    }

    @Test
    public void testG_copyDuringReIndexing() throws Exception {
        
        checkInitialEsState(this.session.getRepositoryName());
        
        launchReIndexingFromAutomation(this.automationSession, users);

        Thread.sleep(150);
        
        DocumentModel rootDocument = null;
        DocumentModel createdWsContainer = null;

        if (TransactionHelper.isTransactionActive()) {
            TransactionHelper.commitOrRollbackTransaction();
        }
        TransactionHelper.startTransaction();

        try {
            ElasticSearchInlineListener.useSyncIndexing.set(Boolean.TRUE);

            // Prepare
            final int nbChildren = 1;
            final int depth = 5;

            rootDocument = this.session.getRootDocument();

            DocumentModel wsContainerToCreate = this.session.createDocumentModel(rootDocument.getPathAsString(), "ws_container", "WorkspaceRoot");
            createdWsContainer = this.session.createDocument(wsContainerToCreate);

            for (int nbChild = 0; nbChild < nbChildren; nbChild++) {
                DocumentModel wsToCeate = this.session.createDocumentModel(createdWsContainer.getPathAsString(), "ws_".concat(String.valueOf(nbChild)),
                        "Workspace");
                DocumentModel createdWs = this.session.createDocument(wsToCeate);

                DocumentModel parent = createdWs;
                for (int level = 0; level < depth; level++) {
                    DocumentModel childToCreate = this.session.createDocumentModel(parent.getPathAsString(),
                            parent.getName().concat("-").concat(String.valueOf(level)), "Folder");
                    parent = this.session.createDocument(childToCreate);
                }
                this.session.save();
            }

            this.session.save();

        } finally {
            TransactionHelper.commitOrRollbackTransaction();
            TransactionHelper.startTransaction();
        }

        // Copy
        ElasticSearchInlineListener.useSyncIndexing.set(Boolean.TRUE);
        DocumentModel copiedFolderish = this.session.copy(createdWsContainer.getRef(), rootDocument.getRef(), "COPIED_ws_container");
        this.session.save();
        log.debug("============== COPIED ================");
        TransactionHelper.commitOrRollbackTransaction();

        Assert.assertNotNull(copiedFolderish);
        
        waitReIndexing(this.session.getRepositoryName());
        
        nbIndices++;
    }

    @Test
    public void testH_cleanIndices() throws Exception {

        String returnedStatus = callOp(this.automationSession, users, CleanESIndices.ID, null);
        log.info(returnedStatus);
        Assert.assertTrue(StringUtils.contains(returnedStatus, "[7] orphan indices deleted"));

        // Nothing to clean now
        returnedStatus = callOp(this.automationSession, users, CleanESIndices.ID, null);
        log.info(returnedStatus);
        Assert.assertTrue(StringUtils.contains(returnedStatus, "Found no orphan indices to clean"));
    }

    @Test
    public void testH_cleanIndicesWhileReIndexing() throws Exception {
        // No wait afeter re-indexing launched
        launchReIndexingFromAutomation(this.automationSession, users);

        String returnedStatus = callOp(this.automationSession, users, CleanESIndices.ID, null);
        log.info(returnedStatus);
        Assert.assertTrue(StringUtils.contains(returnedStatus, "One Zero Down Time Re-Indexing process is in progress"));
    }

    /**
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws Exception
     */
    private void zeroDownTimeReIndexingFromAutomation() throws InterruptedException, ExecutionException, Exception {
        String repoName = this.session.getRepositoryName();

        EsState initialEsState = checkInitialEsState(repoName);

        // Docs list
        DocumentModelList initialDocs = this.getAllDocs(repoName);

        // Launch zero down time re-indexing
        launchReIndexingFromAutomation(this.automationSession, users);

        // Waiting for re-indexing
        waitReIndexing(repoName);
        Assert.assertFalse(ReIndexingRunnerManager.get().isReIndexingInProgress());

        // Refresh index
        this.esAdmin.refreshRepositoryIndex(this.session.getRepositoryName());

        // Asserts:
        this.checkFinalEsState(repoName, initialEsState, initialDocs);
    }

    /**
     * @throws Exception
     */
    public static String launchReIndexingFromAutomation(Session automationSession, String[][] users) throws Exception {
        Map<String, String> param = new HashMap<String, String>();
        param.put("repository", "test");
        return callOp(automationSession, users, ReIndexZeroDownTimeES.ID, param);
    }

    /**
     * @param automationSession
     * @param users
     * @return
     * @throws Exception
     * @throws IOException
     */
    protected static String callOp(Session automationSession, String[][] users, String opId, Map<String, String> params) throws Exception, IOException {
        StringBuilder launchedStatus = new StringBuilder();

        Session session_ = null;
        try {
            session_ = automationSession.getClient().getSession(users[0][0], users[0][1]);
            OperationRequest operationRequest = session_.newRequest(opId);

            if (MapUtils.isNotEmpty(params)) {
                for (Entry<String, String> entry : params.entrySet()) {
                    operationRequest.set(entry.getKey(), entry.getValue());
                }
            }

            FileBlob launchedStatusFile = (FileBlob) operationRequest.execute();


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

    public static EsState checkInitialEsState(String repoName) throws InterruptedException, ExecutionException {
        EsState initialEsState = EsStateChecker.get().getEsState();
        log.info("[Initial Es State]: " + initialEsState.toString());

        // Long initialNbDocs = initialEsState.getNbDocsByIndicesOn(repoName, false).get("nxutest-alias");
        final int initialNbIndices = initialEsState.getNbIndices();
        Assert.assertEquals(nbIndices, initialNbIndices);

        final int initialNbAliases = initialEsState.getNbAliases();
        Assert.assertEquals(nbAliases, initialNbAliases);

        return initialEsState;
    }

    /**
     * @param repoName
     * @param initialState
     * @param initialDocs
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void checkFinalEsState(String repoName, EsState initialState, DocumentModelList initialDocs) throws InterruptedException, ExecutionException {
        EsState finalEsState = EsStateChecker.get().getEsState();
        log.info("[Final Es State]: " + finalEsState.toString());

        // Long finalNbDocs = initialEsState.getNbDocsByIndicesOn(repoName, false).get("nxutest-alias");
        int finalNbAliases = finalEsState.getNbAliases();
        int finalNbIndices = finalEsState.getNbIndices();

        // New index created
        Assert.assertEquals(initialState.getNbIndices() + 1, finalNbIndices);
        nbIndices += 1;

        // Former alias added
        if (indexedOnce) {
            Assert.assertEquals(initialState.getNbAliases(), finalNbAliases);
        } else {
            Assert.assertEquals(initialState.getNbAliases() + 1, finalNbAliases);
            nbAliases += 1;
            indexedOnce = true;
        }

        // Docs list
        DocumentModelList finalDocs = this.getAllDocs(repoName);

        // Check docs lists
        int nbPresent = 0;
        int nbAbsent = 0;
        for (DocumentModel initialDoc : initialDocs) {
            boolean found = false;
            Iterator<DocumentModel> iterator = finalDocs.iterator();
            while (iterator.hasNext() && !found) {
                DocumentModel finalDoc = iterator.next();
                found = finalDoc.getId().equals(initialDoc.getId());
            }
            if (!found) {
                nbAbsent++;
                log.error(String.format("Doc [%s] not found in new index.", initialDoc.getPathAsString()));
            } else {
                nbPresent++;
            }
        }

        Assert.assertEquals(initialDocs.size(), nbPresent + nbAbsent);
        Assert.assertEquals(initialDocs.size(), finalDocs.size());


        // !!! NOTE: Nb docs as rows ar different; but when fetch as docs, its ok: Fetcher used by Es filters
        // (TODO: understand how) => Root is not seen as a Document
        // Assert.assertEquals(initialNbDocs, finalNbDocs);
    }

    /**
     * @param repoName
     * @throws InterruptedException
     */
    public static void waitReIndexing(String repoName) throws InterruptedException {
        Thread.sleep(1000);
        boolean running = ReIndexingRunnerManager.get().isReIndexingInProgress(repoName);
        while (running) {
            Thread.sleep(500);
            running = ReIndexingRunnerManager.get().isReIndexingInProgress(repoName);
        }
    }

    public DocumentModel getDoc(String repoName, String uuid) {
        DocumentModel doc = null;
        DocumentModelList res = this.nxQueryOnEs(repoName, String.format("select * from Document where ecm:uuid = '%s'", uuid));
        if (res.size() > 0) {
            doc = res.get(0);
        }
        return doc;
    }

    public DocumentModelList getWorkspaces(String repoName) {
        return this.nxQueryOnEs(repoName, "select * from Workspace");
    }

    /**
     * @param sessionSystem
     */
    public DocumentModelList getAllDocs(String repoName) {
        return this.nxQueryOnEs(repoName, "select * from Document");
    }

    /**
     * @param repoName
     * @return
     */
    private DocumentModelList nxQueryOnEs(String repoName, String nxql) {
        DocumentModelList res = null;

        CoreSession sessionSystem = null;
        try {
            sessionSystem = CoreInstance.openCoreSessionSystem(repoName);
            NxQueryBuilder queryBuilder = new NxQueryBuilder(sessionSystem);
            queryBuilder.nxql(nxql).limit(1000);

            res = this.esService.query(queryBuilder);
        } finally {
            if (sessionSystem != null) {
                sessionSystem.close();
            }
        }
        return res;
    }

    /**
     * @throws Exception
     *
     */
    public Documents esQueryFromAutomation(String query) throws Exception {
        Documents docs = null;

        Session session_ = null;
        try {
            session_ = this.automationSession.getClient().getSession(users[0][0], users[0][1]);
            docs = (Documents) session_.newRequest(QueryES.ID).set("query", query).execute();

            if (log.isTraceEnabled()) {
                log.debug("[Founded docs on Es]: ");
                for (Document document : docs) {
                    log.debug(document.getPath());
                }
            }
        } finally {
            if (session_ != null) {
                session_.close();
            }
        }

        return docs;
    }

}
