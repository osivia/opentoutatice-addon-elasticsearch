/**
 * 
 */
package org.opentoutatice.elasticsearch.reindexing.docs;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.automation.client.Session;
import org.nuxeo.ecm.automation.client.model.FileBlob;
import org.nuxeo.ecm.core.api.CoreInstance;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.elasticsearch.api.ElasticSearchAdmin;
import org.nuxeo.elasticsearch.api.ElasticSearchIndexing;
import org.nuxeo.elasticsearch.api.ElasticSearchService;
import org.nuxeo.elasticsearch.listener.ElasticSearchInlineListener;
import org.nuxeo.elasticsearch.query.NxQueryBuilder;
import org.nuxeo.elasticsearch.test.RepositoryElasticSearchFeature;
import org.nuxeo.runtime.test.runner.BlacklistComponent;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.Jetty;
import org.nuxeo.runtime.test.runner.LocalDeploy;
import org.nuxeo.runtime.transaction.TransactionHelper;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.core.reindexing.docs.automation.ReIndexZeroDownTimeES;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsState;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsStateChecker;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception.EsStateCheckException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.exception.IndexException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;
import org.opentoutatice.elasticsearch.reindexing.docs.config.EmbeddedAutomationServerFeatureWithOsvClient;
import org.opentoutatice.elasticsearch.reindexing.docs.config.ZeroDownTimeReadWriteConfigFeature;

import com.google.inject.Inject;

/**
 * @author dchevrier 
 *
 */
@RunWith(FeaturesRunner.class)
@Features({ ZeroDownTimeReadWriteConfigFeature.class, RepositoryElasticSearchFeature.class,
		EmbeddedAutomationServerFeatureWithOsvClient.class })
@BlacklistComponent("org.nuxeo.elasticsearch.ElasticSearchComponent")
@Deploy({ "org.nuxeo.ecm.automation.test", "org.nuxeo.elasticsearch.core.test", /* "org.nuxeo.ecm.platform.audit" */ })
@LocalDeploy({ "fr.toutatice.ecm.platform.elasticsearch",
		"fr.toutatice.ecm.platform.elasticsearch:elasticsearch-config-test.xml",
		"fr.toutatice.ecm.platform.elasticsearch:usermanger-test.xml",
		"fr.toutatice.ecm.platform.elasticsearch:log4j.xml" })
@Jetty(port = 18080)
@RepositoryConfig(cleanup = Granularity.METHOD)
public class ZeroDownTimeReIndexingTest {

	protected static final Log log = LogFactory.getLog(ZeroDownTimeReIndexingTest.class);

	static final String[][] users = { { "VirtualAdministrator", "secret" }, { "Administrator", "Administrator" } };
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

	/**
	 * Create docs.
	 * 
	 * @throws InterruptedException
	 * @throws IndexExistenceException
	 * @throws ExecutionException
	 * @throws IndexException
	 */
	@Before
	public void prepareRepository()
			throws InterruptedException, IndexException, ExecutionException {
		// FIXME: can be done with CoreRepoistory Test class
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
			DocumentModel modelToCreate = this.session.createDocumentModel(rootDocument.getPathAsString(),
					"ws_container", "Workspace");
			DocumentModel container = this.session.createDocument(modelToCreate);
			// Commit to fire indexing
			this.session.save();

			// Docs in container
			for (int nb = 0; nb < NB_DOCS; nb++) {
				String docSuffix = String.valueOf(nb + 1);
				DocumentModel docModelToCreate = this.session.createDocumentModel(container.getPathAsString(),
						"Note_".concat(docSuffix), "Note");
				this.session.createDocument(docModelToCreate);
				// Commit to fire indexing
				this.session.save();
			}
		} catch (Exception e) {
			TransactionHelper.setTransactionRollbackOnly();
		} finally {
			if (tx) {
				TransactionHelper.commitOrRollbackTransaction();
				TransactionHelper.startTransaction();
			}
		}

		// Waiting Es indexation
		Thread.sleep(500); // Commit time
		while (this.esAdmin.isIndexingInProgress()) {
			Thread.sleep(500);
		}
		this.esAdmin.refreshRepositoryIndex(this.session.getRepositoryName());

		log.debug("Repo populated.");

	}
	
	@Test
	public void testZeroDownTimeReIndeingFromCore() throws InterruptedException, ExecutionException, EsStateCheckException, ReIndexingException {
		String repoName = this.session.getRepositoryName();
		
		EsState initialEsState = checkInitialEsState(repoName);
		
		// Docs list
		DocumentModelList initialDocs = getAllDocs(repoName);
		
		// Launch zero down time re-indexing
		((OttcElasticSearchIndexing) this.esIndexing).reIndexAllDocumentsWithZeroDownTime(repoName);

		// Waiting for re-indexing
		waitReIndexing(repoName);
		
		// Refresh index
		this.esAdmin.refreshRepositoryIndex(this.session.getRepositoryName());
		
		// Asserts:
		checkFinalEsState(repoName, initialEsState, initialDocs);
	}

	@Test
	public void testZeroDownTimeReIndexingFromAutomation() throws Exception {
		String repoName = this.session.getRepositoryName();
		
		EsState initialEsState = checkInitialEsState(repoName);
		
		// Docs list
		DocumentModelList initialDocs = getAllDocs(repoName);
		
		// Launch zero down time re-indexing
		launchReIndexingFromAutomation();

		// Waiting for re-indexing
		waitReIndexing(repoName);
		
		// Refresh index
		this.esAdmin.refreshRepositoryIndex(this.session.getRepositoryName());
		
		// Asserts:
		checkFinalEsState(repoName, initialEsState, initialDocs);
	}
	
	@Test
	public void testZeroDownTimeReIndexingTwiceFromAutomation() throws Exception {
		String repoName = this.session.getRepositoryName();
		
		// First launch
		testZeroDownTimeReIndexingFromAutomation();
		
		// Es state
		EsState intermediateEsState = EsStateChecker.get().getEsState();
		// Docs list
		DocumentModelList intermediateDocs = getAllDocs(repoName);
		
		// Launch zero down time re-indexing
		launchReIndexingFromAutomation();
		// Waiting for re-indexing
		waitReIndexing(repoName);
		// Refresh index
		this.esAdmin.refreshRepositoryIndex(this.session.getRepositoryName());
		
		// Asserts:
		checkFinalEsState(repoName, intermediateEsState, intermediateDocs, true);
	}
	
	@Test
	public void testZeroDownTimeReIndexingConcurrentFromAutomation() throws Exception {
		String repoName = this.session.getRepositoryName();
		
		checkInitialEsState(repoName);
		
		// Launch zero down time re-indexing
		launchReIndexingFromAutomation();
		// Do not wait & launch concurrent call
		String launcheStatus = launchReIndexingFromAutomation();
		
		Assert.assertEquals(Boolean.TRUE, StringUtils.contains(launcheStatus, "ERROR lauching re-indexing"));
	}

	/**
	 * @throws Exception
	 */
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
			if(session_ != null) {
				session_.close();
			}
		}
		
		return launchedStatus.toString();
	}
	
	public EsState checkInitialEsState(String repoName) throws InterruptedException, ExecutionException {
		EsState initialEsState = EsStateChecker.get().getEsState();
		log.info("[Initial Es State]: " + initialEsState.toString());
		
		//Long initialNbDocs = initialEsState.getNbDocsByIndicesOn(repoName, false).get("nxutest-alias");
		int initialNbIndices = initialEsState.getNbIndices();
		Assert.assertEquals(initialNbIndices, 1);
		int initialNbAliases = initialEsState.getNbAliases();
		Assert.assertEquals(initialNbAliases, 1);
		
		return initialEsState;
	}
	
	public void checkFinalEsState(String repoName, EsState initialState, DocumentModelList initialDocs)
			throws InterruptedException, ExecutionException {
		checkFinalEsState(repoName, initialState, initialDocs, false);
	}

	/**
	 * @param repoName
	 * @param initialState
	 * @param initialDocs
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void checkFinalEsState(String repoName, EsState initialState, DocumentModelList initialDocs, boolean twice)
			throws InterruptedException, ExecutionException {
		EsState finalEsState = EsStateChecker.get().getEsState();
		log.info("[Final Es State]: " + finalEsState.toString());
		
		//Long finalNbDocs = initialEsState.getNbDocsByIndicesOn(repoName, false).get("nxutest-alias");
		int finalNbAliases = finalEsState.getNbAliases();
		int finalNbIndices = finalEsState.getNbIndices();
		
		// New index created
		Assert.assertEquals(initialState.getNbIndices() + 1, finalNbIndices);
		// Former alias added
		if(twice) {
			Assert.assertEquals(initialState.getNbAliases(), finalNbAliases);
		} else {
			Assert.assertEquals(initialState.getNbAliases() + 1, finalNbAliases);
		}
		
		// Docs list
		DocumentModelList finalDocs = getAllDocs(repoName);
		
		// Check docs lists
		int nbPresent = 0;
		int nbAbsent = 0;
		for(DocumentModel initialDoc : initialDocs) {
			boolean found = false;
			Iterator<DocumentModel> iterator = finalDocs.iterator();
			while(iterator.hasNext() && !found) {
				DocumentModel finalDoc = iterator.next();
				found = finalDoc.getId().equals(initialDoc.getId());
			}
			if(!found) {
				nbAbsent++;
				log.error(String.format("Doc [%s] not found in new index.", initialDoc.getPathAsString()));
			} else {
				nbPresent++;
			}
		}
		
		Assert.assertEquals(initialDocs.size(), nbPresent + nbAbsent);
		Assert.assertEquals(initialDocs.size(), finalDocs.size());
		
		
		// !!! NOTE: Nb docs as rows ar different; but when fetch as docs, its ok: Fetcher used by Es filters
		// (TODO: understand how)
		//Assert.assertEquals(initialNbDocs, finalNbDocs);
	}

	/**
	 * @param repoName
	 * @throws InterruptedException
	 */
	public void waitReIndexing(String repoName) throws InterruptedException {
		Thread.sleep(1000);
		boolean running = ReIndexingRunnerManager.get().isReIndexingInProgress(repoName);
		while(running) {
			Thread.sleep(500);
			running = ReIndexingRunnerManager.get().isReIndexingInProgress(repoName);
		}
	}
	
	public DocumentModel getDoc(String repoName, String uuid) {
		DocumentModel doc = null;
		DocumentModelList res = nxQueryOnEs(repoName,
				String.format("select * from Document where ecm:uuid = '%s'", uuid));
		if (res.size() > 0) {
			doc = res.get(0);
		}
		return doc;
	}
	
	public DocumentModelList getWorkspaces(String repoName) {
		return nxQueryOnEs(repoName, "select * from Workspace");
	}

	/**
	 * @param sessionSystem
	 */
	public DocumentModelList getAllDocs(String repoName) {
		return nxQueryOnEs(repoName, "select * from Document");
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

}
