package org.opentoutatice.elasticsearch.reindexing.docs;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.junit.Assert;
import org.junit.Test;
import org.nuxeo.ecm.automation.client.Session;
import org.nuxeo.ecm.automation.client.model.Document;
import org.nuxeo.ecm.automation.client.model.Documents;
import org.nuxeo.ecm.core.api.CoreInstance;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.elasticsearch.listener.ElasticSearchInlineListener;
import org.nuxeo.elasticsearch.query.NxQueryBuilder;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.transaction.TransactionHelper;
import org.opentoutatice.elasticsearch.OttcElasticSearchComponent;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.core.reindexing.docs.TransientIndexUse;
import org.opentoutatice.elasticsearch.core.reindexing.docs.constant.ReIndexingConstants;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception.EsStateCheckException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.IndexNAliasManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.query.filter.ReIndexingTransientAggregate;

import fr.toutatice.ecm.elasticsearch.automation.QueryES;

public class ZeroDownTimeReIndexingReadWriteTest extends ZeroDownTimeReIndexingTest {
	
	@Test
	public void testZeroDownTimeReadWriteDuringReIndexingFromCore() throws EsStateCheckException, ReIndexingException, InterruptedException, ExecutionException {
		String repoName = this.session.getRepositoryName();
		
		Assert.assertEquals(Boolean.FALSE, ReIndexingRunnerManager.get().isReIndexingInProgress(repoName));
		
		// Existing docs
		DocumentModelList workspaces = getWorkspaces(repoName);
		
		// Launch zero down time re-indexing with slow loop wait (cf ZeroDownTimeReadWriteConfigFeature)
		// (~ "suspend" re-indexing to test read-write)
		((OttcElasticSearchIndexing) this.esIndexing).reIndexAllDocumentsWithZeroDownTime(repoName);
		
		// Read - write test when re-indexing is suspending: =========
		Assert.assertEquals(Boolean.TRUE, ReIndexingRunnerManager.get().isReIndexingInProgress(repoName));
		
		// Write test -----------
		String indexOrAlias = ((OttcElasticSearchComponent) this.esAdmin).getIndexNameForRepository(repoName);
		Assert.assertEquals(TransientIndexUse.Write.getAlias(), indexOrAlias);
		
		boolean tx = false;
		DocumentModel createdWs = null;
		try {
			if (TransactionHelper.isTransactionActive()) {
				TransactionHelper.commitOrRollbackTransaction();
				tx = TransactionHelper.startTransaction();
			}
			DocumentModel wsToCreate = this.session.createDocumentModel("Workspace");
			createdWs = this.session.createDocument(wsToCreate);
			log.debug(String.format("[========== Writing test launched for [%s] ==========]", createdWs.getId()));
			// To fire indexing (sync for ws)

			ElasticSearchInlineListener.useSyncIndexing.set(true);
			this.session.save();
		} catch(Exception e) {
			TransactionHelper.setTransactionRollbackOnly();
		} finally {
			if(tx) {
				TransactionHelper.commitOrRollbackTransaction();
				TransactionHelper.startTransaction();
			}
		}
		
		// --------------------------
		
		// Read test -----------
		List<String> repos = new ArrayList<>();
		repos.add(repoName);
		String[] searchIndexes = ((OttcElasticSearchComponent) this.esAdmin).getElasticSearchAdmin().getSearchIndexes(repos);
		log.debug("Search indices: ");
		for(String sIdx : searchIndexes) {
			log.debug(sIdx);
		}
		
		List<String> readIndices = IndexNAliasManager.get().getIndicesOfAlias(TransientIndexUse.Read.getAlias());
		// There exist some empty or null values in list (?...)
		String[] indicesOfReadAlias = new String[readIndices.size()];
		log.debug(String.format("Indices of [%s] alias: ", TransientIndexUse.Read.getAlias()));
		int pos = 0;
		for(String idxOf : readIndices) {
			if(StringUtils.isNotBlank(idxOf)) {
				indicesOfReadAlias[pos] = idxOf;
				log.debug(idxOf);
				pos++;
			}
		}
		
		Assert.assertEquals(Boolean.TRUE, ArrayUtils.isEquals(searchIndexes, indicesOfReadAlias));
		
		// FIXME: cache?
		log.debug(String.format("[========== Reading test for [%s] ==========]", createdWs.getId()));
		DocumentModel reFoundWs = getDoc(repoName, createdWs.getId());
		log.debug("[========== =========== ==========]");
		Assert.assertNotNull(reFoundWs);
		
		DocumentModelList incrementedWorkspaces = getWorkspaces(repoName);
		Assert.assertEquals(incrementedWorkspaces.size(), workspaces.size() + 1);
		
		// ===========================================================
		
		// To be sure switch on new index is done
		waitReIndexing(repoName);
		Assert.assertEquals(Boolean.FALSE, ReIndexingRunnerManager.get().isReIndexingInProgress(repoName));
		
	}
	
	@Test
	public void testZeroDownTimeReadDuplicateDuringReIndexingFromAutomation() throws Exception {
		String repoName = this.session.getRepositoryName();
		final String ALL_DOCS_QUERY = "select * from Document";
		
		Assert.assertEquals(Boolean.FALSE, ReIndexingRunnerManager.get().isReIndexingInProgress(repoName));
		
		// Existing docs 
		Documents allDocsInfirstIndex = esQueryFromAutomation(ALL_DOCS_QUERY);
		
		// Launch zero down time re-indexing with slow loop wait (cf ZeroDownTimeReadWriteConfigFeature)
		// (~ "suspend" re-indexing to test read-write)
		launchReIndexingFromAutomation();
		
		Assert.assertEquals(Boolean.TRUE, ReIndexingRunnerManager.get().isReIndexingInProgress(repoName));
		
		// Check duplicates before end of suspend time 
		Long suspendTime = Long.valueOf(Framework.getProperty(ReIndexingConstants.REINDEXING_WAIT_LOOP_TIME));
		Thread.sleep(suspendTime.longValue() - 1000);
		
		// Query must have duplicates
		try(CoreSession session_ = CoreInstance.openCoreSessionSystem(repoName)) {
			NxQueryBuilder qBuilder = new NxQueryBuilder(session_);
			qBuilder.nxql(ALL_DOCS_QUERY);
			
			SearchResponse response = ((OttcElasticSearchComponent) this.esService).getEsService().search(qBuilder);
			StringTerms duplicateAggs = response.getAggregations().get(ReIndexingTransientAggregate.DUPLICATE_AGGREGATE_NAME);
			// Build duplicate list
			List<String> duplicateIds = new LinkedList<String>();
			
			for (Bucket bucket : duplicateAggs.getBuckets()) {
				if(bucket.getDocCount() > 1) {
					duplicateIds.add(bucket.getKey());
				}
			}
			
			Assert.assertEquals(Boolean.TRUE, duplicateIds.size() > 0);
		}
		
		Documents filteredDocs = esQueryFromAutomation("select * from Document");
		log.debug(String.format("Nb docs on one index: [%s] | Nb docs on twice: [%s]", allDocsInfirstIndex.size(), filteredDocs.size()));
		Assert.assertEquals(allDocsInfirstIndex.size(), filteredDocs.size());
		
		
		waitReIndexing(repoName);
		Assert.assertEquals(Boolean.FALSE, ReIndexingRunnerManager.get().isReIndexingInProgress(repoName));
		
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
			
			if(log.isTraceEnabled()) {
				log.debug("[Founded docs on Es]: ");
				for (Document document : docs) {
					log.debug(document.getPath());
				}
			}
		} finally {
			if(session_ != null) {
				session_.close();
			}
		}
		
		return docs;
	}


}
