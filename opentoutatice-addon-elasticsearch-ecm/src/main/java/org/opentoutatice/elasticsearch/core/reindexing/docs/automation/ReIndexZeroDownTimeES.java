/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.automation;

import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.core.api.impl.blob.StringBlob;
import org.nuxeo.elasticsearch.api.ElasticSearchIndexing;
import org.nuxeo.runtime.api.Framework;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception.EsStateCheckException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;

/**
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
@Operation(id = ReIndexZeroDownTimeES.ID, category = Constants.CAT_SERVICES, label = "Re-index all document of given repository in Elasticsearch with zero down time.")
public class ReIndexZeroDownTimeES {
	
	public static final String ID = "Documents.ReIndexZeroDownTimeES";
	
	/**
	 * Name of repository containing documents to re-index.
	 * If not set, takes repository 'default'.
	 */
	@Param(name = "repository", required = false)
	private String repository = "default";
	
	@OperationMethod
	public StringBlob run() throws Exception {
		String launchedStatus = null;
		try {
			OttcElasticSearchIndexing elasticSearchIndexing = (OttcElasticSearchIndexing) Framework.getService(ElasticSearchIndexing.class);
			
			if(elasticSearchIndexing.reIndexAllDocumentsWithZeroDownTime(getRepository())) {
				launchedStatus = String.format("Re-indexing launched for [%s] repository: check ${NX_LOGS}/zero-down-time-elasticsearch-reindexing.log", getRepository());
			} else {
				launchedStatus = String.format("Re-indexing process yet running for [%s] repository. You can not launch another process for the moment.", getRepository());
			}
		} catch(EsStateCheckException | ReIndexingException e) {
			launchedStatus = handleException(launchedStatus, e);
		}
		return new StringBlob(launchedStatus);
	}

	/**
	 * @param launchedStatus
	 * @param e
	 * @return
	 */
	private String handleException(String launchedStatus, Exception e) {
		StackTraceElement[] stackTrace = e.getStackTrace();
		if(stackTrace != null) {
			StringBuffer trace = new StringBuffer();
			for(StackTraceElement traceElement : stackTrace) {
				trace.append(traceElement.toString());
			}
			launchedStatus = String.format("[ERROR lauching re-indexing]: \r\n %s", trace.toString());
		}
		return launchedStatus;
	}
	
	public String getRepository() {
		return this.repository;
	}
	
}
