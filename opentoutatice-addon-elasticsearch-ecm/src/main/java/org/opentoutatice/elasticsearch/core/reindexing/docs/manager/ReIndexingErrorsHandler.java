/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.manager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opentoutatice.elasticsearch.core.reindexing.docs.ReIndexingRunnerStep;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReindexingRecoverException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.name.IndexName;

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
	
	public void restoreInitialEsState(ReIndexingRunnerStep indexingStep, Object... params) throws ReindexingRecoverException {
		if(log.isDebugEnabled()) {
			log.debug("About to retore initial Es state...");
		}
		
		try {
			switch (indexingStep) {
			case initialization:
				removeTransientAliases((IndexName) params[0], (IndexName) params[1]);
				deleteNewIndex((IndexName) params[1]);
				break;

			default:
				break;
			}
		} catch (ReIndexingException e) {
			throw new ReindexingRecoverException("[Retstoration of initial Es state INTERRUPTED]: ", e);
		}
		
		if(log.isDebugEnabled()) {
			log.debug("Initial Es state restored.");
		}
	}

	protected void removeTransientAliases(IndexName initialIndex, IndexName newIndex) throws ReIndexingException {
		IndexNAliasManager.get().deleteTransientAliases(initialIndex, newIndex);

	}

	private void deleteNewIndex(IndexName indexName) {
		IndexNAliasManager.get().deleteIndex(indexName.toString());
	}

}
