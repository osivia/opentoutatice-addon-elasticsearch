/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.manager;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.helper.Validate;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.elasticsearch.api.ElasticSearchAdmin;
import org.nuxeo.elasticsearch.api.ElasticSearchIndexing;
import org.nuxeo.runtime.api.Framework;
import org.opentoutatice.elasticsearch.OttcElasticSearchComponent;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.config.OttcElasticSearchIndexOrAliasConfig;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsStateChecker;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception.EsStateCheckException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.runner.ReIndexingWork;
import org.opentoutatice.elasticsearch.core.service.OttcElasticSearchAdminImpl;

/**
 * Singleton launching re-indexing works (one work by repository at most).
 * 
 * @author dchevrier
 *
 */
public class ReIndexingRunnerManager {
	
	private static final Log log = LogFactory.getLog(ReIndexingRunnerManager.class);
	
	public static final String REINDEXING_QUEUE_ID = "zeroDownTimeEsReIndexing";
	
	private static final String DOC_TYPE = "doc";

	private IndexNAliasManager indexManager;
	
	private WorkManager workManager;

	private OttcElasticSearchAdminImpl esAdmin;
	private OttcElasticSearchIndexing esIndexing;

	private static ReIndexingRunnerManager instance;

	private ReIndexingRunnerManager() {
		super();

		setIndexManager(IndexNAliasManager.get());
		
		WorkManager workManager = Framework.getService(WorkManager.class);
		Validate.notNull(workManager);
		setWorkManager(workManager);
		
		OttcElasticSearchAdminImpl esAdmin = ((OttcElasticSearchComponent) Framework.getService(ElasticSearchAdmin.class)).getElasticSearchAdmin();
		Validate.notNull(esAdmin);
		setEsAdmin(esAdmin);
		
		OttcElasticSearchIndexing esIndexing = (OttcElasticSearchIndexing) Framework.getService(ElasticSearchIndexing.class);
		Validate.notNull(esAdmin);
		setEsIndexing(esIndexing);
	}

	public static synchronized ReIndexingRunnerManager get() {
		if (instance == null) {
			instance = new ReIndexingRunnerManager();
		}
		return instance;
	}

	/**
	 * Launches Es re-indexing with zero down time on given repository.
	 * 
	 * @param repository
	 * @return ReIndexingSteps: state and status of re-indexing step in progress when calling method
	 */
	public boolean reIndexWithZeroDownTime(String repository) throws EsStateCheckException, ReIndexingException {
		// Launched status
		boolean launchedStatus = false;

		synchronized (this) {
			OttcElasticSearchIndexOrAliasConfig aliasConfig = checkConfig(repository);

			if (isEsInInitialAllowedState(aliasConfig)) {
				// Launch re-indexing
				launchReIndexingRunner(aliasConfig);
				launchedStatus = true;
			} else {
				throw new EsStateCheckException(
						"ElasticSerach initial state is not allow for re-indexing: check state cluster.");
			}
		}

		return launchedStatus;
	}

	private OttcElasticSearchIndexOrAliasConfig checkConfig(String repository) throws ReIndexingException {
		// Find Es configuration associated with repository
		OttcElasticSearchIndexOrAliasConfig aliasCfg = null;
		try {
			String indexOrAliasName = getEsAdmin().getIndexNameForRepository(repository);
			aliasCfg = (OttcElasticSearchIndexOrAliasConfig) getEsAdmin().getIndexConfig().get(indexOrAliasName);
		} catch (Exception e) {
			throw new ReIndexingException(e);
		}
		
		if(aliasCfg == null) {
			throw new ReIndexingException(String.format("No Elasticsearch configuration for [%s] repository.", repository));
		} else if(!DOC_TYPE.equals(aliasCfg.getType())) {
			throw new ReIndexingException(String.format("Elasticsearch configuration for [%s] repository is of [%s] type: configuration must be of 'doc' type.", repository, aliasCfg.getType()));
		}
		
		return aliasCfg;
	}
	
	private boolean isEsInInitialAllowedState(OttcElasticSearchIndexOrAliasConfig aliasConfig) throws EsStateCheckException, ReIndexingException {
		boolean allowedState = false;
		
		if(!isReIndexingRunnerInProgressOn(aliasConfig.getRepositoryName())) {
			allowedState = EsStateChecker.get().aliasExistsWithOnlyOneIndex(aliasConfig.getAliasName()) && EsStateChecker.get().transientAliasesNotExist();
		}
		
		return allowedState;
	}
	
	// Just to catch and build ReIndexing Exception
	private boolean isReIndexingRunnerInProgressOn(String repository) throws ReIndexingException {
		boolean isInProgress = false;
		
		try {
			isInProgress = isReIndexingInProgress(repository);
		} catch(Exception e) {
			throw new ReIndexingException(e);
		}
		
		return isInProgress;
	}

	protected void launchReIndexingRunner(OttcElasticSearchIndexOrAliasConfig aliasCfg) {
		ReIndexingWork reIndexingWork = new ReIndexingWork(aliasCfg, getEsAdmin(), getEsIndexing());
		getWorkManager().schedule(reIndexingWork);
	}
	
	/**
	 * Runtime checker to allow, or not, use of transient aliases.
	 * 
	 * @param repositoryName
	 * @return
	 * @throws InterruptedException 
	 */
	public boolean isReIndexingInProgress(String repositoryName) throws InterruptedException {
		// Check at queue level for the moment 
		// but could look at work level (getWorkManager().getWorkState(workId))
		boolean inProgress = !getWorkManager().awaitCompletion(REINDEXING_QUEUE_ID, 100, TimeUnit.MILLISECONDS);
		
		if(log.isTraceEnabled()) {
			log.trace(String.format("Zero down time re-indexing in progress: [%s]", String.valueOf(inProgress)));
		}
		
		return inProgress; 
	}

	// Getters & Setters =====================
	
	public IndexNAliasManager getIndexManager() {
		return indexManager;
	}

	private void setIndexManager(IndexNAliasManager indexManager) {
		this.indexManager = indexManager;
	}

	public WorkManager getWorkManager() {
		return workManager;
	}

	private void setWorkManager(WorkManager workManager) {
		this.workManager = workManager;
	}

	public OttcElasticSearchAdminImpl getEsAdmin() {
		return esAdmin;
	}

	public void setEsAdmin(OttcElasticSearchAdminImpl esAdmin) {
		this.esAdmin = esAdmin;
	}

	public OttcElasticSearchIndexing getEsIndexing() {
		return esIndexing;
	}

	private void setEsIndexing(OttcElasticSearchIndexing esIndexing) {
		this.esIndexing = esIndexing;
	}

}
