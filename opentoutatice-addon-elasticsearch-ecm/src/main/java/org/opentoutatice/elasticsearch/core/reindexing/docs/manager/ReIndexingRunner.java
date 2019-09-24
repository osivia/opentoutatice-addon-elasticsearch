/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.manager;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.elasticsearch.ElasticSearchConstants;
import org.nuxeo.runtime.api.Framework;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.config.OttcElasticSearchIndexOrAliasConfig;
import org.opentoutatice.elasticsearch.core.reindexing.docs.ReIndexingRunnerStep;
import org.opentoutatice.elasticsearch.core.reindexing.docs.ReIndexingRunnerStepState;
import org.opentoutatice.elasticsearch.core.reindexing.docs.ReIndexingRunnerStepStateStatus;
import org.opentoutatice.elasticsearch.core.reindexing.docs.constant.ReIndexingConstants;
import org.opentoutatice.elasticsearch.core.reindexing.docs.exception.IndexException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.cfg.ReIndexingConfig;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReindexingRecoverException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.name.IndexName;
import org.opentoutatice.elasticsearch.core.service.OttcElasticSearchAdminImpl;

/**
 * @author david
 *
 */
public class ReIndexingRunner {

	private static final Log log = LogFactory.getLog(ReIndexingRunner.class);

	private ReIndexingConfig reIndexingConfig;
	
	private ReIndexingRunnerStep runnerStep;
	
	private OttcElasticSearchAdminImpl esAdmin;

	private OttcElasticSearchIndexing esIndexing;

	private static final String REINDEX_REPOSITORY_QUERY = "select ecm:uuid from Document";

	public ReIndexingRunner(OttcElasticSearchIndexOrAliasConfig nxAliasCfg, OttcElasticSearchAdminImpl esAdmin,
			OttcElasticSearchIndexing esIndexing) {
		super();
		setReIndexingConfig(new ReIndexingConfig(nxAliasCfg));
		esAdmin(esAdmin).esIndexing(esIndexing);
	}

	public void run() throws ReIndexingException {
		
		try {
			final String repository = getRepository();
			
			setRunnerStep(ReIndexingRunnerStep.initialization);
			
			final OttcElasticSearchIndexOrAliasConfig nxAliasCfg = getNxAliasCfg(repository);
			final IndexName newIndex = getNewIndex(repository);
			final IndexName initialIndex = getInitialIndex(repository);
			
			OttcElasticSearchIndexOrAliasConfig newNxAliasCfg = null;
			
			try {
				setRunnerStep(ReIndexingRunnerStep.initialization.stepState(ReIndexingRunnerStepState.started.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));
				
				newNxAliasCfg = createNewEsIndex(newIndex, nxAliasCfg);
				getEsAdmin().initIndex(newNxAliasCfg, false);
	
				createNSwitchOnTransientAliases(initialIndex, newIndex);
				
				setRunnerStep( ReIndexingRunnerStep.initialization.stepState(ReIndexingRunnerStepState.done.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));
			} catch (ReIndexingException e) {
				manageReIndexingError(newIndex, getRunnerStep(), e);
			}
	
			try {
				setRunnerStep(ReIndexingRunnerStep.reIndexing.stepState(ReIndexingRunnerStepState.started.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));
				
				reIndex(repository, newIndex);
				
				setRunnerStep(ReIndexingRunnerStep.reIndexing.stepState(ReIndexingRunnerStepState.done.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));
			} catch (ReIndexingException e) {
				manageReIndexingError(newIndex, ReIndexingRunnerStep.reIndexing, e);
			}
	
			try {
				setRunnerStep(ReIndexingRunnerStep.switching.stepState(ReIndexingRunnerStepState.started.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));
				
				updateEsAlias(nxAliasCfg.getAliasName(), initialIndex, newIndex);
	
				updateEsFormerAlias(nxAliasCfg.getAliasName(), initialIndex);
				deleteTransientAliases(initialIndex, newIndex);
				
				setRunnerStep(ReIndexingRunnerStep.switching.stepState(ReIndexingRunnerStepState.done.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));
			} catch (ReIndexingException e) {
				manageReIndexingError(newIndex, ReIndexingRunnerStep.switching, e);
			}
		
		} finally {
			setRunnerStep(ReIndexingRunnerStep.none);
		}

	}
	

	/**
	 * @param e
	 * @throws ReIndexingException
	 */
	private void manageReIndexingError(IndexName newIndex, ReIndexingRunnerStep step, ReIndexingException e) throws ReIndexingException {
		ReindexingRecoverException re = null;
		step.getStepState().stepStatus(ReIndexingRunnerStepStateStatus.inError.error(e));
		
		try {
			ReIndexingErrorsHandler.get().restoreInitialEsState(step, newIndex);
		} catch (ReindexingRecoverException re_) {
			re = re_;
		}

		logReIndexingException(step, e, re);
		throw e;
	}
	

	// Initialization phase ============================

	protected OttcElasticSearchIndexOrAliasConfig createNewEsIndex(IndexName newIndexName,
			OttcElasticSearchIndexOrAliasConfig nxIndexNAliasCfg) throws ReIndexingException {
		try {
			return IndexNAliasManager.get().createNewIndex(newIndexName, nxIndexNAliasCfg);
		} catch (IndexException | InterruptedException | ExecutionException e) {
			throw new ReIndexingException(e);
		}
	}

	protected void createNSwitchOnTransientAliases(IndexName initialIndex, IndexName newIndex) throws ReIndexingException {
		IndexNAliasManager.get().createTransientAliases(initialIndex, newIndex);
	}

	// Re-indexing phase ============================

	protected void reIndex(String repository, IndexName newIndex) throws ReIndexingException {
		try {
			if (log.isDebugEnabled()) {
				log.debug("About to launch Zero Down Time indexing...");
			}

			// Launch asynchronous indexing
			// FIXME: are async exceptions throwned in calling thread??
			getEsIndexing().runReindexingWorker(repository, REINDEX_REPOSITORY_QUERY);

			if (log.isDebugEnabled()) {
				log.debug("Zero Down Time indexing launched.");
			}
			
			// Status
			setRunnerStep(ReIndexingRunnerStep.reIndexing.stepState(ReIndexingRunnerStepState.inProgress.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));

			// Wait for asynchronous indexing
			waitReIndexing();
		} catch (RuntimeException | InterruptedException e) {
			throw new ReIndexingException(e);
		}
	}

	protected void waitReIndexing() throws InterruptedException {
		// await timeout in ms
		long timeOut = 100;
		
		long loopWaitTime = Long.valueOf(Framework.getProperty(ReIndexingConstants.REINDEXING_WAIT_LOOP_TIME, "30")).longValue();
		TimeUnit unit = Framework.isTestModeSet() ? TimeUnit.MILLISECONDS : TimeUnit.SECONDS;

		long startTime = System.currentTimeMillis();
		if (log.isDebugEnabled()) {
			log.debug(String.format("Starting waiting for re-indexing at [%s] [check every %s %s]",
					String.valueOf(startTime), String.valueOf(loopWaitTime), StringUtils.lowerCase(String.valueOf(unit.toString()))));
		}

		WorkManager workManager = Framework.getService(WorkManager.class);
		boolean awaitCompletion = workManager.awaitCompletion(ElasticSearchConstants.INDEXING_QUEUE_ID, timeOut, unit);

		while (!awaitCompletion) {
			
			if(unit.equals(TimeUnit.SECONDS)) {
				loopWaitTime = loopWaitTime * 1000;
			}
			Thread.sleep(loopWaitTime);
			
			awaitCompletion = workManager.awaitCompletion(ElasticSearchConstants.INDEXING_QUEUE_ID, timeOut, unit);
			
			if(log.isTraceEnabled()) {
				log.trace(String.format("Await completed: [%s]", String.valueOf(awaitCompletion)));
			}
		}

		Validate.isTrue(awaitCompletion);
		
		// Status
		setRunnerStep(ReIndexingRunnerStep.reIndexing.stepState(ReIndexingRunnerStepState.done.stepStatus(ReIndexingRunnerStepStateStatus.successfull)));


		if (log.isDebugEnabled()) {
			long duration = System.currentTimeMillis() - startTime;
			if(unit.equals(TimeUnit.SECONDS) && duration > 0) {
				duration = duration/1000;
			}
			log.debug(String.format("End waiting: re-indexing done in [%s] %s", String.valueOf(duration), StringUtils.lowerCase(String.valueOf(unit.toString()))));
		}
	}

	// Switching phase ===========================

	private void updateEsAlias(String aliasName, IndexName initialIndex, IndexName newIndex)
			throws ReIndexingException {
		IndexNAliasManager.get().updateEsAlias(aliasName, initialIndex, newIndex);
	}

	private void updateEsFormerAlias(String aliasName, IndexName initialIndex) throws ReIndexingException {
		IndexNAliasManager.get().updateEsFormerAlias(aliasName, initialIndex);
	}

	private void deleteTransientAliases(IndexName initialIndex, IndexName newIndex) throws ReIndexingException {
		IndexNAliasManager.get().deleteTransientAliases(initialIndex, newIndex);
	}

	// Logs =======================================

	private void logReIndexingException(ReIndexingRunnerStep step, ReIndexingException e, ReindexingRecoverException re) {
		if (log.isErrorEnabled()) {
			StringBuffer msg = new StringBuffer(
					String.format("[Re-indexing INTERRUPTED during [%s] step]: ", step.toString()));

			switch (step) {
			case initialization:
				msg.append(String.format("[0 / %s] docs indexed.", "TODO: query on VCS (coresession?)"));
				break;
			case reIndexing:
				msg.append(String.format("[%s / %s] docs indexed.", "TODO: query in ES",
						"TODO: query on VCS (coresession?)"));
				break;
			case switching:
				msg.append(String.format("[%s / %s] docs indexed.", "TODO: query in ES",
						"TODO: query on VCS (coresession?)"));
				break;
			default:
				break;
			}
			
			// Error restoring Es state
			if(re != null) {
				msg.append("[Es state Error RECOVERY]: check Es status (aliases and indices must be in incoherent state).");
			} else {
				msg.append("[Es state RECOVERED]: Nx can still work on initial index.");
			}

			log.error(msg.toString());
		}
	}
	
	// Utility
	
	private OttcElasticSearchIndexOrAliasConfig getNxAliasCfg(String repository) {
		return getReIndexingConfig().getNxAliasCfg();
	}
	
	private IndexName getNewIndex(String repository) {
		return getReIndexingConfig().getNewIndex();
	}
	
	private IndexName getInitialIndex(String repository) {
		return getReIndexingConfig().getInitialIndex();
	}
	
	// Getters and Setters ========================
	
	private String getRepository() {
		Validate.notNull(getReIndexingConfig());
		Validate.notNull(getReIndexingConfig().getNxAliasCfg());
		return getReIndexingConfig().getNxAliasCfg().getRepositoryName();
	}

	public ReIndexingConfig getReIndexingConfig() {
		return reIndexingConfig;
	}
	
	private void setReIndexingConfig(ReIndexingConfig reIndexingCfg) {
		this.reIndexingConfig = reIndexingCfg;
	}

	public ReIndexingRunnerStep getRunnerStep() {
		return runnerStep;
	}

	private void setRunnerStep(ReIndexingRunnerStep runnerStep) {
		this.runnerStep = runnerStep;
	}

	public OttcElasticSearchAdminImpl getEsAdmin() {
		return this.esAdmin;
	}
	
	public ReIndexingRunner esAdmin(OttcElasticSearchAdminImpl esAdmin) {
		Validate.notNull(esAdmin);
		this.esAdmin = esAdmin;
		return this;
	}

	public OttcElasticSearchIndexing getEsIndexing() {
		return esIndexing;
	}

	public ReIndexingRunner esIndexing(OttcElasticSearchIndexing esIndexing) {
		Validate.notNull(esIndexing);
		this.esIndexing = esIndexing;
		return this;
	}

}
