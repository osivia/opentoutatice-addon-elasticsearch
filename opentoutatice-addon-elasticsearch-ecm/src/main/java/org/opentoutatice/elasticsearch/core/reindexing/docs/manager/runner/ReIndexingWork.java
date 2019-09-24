/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.manager.runner;

import org.jsoup.helper.Validate;
import org.nuxeo.ecm.core.work.AbstractWork;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.config.OttcElasticSearchIndexOrAliasConfig;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunner;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.core.service.OttcElasticSearchAdminImpl;

/**
 * @author david
 *
 */
public class ReIndexingWork extends AbstractWork {
	
	private static final long serialVersionUID = 7736110665598024499L;
	
	private static final String REINDEXING_WORK_TITLE = "zero-down-time-re-indexing-%s";
	
	private OttcElasticSearchIndexOrAliasConfig aliasCfg;
	
	private OttcElasticSearchAdminImpl esAdmin;
	private OttcElasticSearchIndexing esIndexing;
	
	public ReIndexingWork(OttcElasticSearchIndexOrAliasConfig aliasCfg, OttcElasticSearchAdminImpl esAdmin,
			OttcElasticSearchIndexing esIndexing) {
		setAliasCfg(aliasCfg);
		
		setEsAdmin(esAdmin);
		setEsIndexing(esIndexing);
	}

	@Override
	public String getTitle() {
		Validate.notNull(getAliasCfg());
		return String.format(REINDEXING_WORK_TITLE, getAliasCfg().getRepositoryName());
	}
	
	@Override
    public String getCategory() {
        return ReIndexingRunnerManager.REINDEXING_QUEUE_ID;
    }

	@Override
	public void work() throws Exception {
		// Start re-indexing runner
		new ReIndexingRunner(getAliasCfg(), getEsAdmin(), getEsIndexing()).run();
	}

	public OttcElasticSearchIndexOrAliasConfig getAliasCfg() {
		return aliasCfg;
	}

	public void setAliasCfg(OttcElasticSearchIndexOrAliasConfig aliasCfg) {
		this.aliasCfg = aliasCfg;
	}

	public OttcElasticSearchAdminImpl getEsAdmin() {
		return esAdmin;
	}

	private void setEsAdmin(OttcElasticSearchAdminImpl esAdmin) {
		this.esAdmin = esAdmin;
	}

	public OttcElasticSearchIndexing getEsIndexing() {
		return esIndexing;
	}

	private void setEsIndexing(OttcElasticSearchIndexing esIndexing) {
		this.esIndexing = esIndexing;
	}

}
