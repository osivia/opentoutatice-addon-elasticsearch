/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.runner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.helper.Validate;
import org.nuxeo.ecm.core.work.AbstractWork;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.config.OttcElasticSearchIndexOrAliasConfig;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.status.ReIndexingProcessStatus;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.status.ReIndexingProcessStatusBuilder;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.RecoveringReIndexingException;
import org.opentoutatice.elasticsearch.core.service.OttcElasticSearchAdminImpl;

/**
 * @author david
 *
 */
public class ReIndexingWork extends AbstractWork {

    private static final long serialVersionUID = 7736110665598024499L;

    private static final Log log = LogFactory.getLog(ReIndexingWork.class);

    private static final String REINDEXING_WORK_TITLE = "zero-down-time-re-indexing-%s";

    private OttcElasticSearchIndexOrAliasConfig aliasCfg;

    private OttcElasticSearchAdminImpl esAdmin;
    private OttcElasticSearchIndexing esIndexing;

    public ReIndexingWork(OttcElasticSearchIndexOrAliasConfig aliasCfg, OttcElasticSearchAdminImpl esAdmin, OttcElasticSearchIndexing esIndexing) {
        this.setAliasCfg(aliasCfg);

        this.setEsAdmin(esAdmin);
        this.setEsIndexing(esIndexing);
    }

    @Override
    public String getTitle() {
        Validate.notNull(this.getAliasCfg());
        return String.format(REINDEXING_WORK_TITLE, this.getAliasCfg().getRepositoryName());
    }

    @Override
    public String getCategory() {
        return ReIndexingRunnerManager.REINDEXING_QUEUE_ID;
    }

    @Override
    public void work() throws Exception {
        try {
            // Start re-indexing runner
            new ReIndexingRunner(this.getAliasCfg(), this.getEsAdmin(), this.getEsIndexing()).run();
        } finally {
            this.logReIndexingEndStatus();
        }
    }

    public OttcElasticSearchIndexOrAliasConfig getAliasCfg() {
        return this.aliasCfg;
    }

    public void setAliasCfg(OttcElasticSearchIndexOrAliasConfig aliasCfg) {
        this.aliasCfg = aliasCfg;
    }

    public OttcElasticSearchAdminImpl getEsAdmin() {
        return this.esAdmin;
    }

    private void setEsAdmin(OttcElasticSearchAdminImpl esAdmin) {
        this.esAdmin = esAdmin;
    }

    public OttcElasticSearchIndexing getEsIndexing() {
        return this.esIndexing;
    }

    private void setEsIndexing(OttcElasticSearchIndexing esIndexing) {
        this.esIndexing = esIndexing;
    }

    protected void logReIndexingEndStatus() {
        ReIndexingProcessStatus processStatus = ReIndexingProcessStatusBuilder.get().build(this.getAliasCfg().getRepositoryName());
        log.info(processStatus.toString());

    }

}
