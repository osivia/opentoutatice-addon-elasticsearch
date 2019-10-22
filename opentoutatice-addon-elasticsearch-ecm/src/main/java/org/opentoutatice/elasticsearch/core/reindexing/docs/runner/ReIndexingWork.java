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
import org.opentoutatice.elasticsearch.core.reindexing.docs.constant.ReIndexingConstants;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsState;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.status.ReIndexingProcessStatus;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.status.ReIndexingProcessStatusBuilder;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
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

    private EsState initialEsState;

    public ReIndexingWork(OttcElasticSearchIndexOrAliasConfig aliasCfg, OttcElasticSearchAdminImpl esAdmin, OttcElasticSearchIndexing esIndexing,
            EsState initialEsState) {
        this.setAliasCfg(aliasCfg);

        this.setEsAdmin(esAdmin);
        this.setEsIndexing(esIndexing);

        this.setInitialEsState(initialEsState);
    }

    @Override
    public String getTitle() {
        Validate.notNull(this.getAliasCfg());
        return String.format(REINDEXING_WORK_TITLE, this.getAliasCfg().getRepositoryName());
    }

    @Override
    public String getCategory() {
        return ReIndexingConstants.REINDEXING_MANAGER_QUEUE_ID;
    }

    @Override
    public void work() throws Exception {
        if (log.isInfoEnabled()) {
            logLaunchingInfos(this.getId(), getAliasCfg().getRepositoryName(), getInitialEsState());
        }
        try {
            // Start re-indexing runner
            new ReIndexingRunner(this.getId(), this.getAliasCfg(), this.getEsAdmin(), this.getEsIndexing(), this.getInitialEsState()).run();
        } finally {
            if (log.isInfoEnabled()) {
                logReIndexingEndStatus(this.getId());
            }
            cleanLogsInfos();
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

    public EsState getInitialEsState() {
        return initialEsState;
    }

    private void setInitialEsState(EsState initialEsState) {
        this.initialEsState = initialEsState;
    }

    protected void logReIndexingEndStatus(String workId) {
        ReIndexingProcessStatus processStatus = ReIndexingProcessStatusBuilder.get().build(workId, getAliasCfg().getRepositoryName());
        log.info(processStatus.toString());
    }

    // Logs =================================

    private void logLaunchingInfos(String workId, String repositoryName, EsState initialEsState) {
        StringBuffer sb = new StringBuffer(
                String.format("=============== ES Reindexing Process [LAUNCHED] for [%s] repository ===============", repositoryName));
        sb.append(System.lineSeparator());
        sb.append(String.format("State: %s", initialEsState != null ? initialEsState.toString() : "---")).append(System.lineSeparator());

        // For initial logs
        long initialNbDocsInBdd = ReIndexingProcessStatusBuilder.get().getNbDocsInBdd(repositoryName);
        // To be access for end status logs in work
        ReIndexingRunnerManager.get().setInitialNbDocsInBddFor(workId, initialNbDocsInBdd);

        sb.append(String.format("Number of documents in BDD to index: [%s] ", String.valueOf(initialNbDocsInBdd))).append(System.lineSeparator());

        log.info(sb.toString());
    }

    private void cleanLogsInfos() {
        ReIndexingRunnerManager.get().cleanLogsInfos();
    }

}
