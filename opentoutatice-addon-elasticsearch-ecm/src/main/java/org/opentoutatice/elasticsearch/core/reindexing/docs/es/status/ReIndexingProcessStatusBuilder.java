/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.es.status;

import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.count.CountResponse;
import org.nuxeo.ecm.core.api.CoreInstance;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.IterableQueryResult;
import org.nuxeo.ecm.core.query.sql.NXQL;
import org.nuxeo.elasticsearch.api.ElasticSearchAdmin;
import org.nuxeo.elasticsearch.query.NxqlQueryConverter;
import org.nuxeo.runtime.api.Framework;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchAdmin;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsStateChecker;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.RecoveringReIndexingException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.runner.step.ReIndexingRunnerStep;
import org.opentoutatice.elasticsearch.core.reindexing.docs.runner.step.ReIndexingRunnerStepStateStatus;

/**
 * @author david
 *
 */
public class ReIndexingProcessStatusBuilder {

    // FIXME: are versions indexed?
    // Note: COUNT(ecm:uuid) doesn't work???! -> return 0!
    private static final String TOTAL_DOCS_IN_BDD_QUERY = "select ecm:uuid from Document";

    private static ReIndexingProcessStatusBuilder instance;

    private ReIndexingProcessStatusBuilder() {
    };

    public static ReIndexingProcessStatusBuilder get() {
        if (instance == null) {
            instance = new ReIndexingProcessStatusBuilder();
        }
        return instance;
    }

    public ReIndexingProcessStatus build(String repository) {
        ReIndexingProcessStatus status = new ReIndexingProcessStatus();
        
        // Status
        status.setStatus(ReIndexingRunnerManager.get().getRunnerStepFor(repository).getStepState().getStepStatus());
        // Duration
        status.setStartTime(new Date(ReIndexingRunnerManager.get().getStartTimeFor(repository)));
        status.setEndTime(ReIndexingRunnerManager.get().getEndTimeFor(repository));
        status.setDuration(this.getDuration(status));
        // State
        try {
            status.setEsState(EsStateChecker.get().getEsState());
        } catch (InterruptedException | ExecutionException e) {
            // Nothing: do not block for logs
        }
        // Number of documents in DBB
        status.setNbDocsInBdd(this.getNbDocsInBdd(repository));
        // Number of documents in new index
        status.setNbDocsInNewIndex(this.getNbDocsInNewIndex(repository));
        // Average speed
        status.setAverageReIndexingSpeed(this.getAverageSpeed(status));
        // Number of contributed documents not indexed
        status.setNbContributedDocsNotIndexed(this.getNbContributedDocsNotIndexed(repository));

        return status;
    }

    /**
     * Re-indexing process duration in ms.
     */
    private float getDuration(ReIndexingProcessStatus status) {
        return (status.getEndTime() - status.getStartTime().getTime()) / (float) 1000;
    }

    protected long getNbDocsInBdd(String repository) {
        long nb = 0;

        CoreSession session = null;
        IterableQueryResult rows = null;
        try {
            session = CoreInstance.openCoreSessionSystem(repository);
            rows = session.queryAndFetch(TOTAL_DOCS_IN_BDD_QUERY, NXQL.NXQL, new Object[0]);
            nb = rows.size();
        } finally {
            if (rows != null) {
                rows.close();
            }

            if (session != null) {
                session.close();
            }
        }
        return nb;
    }

    protected long getNbDocsInNewIndex(String repository) {
        long nb = 0;
        
        try {
            OttcElasticSearchAdmin esAdmin = (OttcElasticSearchAdmin) Framework.getService(ElasticSearchAdmin.class);

            CountResponse response = esAdmin.getClient().prepareCount(ReIndexingRunnerManager.get().getLastIndexFor(repository).toString())
                    .setQuery(NxqlQueryConverter.toESQueryBuilder(TOTAL_DOCS_IN_BDD_QUERY)).get();

            nb = response.getCount();
        } catch (Exception e) {
            // Nothing: do not block for logs
        }

        return nb;
    }

    private float getAverageSpeed(ReIndexingProcessStatus status) {
        return status.getDuration() > 0 ? status.getNbDocsInNewIndex() / status.getDuration() : 0;
    }

    protected long getNbContributedDocsNotIndexed(String repository) {
        return this.getNbDocsInNewIndex(repository) > 0 ? this.getNbDocsInBdd(repository) - this.getNbDocsInNewIndex(repository) : 0;
    }

}
