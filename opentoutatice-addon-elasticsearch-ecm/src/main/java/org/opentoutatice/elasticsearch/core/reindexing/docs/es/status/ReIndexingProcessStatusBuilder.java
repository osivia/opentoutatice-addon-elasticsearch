/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.es.status;

import java.text.SimpleDateFormat;
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
import org.opentoutatice.elasticsearch.core.reindexing.docs.index.IndexName;
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
    public static final String TOTAL_DOCS_IN_BDD_QUERY = "select ecm:uuid from Document";
    public static final String NB_CREATED_DOCS_DURING_REINDEXING = "select ecm:uuid from Document where dc:created >= TIMESTAMP '%s'";
    public static final String NB_MODIFIED_DOCS_DURING_REINDEXING = "select ecm:uuid from Document where dc:modified >= TIMESTAMP '%s'";
    
    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    private static ReIndexingProcessStatusBuilder instance;

    private ReIndexingProcessStatusBuilder() {
    };

    public static ReIndexingProcessStatusBuilder get() {
        if (instance == null) {
            instance = new ReIndexingProcessStatusBuilder();
        }
        return instance;
    }

    public ReIndexingProcessStatus build(String workId, String repository) {
        ReIndexingProcessStatus status = new ReIndexingProcessStatus();
        
        // Status
        status.setStatus(ReIndexingRunnerManager.get().getRunnerStepFor(workId).getStepState().getStepStatus());
        
        // Duration
        status.setStartTime(new Date(ReIndexingRunnerManager.get().getStartTimeFor(workId)));
        status.setEndTime(ReIndexingRunnerManager.get().getEndTimeFor(workId));
        status.setDuration(this.getDuration(status));
        
        // State
        try {
            status.setEsState(EsStateChecker.get().getEsState());
        } catch (InterruptedException | ExecutionException e) {
            // Nothing: do not block for logs
        }
        
        // Number of documents in DBB
        status.setInitialNbDocsInBdd(ReIndexingRunnerManager.get().getInitialNbDocsInBddFor(workId));
        status.setNbDocsInBdd(this.getNbDocsInBdd(repository));
        
        // Number of documents in new index
        status.setNewIndex(ReIndexingRunnerManager.get().getNewIndexFor(workId));
        status.setNbDocsInNewIndex(getNbDocsInNewIndex(status));
        // Average speed
        status.setAverageReIndexingSpeed(getAverageSpeed(status));
        
        // Contributions during re-indexing
        status.setNbCreatedDocsDuringReIndexing(getNbCreatedDocsDuringReIndexing(status, repository));
        status.setNbModifiedDocsDuringReIndexing(getNbModifiedDocsDuringReIndexing(status, repository));
        status.setNbDeletedDocsDuringReIndexing(getNbDeletedDocsDuringReIndexing(status));
        // Number of contributed documents not indexed
        status.setNbContributedDocsNotIndexed(this.getNbContributedDocsNotIndexed(status));

        return status;
    }

    /**
     * Re-indexing process duration in ms.
     */
    private float getDuration(ReIndexingProcessStatus status) {
        return (status.getEndTime() - status.getStartTime().getTime()) / (float) 1000;
    }

    public long getNbDocsInBdd(String repository) {
        return queryNFetch(TOTAL_DOCS_IN_BDD_QUERY, repository);
    }

    protected long getNbDocsInNewIndex(ReIndexingProcessStatus status) {
        long nb = 0;
        
        // New Index exists
        IndexName newIndex = status.getNewIndex();
        if(newIndex != null) {
            try {
                OttcElasticSearchAdmin esAdmin = (OttcElasticSearchAdmin) Framework.getService(ElasticSearchAdmin.class);
    
                CountResponse response = esAdmin.getClient().prepareCount(newIndex.toString())
                        .setQuery(NxqlQueryConverter.toESQueryBuilder(TOTAL_DOCS_IN_BDD_QUERY)).get();
    
                nb = response.getCount();
            } catch (Exception e) {
                // Nothing: do not block for logs
            }
        }

        return nb;
    }

    protected float getAverageSpeed(ReIndexingProcessStatus status) {
        return status.getDuration() > 0 ? status.getNbDocsInNewIndex() / status.getDuration() : 0;
    }
    
    protected long getNbCreatedDocsDuringReIndexing(ReIndexingProcessStatus status, String repository) {
        return queryNFetch(String.format(NB_CREATED_DOCS_DURING_REINDEXING, dateFormat.format(status.getStartTime())), repository);
    }
    
    protected long getNbModifiedDocsDuringReIndexing(ReIndexingProcessStatus status, String repository) {
        return queryNFetch(String.format(NB_MODIFIED_DOCS_DURING_REINDEXING, dateFormat.format(status.getStartTime())), repository);
    }
    
    protected long getNbDeletedDocsDuringReIndexing(ReIndexingProcessStatus status) {
        return status.getInitialNbDocsInBdd() + status.getNbCreatedDocsDuringReIndexing() - status.getNbDocsInBdd();
    }
    
    // FIXME: TODO with nb docs created, modified, after ... in bdd or with former alias ??? / TODO: deleted docs
    protected long getNbContributedDocsNotIndexed(ReIndexingProcessStatus status) {
        // Nb docs created or modfied: query
        // deleted docs: difference
        return status.getNbDocsInNewIndex() > 0 ? status.getNbDocsInBdd() - status.getNbDocsInNewIndex() : 0;
    }
    
    /**
     * @param repository
     * @return
     */
    protected long queryNFetch(String query, String repository) {
        long nb = 0;

        CoreSession session = null;
        IterableQueryResult rows = null;
        try {
            session = CoreInstance.openCoreSessionSystem(repository);
            rows = session.queryAndFetch(query, NXQL.NXQL, new Object[0]);
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

}
