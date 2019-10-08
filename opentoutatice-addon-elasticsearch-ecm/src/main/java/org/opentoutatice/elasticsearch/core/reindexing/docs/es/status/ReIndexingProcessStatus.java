/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.es.status;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsState;
import org.opentoutatice.elasticsearch.core.reindexing.docs.index.IndexName;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.runner.step.ReIndexingRunnerStepStateStatus;

/**
 * @author david
 *
 */
public class ReIndexingProcessStatus {

    public static final String CONTRIBUTED_DOCS_DURING_REINDEXING_QUERY = "select * from Document where (dc:created >= datetime '%s') or (dc:modified >= datetime '%s')";

    protected static DecimalFormat decimalFormat = new DecimalFormat("##.###");

    private ReIndexingRunnerStepStateStatus status;

    private Date startTime;
    private long endTime;
    private float duration;

    private EsState esState;
    private IndexName newIndex;
    
    private long initialNbDocsInBdd;
    private long nbDocsInBdd;

    private long nbDocsInNewIndex;
    private long nbDocsInFormerIndex;

    private float averageReIndexingSpeed;

    private long nbCreatedDocsDuringReIndexing;
    private long nbModifiedDocsDuringReIndexing;
    private long nbDeletedDocsDuringReIndexing;
    
    private long nbContributedDocsNotIndexed;

    public ReIndexingProcessStatus build(String workId, String repository) {
        return ReIndexingProcessStatusBuilder.get().build(workId, repository);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer(); 

        sb.append(System.lineSeparator());
        sb.append("=============== ES Reindexing Process [DONE] ===============").append(System.lineSeparator());
        sb.append(String.format("Status: [%s]", this.getStatus().getMessage())).append(System.lineSeparator());
        sb.append(String.format("Duration: [%s] s", decimalFormat.format(this.getDuration()))).append(System.lineSeparator());
        sb.append(String.format("State: %s", this.getEsState() != null ? this.getEsState().toString() : "---")).append(System.lineSeparator());
        
        if(getNewIndex() != null) {
            sb.append(String.format("New index: [%s]", getNewIndex().toString())).append(System.lineSeparator());
            sb.append("Contributions during re-indexing:").append(System.lineSeparator());
            sb.append(String.format(" Created documents:    [%s]", getNbCreatedDocsDuringReIndexing())).append(System.lineSeparator());
            sb.append(String.format(" Modified documents:   [%s]", getNbModifiedDocsDuringReIndexing())).append(System.lineSeparator());
            sb.append(String.format(" Suppressed documents: [%s]", getNbDeletedDocsDuringReIndexing())).append(System.lineSeparator());
            
            sb.append(String.format("Number of indexed documents in new index: [%s] / [%s] documents in DBB", String.valueOf(this.getNbDocsInNewIndex()),
                    String.valueOf(this.getNbDocsInBdd()))).append(System.lineSeparator());
            
            if (ReIndexingRunnerStepStateStatus.inError.equals(this.getStatus())) {
                // Some contributions has done
                if(getNbCreatedDocsDuringReIndexing() + getNbModifiedDocsDuringReIndexing() > 0) {
                    // Error during indexing or switching step:
                    // Recovery mode has restored old index (or not id error during recovery)
                    // so creation & modification contributions are not present in old index - deletion can not be recovered
                    sb.append(String.format("(Creation & modification contributions are not present in restored current index. You can reindex them with query: %s )", String.format(CONTRIBUTED_DOCS_DURING_REINDEXING_QUERY,
                            ReIndexingProcessStatusBuilder.dateFormat.format(this.getStartTime()), ReIndexingProcessStatusBuilder.dateFormat.format(this.getStartTime())))).append(System.lineSeparator());
                }
            }
            
            sb.append(String.format("Average ReIndexing Speed: [%s] docs/s", decimalFormat.format(this.getAverageReIndexingSpeed())))
                    .append(System.lineSeparator());
        } else {
            // Error during initialization step
            sb.append("New index: [---]").append(System.lineSeparator());
        }

        if (ReIndexingRunnerStepStateStatus.inError.equals(this.getStatus())) {
            sb.append(String.format("===== CAUSE of [%s] ===== : ", ReIndexingRunnerStepStateStatus.inError.getMessage()));
        } else {
            sb.append("======================================================================");
        }

        return sb.toString();
    }

    // Getters & setters
    public ReIndexingRunnerStepStateStatus getStatus() {
        return this.status;
    }

    public void setStatus(ReIndexingRunnerStepStateStatus Status) {
        this.status = Status;
    }

    public Date getStartTime() {
        return this.startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return this.endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public float getDuration() {
        return this.duration;
    }

    public void setDuration(float duration) {
        this.duration = duration;
    }

    public EsState getEsState() {
        return this.esState;
    }

    public void setEsState(EsState esState) {
        this.esState = esState;
    }
    
    public IndexName getNewIndex() {
        return newIndex;
    }
    
    public void setNewIndex(IndexName newIndex) {
        this.newIndex = newIndex;
    }

    public long getNbDocsInBdd() {
        return this.nbDocsInBdd;
    }
    
    public long getInitialNbDocsInBdd() {
        return initialNbDocsInBdd;
    }
    
    public void setInitialNbDocsInBdd(long initialNbDocsInBdd) {
        this.initialNbDocsInBdd = initialNbDocsInBdd;
    }

    public void setNbDocsInBdd(long nbDocsInBdd) {
        this.nbDocsInBdd = nbDocsInBdd;
    }

    public long getNbDocsInNewIndex() {
        return this.nbDocsInNewIndex;
    }

    public void setNbDocsInNewIndex(long nbDocsInNewIndex) {
        this.nbDocsInNewIndex = nbDocsInNewIndex;
    }

    public long getNbDocsInFormerIndex() {
        return this.nbDocsInFormerIndex;
    }

    public void setNbDocsInFormerIndex(long nbDocsInFormerIndex) {
        this.nbDocsInFormerIndex = nbDocsInFormerIndex;
    }

    public float getAverageReIndexingSpeed() {
        return this.averageReIndexingSpeed;
    }

    public void setAverageReIndexingSpeed(float averageReIndexingSpeed) {
        this.averageReIndexingSpeed = averageReIndexingSpeed;
    }
    
    public long getNbCreatedDocsDuringReIndexing() {
        return nbCreatedDocsDuringReIndexing;
    }

    
    public void setNbCreatedDocsDuringReIndexing(long nbCreatedDocsDuringReIndexing) {
        this.nbCreatedDocsDuringReIndexing = nbCreatedDocsDuringReIndexing;
    }
    
    public long getNbModifiedDocsDuringReIndexing() {
        return nbModifiedDocsDuringReIndexing;
    }
    
    public void setNbModifiedDocsDuringReIndexing(long nbModifiedDocsDuringReIndexing) {
        this.nbModifiedDocsDuringReIndexing = nbModifiedDocsDuringReIndexing;
    }
    
    public long getNbDeletedDocsDuringReIndexing() {
        return nbDeletedDocsDuringReIndexing;
    }
    
    public void setNbDeletedDocsDuringReIndexing(long nbDeletedDocsDuringReIndexing) {
        this.nbDeletedDocsDuringReIndexing = nbDeletedDocsDuringReIndexing;
    }

    public long getNbContributedDocsNotIndexed() {
        return this.nbContributedDocsNotIndexed;
    }

    public void setNbContributedDocsNotIndexed(long nbContributedDocsNotIndexed) {
        this.nbContributedDocsNotIndexed = nbContributedDocsNotIndexed;
    }
    
}
