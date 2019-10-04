/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.es.status;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsState;
import org.opentoutatice.elasticsearch.core.reindexing.docs.runner.step.ReIndexingRunnerStepStateStatus;

/**
 * @author david
 *
 */
public class ReIndexingProcessStatus {

    public static final String CONTRIBUTED_DOCS_DURING_REINDEXING_QUERY = "select count(ecm:uuid) from Document where (dc:created >= datetime '%s') or (dc:modified >= datetime '%s')";

    protected static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    protected static DecimalFormat decimalFormat = new DecimalFormat("##.###");

    private ReIndexingRunnerStepStateStatus status;

    private Date startTime;

    private long endTime;

    private float duration;

    private EsState esState;

    private long nbDocsInBdd;

    private long nbDocsInNewIndex;

    private long nbDocsInFormerIndex;

    private float averageReIndexingSpeed;

    private long nbContributedDocsNotIndexed;

    public ReIndexingProcessStatus build(String repository) {
        return ReIndexingProcessStatusBuilder.get().build(repository);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();

        sb.append(System.lineSeparator());
        sb.append("=============== ES Reindexing Process [DONE] ===============").append(System.lineSeparator());
        sb.append(String.format("Status: [%s]", this.getStatus().getMessage())).append(System.lineSeparator());
        sb.append(String.format("Duration: [%s] s", decimalFormat.format(this.getDuration()))).append(System.lineSeparator());
        sb.append(String.format("State: %s", this.getEsState() != null ? this.getEsState().toString() : "---")).append(System.lineSeparator());
        sb.append(String.format("Number of indexed documents: [%s] / [%s] documents in DBB", String.valueOf(this.getNbDocsInNewIndex()),
                String.valueOf(this.getNbDocsInBdd()))).append(System.lineSeparator());
        sb.append(String.format("Average ReIndexing Speed: [%s] docs/s", decimalFormat.format(this.getAverageReIndexingSpeed())))
                .append(System.lineSeparator());

        if (ReIndexingRunnerStepStateStatus.inError.equals(this.getStatus())) {
            sb.append(String.format("Number of contributed documents not indexed: [%s]     ", String.valueOf(this.getNbContributedDocsNotIndexed())));
            if (this.getNbContributedDocsNotIndexed() > 0) {
                sb.append(String.format("(You can reindex them on former index with query: %s )", String.format(CONTRIBUTED_DOCS_DURING_REINDEXING_QUERY,
                        dateFormat.format(this.getStartTime()), dateFormat.format(this.getStartTime()))));
            }
            sb.append(System.lineSeparator());
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

    public long getNbDocsInBdd() {
        return this.nbDocsInBdd;
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

    public long getNbContributedDocsNotIndexed() {
        return this.nbContributedDocsNotIndexed;
    }

    public void setNbContributedDocsNotIndexed(long nbContributedDocsNotIndexed) {
        this.nbContributedDocsNotIndexed = nbContributedDocsNotIndexed;
    }

}
