/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.runner.works;

import java.util.List;

import org.nuxeo.elasticsearch.work.BucketIndexingWorker;
import org.nuxeo.elasticsearch.work.ScrollingIndexingWorker;
import org.opentoutatice.elasticsearch.core.reindexing.docs.constant.ReIndexingConstants;

/**
 * @author david
 */
public class ScrollingReIndexingWorker extends ScrollingIndexingWorker {

    public ScrollingReIndexingWorker(String repositoryName, String nxql) {
        super(repositoryName, nxql);
    }

    private static final long serialVersionUID = -1871761129531962566L;

    @Override
    public String getCategory() {
        return ReIndexingConstants.REINDEXING_QUEUE_ID;
    }

    @Override
    protected void scheduleBucketWorker(List<String> bucket, boolean isLast) {
        if (bucket.isEmpty()) {
            return;
        }
        BucketIndexingWorker subWorker = new BucketReIndexingWorker(repositoryName, bucket, isLast);
        getWorkManager().schedule(subWorker);
    }
}
