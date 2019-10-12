/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.runner.works;

import java.util.List;

import org.nuxeo.elasticsearch.work.BucketIndexingWorker;
import org.opentoutatice.elasticsearch.core.reindexing.docs.constant.ReIndexingConstants;


/**
 * @author david
 */
public class BucketReIndexingWorker extends BucketIndexingWorker {

    private static final long serialVersionUID = -1168775359128801334L;

    public BucketReIndexingWorker(String repositoryName, List<String> docIds, boolean warnAtEnd) {
        super(repositoryName, docIds, warnAtEnd);
    }

    @Override
    public String getCategory() {
        return ReIndexingConstants.REINDEXING_QUEUE_ID;
    }

}
