/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.automation;

import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.core.api.impl.blob.StringBlob;
import org.nuxeo.elasticsearch.api.ElasticSearchIndexing;
import org.nuxeo.runtime.api.Framework;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;

/**
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
@Operation(id = ReIndexZeroDownTimeES.ID, category = Constants.CAT_SERVICES,
        label = "Re-index all document of given repository in Elasticsearch with zero down time.")
public class ReIndexZeroDownTimeES {

    public static final String ID = "Documents.ReIndexZeroDownTimeES";

    /**
     * Name of repository containing documents to re-index. If not set, takes
     * repository 'default'.
     */
    @Param(name = "repository", required = false)
    private String repository = "default";

    @OperationMethod
    public StringBlob run() throws Exception {
        String launchedStatus = null;
        OttcElasticSearchIndexing elasticSearchIndexing = (OttcElasticSearchIndexing) Framework.getService(ElasticSearchIndexing.class);

        if (elasticSearchIndexing.reIndexAllDocumentsWithZeroDownTime(this.getRepository())) {
            launchedStatus = String.format("Re-indexing launched for [%s] repository: check %s/zero-down-time-elasticsearch-reindexing.log",
                    this.getRepository(), Framework.getProperty("nuxeo.log.dir"));
        }

        return new StringBlob(launchedStatus);
    }

    public String getRepository() {
        return this.repository;
    }

}
