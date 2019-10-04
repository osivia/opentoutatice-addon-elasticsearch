package org.opentoutatice.elasticsearch.api;

import org.nuxeo.elasticsearch.api.ElasticSearchIndexing;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception.ReIndexingStateException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception.ReIndexingStatusException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;

/**
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
public interface OttcElasticSearchIndexing extends ElasticSearchIndexing {

    /**
     * Re-index all documents of given repository in Elasticsearch with zero down time.
     *
     * @param repository
     * @return status of re-indexing
     * @throws ReIndexingException
     */
    public boolean reIndexAllDocumentsWithZeroDownTime(String repository) throws ReIndexingStatusException, ReIndexingStateException, ReIndexingException;

}
