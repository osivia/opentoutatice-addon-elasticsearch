/**
 *
 */
package org.opentoutatice.elasticsearch.core.service;

import static org.nuxeo.elasticsearch.ElasticSearchConstants.DOC_TYPE;

import java.util.List;
import java.util.regex.Matcher;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.api.IterableQueryResult;
import org.nuxeo.ecm.core.api.SortInfo;
import org.nuxeo.ecm.core.api.impl.DocumentModelListImpl;
import org.nuxeo.ecm.platform.query.api.Aggregate;
import org.nuxeo.ecm.platform.query.api.Bucket;
import org.nuxeo.elasticsearch.aggregate.AggregateEsBase;
import org.nuxeo.elasticsearch.api.EsResult;
import org.nuxeo.elasticsearch.core.EsResultSetImpl;
import org.nuxeo.elasticsearch.fetcher.Fetcher;
import org.nuxeo.elasticsearch.query.NxQueryBuilder;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.metrics.MetricsService;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchService;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.query.filter.ReIndexingTransientAggregate;
import org.opentoutatice.elasticsearch.fulltext.constants.FullTextConstants;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

/**
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
public class OttcElasticSearchServiceImpl implements OttcElasticSearchService {

    private static final Log log = LogFactory.getLog(OttcElasticSearchServiceImpl.class);

    private static final java.lang.String LOG_MIN_DURATION_FETCH_KEY = "org.nuxeo.elasticsearch.core.log_min_duration_fetch_ms";

    private static final long LOG_MIN_DURATION_FETCH_NS = Long.parseLong(Framework.getProperty(LOG_MIN_DURATION_FETCH_KEY, "200")) * 1000000;
    
    // Metrics
    protected final MetricRegistry registry = SharedMetricRegistries.getOrCreate(MetricsService.class.getName());

    protected final Timer searchTimer;

    protected final Timer fetchTimer;

    private final OttcElasticSearchAdminImpl esa;

    public OttcElasticSearchServiceImpl(OttcElasticSearchAdminImpl esa) {
        this.esa = esa;
        this.searchTimer = this.registry.timer(MetricRegistry.name("nuxeo", "elasticsearch", "service", "search"));
        this.fetchTimer = this.registry.timer(MetricRegistry.name("nuxeo", "elasticsearch", "service", "fetch"));
    }

    @Deprecated
    @Override
    public DocumentModelList query(CoreSession session, String nxql, int limit, int offset, SortInfo... sortInfos) throws ClientException {
        NxQueryBuilder query = new NxQueryBuilder(session).nxql(nxql).limit(limit).offset(offset).addSort(sortInfos);
        return this.query(query);
    }

    @Deprecated
    @Override
    public DocumentModelList query(CoreSession session, QueryBuilder queryBuilder, int limit, int offset, SortInfo... sortInfos) throws ClientException {
        NxQueryBuilder query = new NxQueryBuilder(session).esQuery(queryBuilder).limit(limit).offset(offset).addSort(sortInfos);
        return this.query(query);
    }

    @Override
    public DocumentModelList query(NxQueryBuilder queryBuilder) throws ClientException {
        return this.queryAndAggregate(queryBuilder).getDocuments();
    }

    @Override
    public EsResult queryAndAggregate(NxQueryBuilder queryBuilder) throws ClientException {
        SearchResponse response = this.search(queryBuilder);
        List<Aggregate> aggs = this.getAggregates(queryBuilder, response);
        if (queryBuilder.returnsDocuments()) {
            DocumentModelListImpl docs = this.getDocumentModels(queryBuilder, response);
            return new EsResult(docs, aggs);
        } else {
            IterableQueryResult rows = this.getRows(queryBuilder, response);
            return new EsResult(rows, aggs);
        }
    }

    protected DocumentModelListImpl getDocumentModels(NxQueryBuilder queryBuilder, SearchResponse response) {
        DocumentModelListImpl ret;
        long totalSize = response.getHits().getTotalHits();
        if (!queryBuilder.returnsDocuments() || (response.getHits().getHits().length == 0)) {
            ret = new DocumentModelListImpl(0);
            ret.setTotalSize(totalSize);
            return ret;
        }
        Context stopWatch = this.fetchTimer.time();
        Fetcher fetcher = queryBuilder.getFetcher(response, this.esa.getRepositoryMap());
        try {
            ret = fetcher.fetchDocuments();
        } finally {
            this.logMinDurationFetch(stopWatch.stop(), totalSize);
        }
        ret.setTotalSize(totalSize);
        return ret;
    }

    private void logMinDurationFetch(long duration, long totalSize) {
        if (log.isDebugEnabled() && (duration > LOG_MIN_DURATION_FETCH_NS)) {
            String msg = String.format("Slow fetch duration_ms:\t%.2f\treturning:\t%d documents", duration / 1000000.0, totalSize);
            if (log.isTraceEnabled()) {
                log.trace(msg, new Throwable("Slow fetch document stack trace"));
            } else {
                log.debug(msg);
            }
        }
    }

    protected List<Aggregate> getAggregates(NxQueryBuilder queryBuilder, SearchResponse response) {
        for (AggregateEsBase<? extends Bucket> agg : queryBuilder.getAggregates()) {
            InternalFilter filter = response.getAggregations().get(NxQueryBuilder.getAggregateFilterId(agg));
            if (filter == null) {
                continue;
            }
            MultiBucketsAggregation mba = filter.getAggregations().get(agg.getId());
            if (mba == null) {
                continue;
            }
            agg.parseEsBuckets(mba.getBuckets());
        }
        @SuppressWarnings("unchecked")
        List<Aggregate> ret = (List<Aggregate>) (List<?>) queryBuilder.getAggregates();
        return ret;
    }

    private IterableQueryResult getRows(NxQueryBuilder queryBuilder, SearchResponse response) {
        return new EsResultSetImpl(response, queryBuilder.getSelectFieldsAndTypes());
    }

    // Re-indexing FORK ===========================
    // protected-> public for tests
    public SearchResponse search(NxQueryBuilder query) {
        Context stopWatch = this.searchTimer.time();
        try {
            SearchRequestBuilder request = this.buildEsSearchRequest(query);
            // Highlight
            request = addHighlight(request, query);

            // For logs performance
            long startTime = System.currentTimeMillis();

            // FIXME: Duplicate re-indexing filter is managed, for the moment, for one repository configuration only
            if (query.getSearchRepositories().size() == 1) {
                try {
                    if (ReIndexingRunnerManager.get().isReIndexingInProgress(query.getSearchRepositories().get(0))) {
                        request = ReIndexingTransientAggregate.get().aggregateDuplicate(request, query.getLimit());
                    }
                } catch (InterruptedException e) {
                    if (log.isErrorEnabled()) {
                        log.error(e);
                    }
                }
            }

            if (log.isDebugEnabled()) {
                long duration = System.currentTimeMillis() - startTime;
                log.debug(String.format("#Add aggregate: [TE_%s_TE] ms", String.valueOf(duration)));
            }

            this.logSearchRequest(request, query);

            // For logs performance
            long startTime_ = System.currentTimeMillis();

            SearchResponse response = request.execute().actionGet();

            if (log.isDebugEnabled()) {
                long duration = System.currentTimeMillis() - startTime_;
                log.debug(String.format("#Es search: [TE_%s_TE] ms", String.valueOf(duration)));
            }

            this.logSearchResponse(response);
            return response;
        } finally {
            stopWatch.stop();
        }
    }

    protected SearchRequestBuilder addHighlight(SearchRequestBuilder request, NxQueryBuilder query) {
    	SearchRequestBuilder req = request;
    	if(query.getNxql() != null) {
    		Matcher matcher = FullTextConstants.FULLTEXT_QUERY_FIELDS.matcher(query.getNxql());
    		if(matcher.matches()) {
    			String[] fields = StringUtils.split(matcher.group(1), FullTextConstants.COMMA);
    			if(fields != null) {
    				// Configure highlight
    				request.setHighlighterPreTags(FullTextConstants.PRE_TAG).setHighlighterPostTags(FullTextConstants.POST_TAG);
    				Integer fgtsSize = FullTextConstants.FGTS_SIZE != null ? FullTextConstants.FGTS_SIZE : null;
    				Integer fgtsNb = FullTextConstants.FGTS_NB != null ? FullTextConstants.FGTS_NB : null;
    				for (String field : fields) {
    					if(fgtsSize != null && fgtsNb != null) {
    						request.addHighlightedField(StringUtils.trim(StringUtils.substringBefore(field, FullTextConstants.UPPER)), fgtsSize, fgtsNb);
    					} else if (fgtsSize != null && fgtsNb == null) {
    						request.addHighlightedField(StringUtils.trim(StringUtils.substringBefore(field, FullTextConstants.UPPER)), fgtsSize);
    					} else {
    						request.addHighlightedField(StringUtils.trim(StringUtils.substringBefore(field, FullTextConstants.UPPER)));
    					}
					}
    			}
    		}
    	}
		return req;
	}

	protected SearchRequestBuilder buildEsSearchRequest(NxQueryBuilder query) {
        SearchRequestBuilder request = this.esa.getClient().prepareSearch(this.esa.getSearchIndexes(query.getSearchRepositories())).setTypes(DOC_TYPE)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        query.updateRequest(request);
        if (query.isFetchFromElasticsearch()) {
            // fetch the _source without the binaryfulltext field
            request.setFetchSource(this.esa.getIncludeSourceFields(), this.esa.getExcludeSourceFields());
        }
        return request;
    }

    protected void logSearchResponse(SearchResponse response) {
        if (log.isTraceEnabled()) {
            log.trace("Response: " + response.toString());
        }
    }

    protected void logSearchRequest(SearchRequestBuilder request, NxQueryBuilder query) {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Search query: curl -XGET 'http://localhost:9200/%s/%s/_search?pretty' -d '%s'", this.getSearchIndexesAsString(query),
                    DOC_TYPE, request.toString()));
        }
    }

    protected String getSearchIndexesAsString(NxQueryBuilder query) {
        return StringUtils.join(this.esa.getSearchIndexes(query.getSearchRepositories()), ',');
    }

}
