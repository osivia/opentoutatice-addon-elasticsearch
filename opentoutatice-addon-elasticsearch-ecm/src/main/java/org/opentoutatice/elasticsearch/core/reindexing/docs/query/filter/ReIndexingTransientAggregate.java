/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.query.filter;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;

/**
 * @author david
 *
 */
public class ReIndexingTransientAggregate {
	
	public static final String DUPLICATE_AGGREGATE_NAME = "duplicate_aggregate";
	public static final String DUPLICATE_AGGREGATE_FIELD = "ecm:uuid";
	
	private static ReIndexingTransientAggregate instance;
	
	private ReIndexingTransientAggregate() {};
	
	public static synchronized ReIndexingTransientAggregate get() {
		if(instance == null) {
			instance = new ReIndexingTransientAggregate();
		}
		return instance;
	}
	
	public SearchRequestBuilder aggregateDuplicate(final SearchRequestBuilder request, int nbBuckets) {
		
		// Group found documents by ecm:uuid (so one document by bucket in no duplicate, 2 if duplicates)
		// If duplicates in buckate, keep one originating from new index
		TermsBuilder termsBuilder = AggregationBuilders.terms(DUPLICATE_AGGREGATE_NAME).field("ecm:uuid");
		termsBuilder.size(nbBuckets);
		
		request.addAggregation(termsBuilder);
		
		return request;
	}

}
