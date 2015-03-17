package fr.toutatice.ecm.elasticsearch.query;

import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.elasticsearch.fetcher.Fetcher;
import org.nuxeo.elasticsearch.query.NxQueryBuilder;

import fr.toutatice.ecm.elasticsearch.fetcher.TTCEsFetcher;

public class TTCNxQueryBuilder extends NxQueryBuilder {

	private Fetcher fetcher;
	
	public TTCNxQueryBuilder(CoreSession coreSession) {
		super(coreSession);
		
	}
	
	@Override
    public Fetcher getFetcher(SearchResponse response, Map<String, String> repoNames) {
		this.fetcher = new TTCEsFetcher(getSession(), response, repoNames);
        return this.fetcher;
    }

	public SearchResponse getSearchResponse() {
		return ((TTCEsFetcher) this.fetcher).getResponse();
	}
	
}
