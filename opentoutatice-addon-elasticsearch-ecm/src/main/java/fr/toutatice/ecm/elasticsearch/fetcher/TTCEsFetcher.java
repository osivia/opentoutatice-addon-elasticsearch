package fr.toutatice.ecm.elasticsearch.fetcher;

import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.impl.DocumentModelListImpl;
import org.nuxeo.elasticsearch.fetcher.Fetcher;

public class TTCEsFetcher extends Fetcher {

	public TTCEsFetcher(CoreSession session, SearchResponse response, Map<String, String> repoNames) {
		super(session, response, repoNames);
	}

	@Override
	public DocumentModelListImpl fetchDocuments() {
		return new DocumentModelListImpl();
	}
	
    public SearchResponse getResponse() {
        return super.getResponse();
    }
    
}
