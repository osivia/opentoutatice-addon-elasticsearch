/*
 * (C) Copyright 2014 Académie de Rennes (http://www.ac-rennes.fr/), OSIVIA (http://www.osivia.com) and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 *
 * Contributors:
 *   mberhaut1
 *    
 */
package fr.toutatice.ecm.elasticsearch.query;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.internal.InternalSearchResponse;
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
	    if(this.fetcher != null){
	        return ((TTCEsFetcher) this.fetcher).getResponse();
	    }
	    return new SearchResponse(InternalSearchResponse.empty(), StringUtils.EMPTY, 0, 0, 0, null);
	}
	
    @Override
    public boolean isFetchFromElasticsearch() {
        return true;
    }
	
}
