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

import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.SortInfo;
import org.nuxeo.elasticsearch.fetcher.Fetcher;
import org.nuxeo.elasticsearch.query.NxQueryBuilder;

import fr.toutatice.ecm.elasticsearch.fetcher.TTCEsFetcher;

public class TTCNxQueryBuilder extends NxQueryBuilder {

    private static final String RCD_VARIABLES_PREFIX = "rcd:globalVariablesValues.";

    private static final String DC_TITLE = "dc:title";

    private static final String LOWERCASE_SUFFIX = ".lowercase";

	private Fetcher fetcher;
	
    /** Indicates if this builder is used from automation or from Nuxeo core. */
    private boolean automationCall = true;

    /**
     * Constructor.
     * 
     * @param coreSession
     */
	public TTCNxQueryBuilder(CoreSession coreSession) {
		super(coreSession);
	}
	
    /**
     * Gets Fetcher according to client calling (automation or Nuxeo core).
     */
	@Override
    public Fetcher getFetcher(SearchResponse response, Map<String, String> repoNames) {
        if (this.automationCall) {
            this.fetcher = new TTCEsFetcher(getSession(), response, repoNames);
        } else {
            this.fetcher = super.getFetcher(response, repoNames);
        }
        return this.fetcher;
    }

    @Override
    public SortBuilder[] getSortBuilders() {
        SortBuilder[] ret;
        if (getSortInfos().isEmpty()) {
            return new SortBuilder[0];
        }
        ret = new SortBuilder[getSortInfos().size()];
        int i = 0;
        for (SortInfo sortInfo : getSortInfos()) {
            ret[i++] = new FieldSortBuilder(sortInfo.getSortColumn()).order(sortInfo.getSortAscending() ? SortOrder.ASC : SortOrder.DESC);
        }
        return ret;
    }

	public SearchResponse getSearchResponse() {
	    if(this.fetcher != null){
	        return ((TTCEsFetcher) this.fetcher).getResponse();
	    }
	    return new SearchResponse(InternalSearchResponse.empty(), StringUtils.EMPTY, 0, 0, 0, null);
	}

    /**
     * @return the automationCall
     */
    public boolean isAutomationCall() {
        return automationCall;
    }

    /**
     * @param automationCall the automationCall to set
     * @return NxQueryBuilder
     */
    public NxQueryBuilder setAutomationCall(boolean automationCall) {
        this.automationCall = automationCall;
        return this;
    }

    @Override
    public boolean isFetchFromElasticsearch() {
        boolean is = true;
        if (!this.automationCall) {
            is = super.isFetchFromElasticsearch();
        }
        return is;
    }

    @Override
    public QueryBuilder makeQuery() {
        QueryBuilder esQueryBuilder = super.makeQuery();

        // Adapt order by when dc:title (for the moment)
        if (StringUtils.contains(getNxql().toLowerCase(), "order by")) {
            adaptSortInfos(getNxql());
        }

        return esQueryBuilder;
    }

    /**
     * Adapt field order when defined as lower case meta-field
     * in ES mapping.
     * 
     * @param nxql
     * @return
     */
    private void adaptSortInfos(String nxql) {
        ListIterator<SortInfo> listIterator = super.getSortInfos().listIterator();

        while (listIterator.hasNext()) {
            SortInfo sortInfo = listIterator.next();
            String sortColumn = sortInfo.getSortColumn();
            if (StringUtils.equalsIgnoreCase(DC_TITLE, sortColumn) || StringUtils.startsWith(sortColumn, RCD_VARIABLES_PREFIX)) {
                sortInfo.setSortColumn(sortColumn.concat(LOWERCASE_SUFFIX));
            }
        }
    }
	
}
