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
 * mberhaut1
 *
 */
package fr.toutatice.ecm.elasticsearch.query;

import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.SortInfo;
import org.nuxeo.elasticsearch.fetcher.Fetcher;
import org.nuxeo.elasticsearch.query.NxQueryBuilder;
import org.nuxeo.elasticsearch.query.NxqlQueryConverter;
import org.opentoutatice.elasticsearch.fulltext.constants.FullTextConstants;

import fr.toutatice.ecm.elasticsearch.fetcher.TTCEsFetcher;

public class TTCNxQueryBuilder extends NxQueryBuilder {

	private static final Log log = LogFactory.getLog(TTCNxQueryBuilder.class);

	private static final String RCD_VARIABLES_PREFIX = "rcd:globalVariablesValues.";

	private static final String DC_TITLE = "dc:title";

	private static final String LOWERCASE_SUFFIX = ".lowercase";

	public static final String ORDER_BY = "order by";
	
	public static final String SCORE = "_score";

	private Fetcher fetcher;

	/** Indicates if this builder is used from automation or from Nuxeo core. */
	private boolean automationCall = true;

	protected CoreSession session;

	protected boolean isFullTextQuery;
	protected String originalNxqlfullTextClause;
	protected String fullTextTerms;
	protected String[] fullTextFields;

	/**
	 * Constructor.
	 *
	 * @param coreSession
	 */
	public TTCNxQueryBuilder(CoreSession coreSession) {
		super(coreSession);
		this.session = coreSession;
	}

	/**
	 * Gets Fetcher according to client calling (automation or Nuxeo core).
	 */
	@Override
	public Fetcher getFetcher(SearchResponse response, Map<String, String> repoNames) {
		if (this.automationCall) {
			this.fetcher = new TTCEsFetcher(this.getSession(), response, repoNames);
		} else {
			this.fetcher = super.getFetcher(response, repoNames);
		}
		return this.fetcher;
	}

	@Override
	public SortBuilder[] getSortBuilders() {
		SortBuilder[] ret;
		if (this.getSortInfos().isEmpty()) {
			return new SortBuilder[0];
		}
		ret = new SortBuilder[this.getSortInfos().size()];
		int i = 0;
		for (SortInfo sortInfo : this.getSortInfos()) {
			ret[i++] = new FieldSortBuilder(sortInfo.getSortColumn())
					.order(sortInfo.getSortAscending() ? SortOrder.ASC : SortOrder.DESC).ignoreUnmapped(true);
		}
		return ret;
	}

	public SortBuilder[] getSortBuilders(List<SortInfo> sortInfos) {
		SortBuilder[] ret;
		if (sortInfos.isEmpty()) {
			return new SortBuilder[0];
		}
		ret = new SortBuilder[sortInfos.size()];
		int i = 0;
		for (SortInfo sortInfo : sortInfos) {
			ret[i++] = new FieldSortBuilder(sortInfo.getSortColumn())
					.order(sortInfo.getSortAscending() ? SortOrder.ASC : SortOrder.DESC).ignoreUnmapped(true);
		}
		return ret;
	}

	public SearchResponse getSearchResponse() {
		if (this.fetcher != null) {
			return ((TTCEsFetcher) this.fetcher).getResponse();
		}
		return new SearchResponse(InternalSearchResponse.empty(), StringUtils.EMPTY, 0, 0, 0, null);
	}

	/**
	 * @return the automationCall
	 */
	public boolean isAutomationCall() {
		return this.automationCall;
	}

	/**
	 * @param automationCall the automationCall to set
	 * @return NxQueryBuilder
	 */
	public NxQueryBuilder setAutomationCall(boolean automationCall) {
		this.automationCall = automationCall;
		return this;
	}

	public boolean isFullTextQuery() {
		return isFullTextQuery;
	}

	public void setFullTextQuery(boolean isFullTextQuery) {
		this.isFullTextQuery = isFullTextQuery;
	}

	public String getOriginalNxqlfullTextClause() {
		return originalNxqlfullTextClause;
	}

	public void setOriginalNxqlfullTextClause(String originalNxqlfullTextQuery) {
		this.originalNxqlfullTextClause = originalNxqlfullTextQuery;
	}

	public String getFullTextTerms() {
		return fullTextTerms;
	}

	public void setFullTextTerms(String fullTextTerms) {
		this.fullTextTerms = fullTextTerms;
	}

	public String[] getFullTextFields() {
		return fullTextFields;
	}

	public void setFullTextFields(String[] fullTextFields) {
		this.fullTextFields = fullTextFields;
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

		if (this.getNxql() != null) {
			if (StringUtils.contains(this.getNxql().toLowerCase(), ORDER_BY)) {
				this.adaptSortInfos(this.getNxql());
			}
		}

		return esQueryBuilder;
	}

	@Override
	public void updateRequest(SearchRequestBuilder request) {
		super.updateRequest(request);

		if (isFullTextQuery()) {
			// Sort
			if (StringUtils.contains(this.getOriginalNxqlfullTextClause().toLowerCase(), ORDER_BY)) {
				for (SortBuilder sort : this
						.getSortBuilders(NxqlQueryConverter.getSortInfo(this.getOriginalNxqlfullTextClause()))) {
					request.addSort(sort);
				}
				request.addSort(new FieldSortBuilder(SCORE).order(SortOrder.DESC));
			}

			// Highlight
			addHighlight(request);

			// Suggest
			//addSuggest(request);
		}
	}

	/**
	 * Adapt field order when defined as lower case meta-field in ES mapping.
	 *
	 * @param nxql
	 * @return
	 */
	private void adaptSortInfos(String nxql) {
		ListIterator<SortInfo> listIterator = super.getSortInfos().listIterator();

		while (listIterator.hasNext()) {
			SortInfo sortInfo = listIterator.next();
			String sortColumn = sortInfo.getSortColumn();
			if (StringUtils.equalsIgnoreCase(DC_TITLE, sortColumn)
					|| StringUtils.startsWith(sortColumn, RCD_VARIABLES_PREFIX)) {
				sortInfo.setSortColumn(sortColumn.concat(LOWERCASE_SUFFIX));
			}
		}
	}

	protected SearchRequestBuilder addHighlight(SearchRequestBuilder request) {

		String[] fields = getFullTextFields();
		if (fields != null) {
			// Configure highlight
			request.setHighlighterPreTags(FullTextConstants.PRE_TAG).setHighlighterPostTags(FullTextConstants.POST_TAG);
			Integer fgtsSize = FullTextConstants.FGTS_SIZE != null ? FullTextConstants.FGTS_SIZE : null;
			Integer fgtsNb = FullTextConstants.FGTS_NB != null ? FullTextConstants.FGTS_NB : null;

			for (String field : fields) {
				if (fgtsSize != null && fgtsNb != null) {
					request.addHighlightedField(
							StringUtils.trim(StringUtils.substringBefore(field, FullTextConstants.UPPER)), fgtsSize,
							fgtsNb);
				} else if (fgtsSize != null && fgtsNb == null) {
					request.addHighlightedField(
							StringUtils.trim(StringUtils.substringBefore(field, FullTextConstants.UPPER)), fgtsSize);
				} else {
					request.addHighlightedField(
							StringUtils.trim(StringUtils.substringBefore(field, FullTextConstants.UPPER)));
				}
			}
		}

		return request;
	}

	protected SearchRequestBuilder addSuggest(SearchRequestBuilder request) {

		TermSuggestionBuilder termSuggestion = SuggestBuilder.termSuggestion("title").field("dc:title.fulltext")
				.maxEdits(1).text(getFullTextTerms());
		request.addSuggestion(termSuggestion);

		return request;
	}

}
