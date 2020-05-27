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
package fr.toutatice.ecm.elasticsearch.automation;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.nuxeo.ecm.automation.OperationContext;
import org.nuxeo.ecm.automation.OperationException;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.automation.jaxrs.DefaultJsonAdapter;
import org.nuxeo.ecm.automation.jaxrs.JsonAdapter;
import org.nuxeo.ecm.automation.jaxrs.io.documents.JsonDocumentWriter;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.NuxeoPrincipal;
import org.nuxeo.ecm.core.schema.SchemaManager;
import org.nuxeo.ecm.core.schema.types.Schema;
import org.nuxeo.ecm.core.security.SecurityService;
import org.nuxeo.elasticsearch.api.ElasticSearchAdmin;
import org.nuxeo.elasticsearch.api.ElasticSearchService;
import org.nuxeo.elasticsearch.query.NxqlQueryConverter;
import org.nuxeo.runtime.api.Framework;
import org.opentoutatice.elasticsearch.OttcElasticSearchComponent;
import org.opentoutatice.elasticsearch.fulltext.constants.FullTextConstants;

import fr.toutatice.ecm.elasticsearch.helper.SQLHelper;
import fr.toutatice.ecm.elasticsearch.query.TTCNxQueryBuilder;
import fr.toutatice.ecm.elasticsearch.search.TTCSearchResponse;
import net.sf.json.JSONObject;

@Operation(id = QueryES.ID, category = Constants.CAT_FETCH, label = "Query via ElasticSerach", description = "Perform a query on ElasticSerach instead of Repository")
public class QueryES {

	private static final Log log = LogFactory.getLog(QueryES.class);

	public static final String ID = "Document.QueryES";

	protected static final Integer DEFAULT_MAX_SIZE_RESULTS = Integer
			.valueOf(Framework.getProperty("ottc.es.query.default.limit", "1000")) + 1;

	protected static final int OLD_DEFAULT_MAX_SIZE_RESULTS = 10000;
	
	public static enum QueryLanguage {
		NXQL, ES;
	}

	@Context
	protected CoreSession session;

	@Context
	protected OperationContext ctx;

	@Context
	protected ElasticSearchService elasticSearchService;

	@Context
	protected ElasticSearchAdmin elasticSearchAdmin;

	@Context
	protected SchemaManager schemaManager;

	@Param(name = "query", required = true)
	protected String query;

	@Param(name = "queryLanguage", required = false, description = "Language of the query parameter : NXQL or ES.", values = {
			"NXQL", "ES" })
	protected String queryLanguage = QueryLanguage.NXQL.name();

	@Param(name = "pageSize", required = false)
	protected Integer pageSize;

	@Param(name = "currentPageIndex", required = false)
	protected Integer currentPageIndex;

	@Deprecated
	@Param(name = "page", required = false)
	// For Document.PageProvider only: to remove later
	protected Integer page;

	@Param(name = "X-NXDocumentProperties", required = false)
	protected String nxProperties;

	@OperationMethod
	public JsonAdapter run() throws OperationException {
		try {
			switch (QueryLanguage.valueOf(this.queryLanguage)) {
			case NXQL:
				return this.runNxqlSearch();
			case ES:
				return this.runEsSearch();
			default:
				throw new OperationException(
						"Illegal argument value for parameter 'queryLanguage' : " + this.queryLanguage);
			}
		} catch (final IllegalArgumentException e) {
			throw new OperationException("Illegal argument value for parameter 'queryLanguage' : " + this.queryLanguage,
					e);
		}
	}

	@OperationMethod
	public JsonAdapter runNxqlSearch() throws OperationException {
		// For performance logs
		long startTime = System.currentTimeMillis();
		if (log.isDebugEnabled()) {
			log.debug(String.format("NXQL Query: [%s]", this.query));
		}

		// Query builder
		TTCNxQueryBuilder queryBuilder = this.getNxQueryBuilder();
		SearchResponse esResponse = this.nxqlSearch(queryBuilder);

		return responseAsJson(startTime, queryBuilder, esResponse);
	}

	private JsonAdapter responseAsJson(long startTime, TTCNxQueryBuilder queryBuilder, SearchResponse esResponse) {
		
		TTCSearchResponse response;
		// Bound results
		long hits = esResponse.getHits().getHits().length;
		if (hits > DEFAULT_MAX_SIZE_RESULTS) {
			// Monitoring
			if (log.isInfoEnabled()) {
				// Current principal name
				Principal principal = this.session.getPrincipal();
				String principalName = (principal != null) && (principal.getName() != null) ? principal.getName()
						: "null";

				log.info(String.format("[%s][hits: %s][limit: %s][%s]", principalName, String.valueOf(hits),
						String.valueOf(queryBuilder.getLimit()), this.query));
			}

			// Empty response
			SearchResponse emptyResponse = new SearchResponse(InternalSearchResponse.empty(), StringUtils.EMPTY, 0, 0,
					0, new ShardSearchFailure[0]);
			response = new TTCSearchResponse(emptyResponse, 0, 0, null);
		} else {
			// Compat mode
			String schemas = this.nxProperties;
			if (this.nxProperties == null) {
				schemas = this.getSchemasFromHeader(this.ctx);
			}

			// Response
			response = new TTCSearchResponse(esResponse, this.pageSize, this.currentPageIndex,
					this.formatSchemas(schemas));
		}

		// Response builder
		JsonAdapter responseAsJson = new DefaultJsonAdapter(response);

		if (log.isDebugEnabled()) {
			long duration = System.currentTimeMillis() - startTime;
			log.debug(String.format("#runNxqlSearch: [TA_%s_TA] ms ", String.valueOf(duration)));
		}

		return responseAsJson;
	}

	protected SearchResponse nxqlSearch(TTCNxQueryBuilder builder) throws OperationException {
		
		if(isFullTextQuery(this.query)) {
			builder.setFullTextQuery(true);
			builder.esQuery(getFullTextEsQueryBuilder(FullTextConstants.FULLTEXT_QUERY_FIELDS.matcher(this.query), builder));
		} else {
			builder.nxql(SQLHelper.getInstance().escape(this.query));
		}
		return paginatedSearchResponse(builder);
		
	}

	protected SearchResponse paginatedSearchResponse(TTCNxQueryBuilder builder) {
		// Compat mode
		Integer currentPageIndex = this.currentPageIndex;
		if (this.currentPageIndex == null) {
			currentPageIndex = this.page;
		}

		if ((null != currentPageIndex) && (null != this.pageSize)) {
			builder.offset((0 <= currentPageIndex ? currentPageIndex : 0) * this.pageSize);
			builder.limit(this.pageSize);
		} else {
			builder.limit(OLD_DEFAULT_MAX_SIZE_RESULTS);
		}

		this.elasticSearchService.query(builder);
		return builder.getSearchResponse();
	}

	protected TTCNxQueryBuilder getNxQueryBuilder() {
		return new TTCNxQueryBuilder(this.session);
	}

	/**
	 * Gets schemas from Header.
	 *
	 * @param ctx
	 * @return schemas
	 */
	// TODO: to remove when client ES query will send schema in header
	public String getSchemasFromHeader(OperationContext ctx) {

		String schemas;

		if ("transaction".equals(ctx.get("contextType"))) {
			schemas = (String) ctx.get(JsonDocumentWriter.DOCUMENT_PROPERTIES_HEADER);
		} else {
			HttpServletRequest httpRequest = (HttpServletRequest) ctx.get("request");
			schemas = httpRequest.getHeader(JsonDocumentWriter.DOCUMENT_PROPERTIES_HEADER);
		}
		return !StringUtils.equals("*", schemas) ? schemas : null;
	}
	
	protected boolean isFullTextQuery(String query) {
		return FullTextConstants.FULLTEXT_QUERY_INDICATOR.matcher(query).matches();
	}
	
	protected QueryBuilder getFullTextEsQueryBuilder(Matcher matcher, TTCNxQueryBuilder builder) throws OperationException {
		// Result
		BoolQueryBuilder esBoolQueryBuilder = QueryBuilders.boolQuery();
		
		String[] fields = ((OttcElasticSearchComponent) this.elasticSearchAdmin).getElasticSearchAdmin().getFullTextFields();
		// To get groups
		matcher.matches();
		if (matcher.group(2) != null) {
			fields = StringUtils.split(StringUtils.trim(matcher.group(3)), FullTextConstants.COMMA);
		}
		// Store for highlight
		builder.setFullTextFields(fields);

		String terms = matcher.group(4);
		if (terms != null) {
			builder.setFullTextTerms(terms);
			
			MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(FullTextConstants.EXACT_FIELD_TITLE, terms)
					.operator(MatchQueryBuilder.Operator.OR)
					.boost(2F);
			
			MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery(terms, fields)
					.operator(MatchQueryBuilder.Operator.OR);
			
			MultiMatchQueryBuilder multiMatchFuzzyQuery = QueryBuilders.multiMatchQuery(terms, fields)
				.operator(MatchQueryBuilder.Operator.OR)
				.fuzziness(FullTextConstants.FUZZINESS)
				.maxExpansions(FullTextConstants.FUZZINESS_MAX_EXPANSIONS)
				.prefixLength(FullTextConstants.FUZZINESS_PREFIX_LENGTH);
			
			FunctionScoreQueryBuilder functionScoreQueryFuzzy = QueryBuilders.functionScoreQuery(multiMatchFuzzyQuery);
			functionScoreQueryFuzzy.add(ScoreFunctionBuilders.factorFunction(0.01F));
			
			esBoolQueryBuilder.must(QueryBuilders.boolQuery()
					.should(matchQuery).should(multiMatchQuery).should(functionScoreQueryFuzzy));
			

			QueryBuilder filterBuilder = null;
			String clause = matcher.group(6);
			if (clause != null) {
				builder.setOriginalNxqlfullTextClause(clause);
				
				filterBuilder = NxqlQueryConverter.toESQueryBuilder(clause);
				esBoolQueryBuilder.must(filterBuilder);
			}

		} else {
			throw new OperationException(String.format("Missing Full Text terms in query: %s", query));
		}

		return esBoolQueryBuilder;
	}

	protected List<String> formatSchemas(String nxProperties) {
		List<String> schemas = new ArrayList<String>();

		if (StringUtils.isNotBlank(nxProperties)) {
			String[] schemasList = nxProperties.split(",");
			for (String schema : schemasList) {
				Schema sch = this.schemaManager.getSchema(StringUtils.trim(schema));
				if (null != sch) {
					String prefix = sch.getNamespace().prefix;
					schemas.add(StringUtils.isNotBlank(prefix) ? prefix : sch.getName());
				} else {
					if (log.isDebugEnabled()) {
						log.debug("Unknown schema '" + schema + "' (query='" + this.query + "')");
					}
				}
			}
		}

		return schemas;
	}
	

	protected JsonAdapter runEsSearch() throws OperationException {

		final SearchRequestBuilder request = this.elasticSearchAdmin.getClient()
				.prepareSearch(this.elasticSearchAdmin.getIndexNameForRepository(this.session.getRepositoryName()))
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		request.setSource(this.getESRequestPayload());

		if (log.isDebugEnabled()) {
			log.debug(String.format("Search query: curl -XGET 'http://localhost:9200/%s/_search?pretty' -d '%s'",
					this.elasticSearchAdmin.getIndexNameForRepository(this.session.getRepositoryName()),
					request.toString()));
		}
		try {
			final SearchResponse esResponse = request.get();

			if (log.isDebugEnabled()) {
				log.debug("Result: " + esResponse.toString());
			}

			return new DefaultJsonAdapter(esResponse);
		} catch (final ElasticsearchException e) {
			throw new OperationException("Error while executing the ElasticSearch request", e);
		}
	}

	public String getESRequestPayload() {
		final Principal principal = this.session.getPrincipal();
		if ((principal == null)
				|| ((principal instanceof NuxeoPrincipal) && ((NuxeoPrincipal) principal).isAdministrator())) {
			return this.query;
		}

		final String[] principals = SecurityService.getPrincipalsToCheck(principal);
		final JSONObject payloadJson = JSONObject.fromObject(this.query);
		JSONObject query;
		if (payloadJson.has("query")) {
			query = payloadJson.getJSONObject("query");
			payloadJson.remove("query");
		} else {
			query = JSONObject.fromObject("{\"match_all\":{}}");
		}
		final JSONObject filterAcl = new JSONObject().element("terms", new JSONObject().element("ecm:acl", principals));
		final JSONObject newQuery = new JSONObject().element("filtered",
				new JSONObject().element("query", query).element("filter", filterAcl));
		payloadJson.put("query", newQuery);
		final String filteredPayload = payloadJson.toString();

		return filteredPayload;
	}

}
