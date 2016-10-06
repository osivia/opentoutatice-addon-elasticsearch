/*
 * (C) Copyright 2014 Académie de Rennes (http://www.ac-rennes.fr/), OSIVIA (http://www.osivia.com) and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * Contributors:
 * mberhaut1
 */
package fr.toutatice.ecm.elasticsearch.automation;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.nuxeo.ecm.automation.OperationException;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.automation.jaxrs.DefaultJsonAdapter;
import org.nuxeo.ecm.automation.jaxrs.JsonAdapter;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.NuxeoPrincipal;
import org.nuxeo.ecm.core.schema.SchemaManager;
import org.nuxeo.ecm.core.schema.types.Schema;
import org.nuxeo.ecm.core.security.SecurityService;
import org.nuxeo.elasticsearch.api.ElasticSearchAdmin;
import org.nuxeo.elasticsearch.api.ElasticSearchService;
import org.nuxeo.elasticsearch.query.NxQueryBuilder;
import fr.toutatice.ecm.elasticsearch.helper.SQLHelper;
import fr.toutatice.ecm.elasticsearch.query.TTCNxQueryBuilder;
import fr.toutatice.ecm.elasticsearch.search.TTCSearchResponse;
import net.sf.json.JSONObject;

@Operation(id = QueryES.ID, category = Constants.CAT_FETCH, label = "Query via ElasticSerach", description = "Perform a query on ElasticSerach instead of Repository")
public class QueryES {

	private static final Log LOG = LogFactory.getLog(QueryES.class);
	private static final int DEFAULT_MAX_RESULT_SIZE = 10000;
	public static final String ID = "Document.QueryES";

	public static enum QueryLanguage {
		NXQL, ES;
	}

	@Context
	CoreSession session;

	@Context
	ElasticSearchService elasticSearchService;

	@Context
	ElasticSearchAdmin elasticSearchAdmin;

	@Context
	SchemaManager schemaManager;

	@Param(name = "query", required = true)
	protected String query;

	@Param(name = "queryLanguage", required = false, description = "Language of the query parameter : NXQL or ES.", values = { "NXQL" })
	protected String queryLanguage = QueryLanguage.NXQL.name();

	@Param(name = "pageSize", required = false)
	protected Integer pageSize;

	@Param(name = "currentPageIndex", required = false)
	protected Integer currentPageIndex;

	@Param(name = "X-NXDocumentProperties", required = false)
	protected String nxProperties;

	@OperationMethod
	public JsonAdapter run() throws OperationException {
		try {
			switch (QueryLanguage.valueOf(queryLanguage)) {
			case NXQL:
				return runNxqlSearch();
			case ES:
				return runEsSearch();
			default:
				throw new OperationException("Illegal argument value for parameter 'queryLanguage' : " + queryLanguage);
			}
		} catch (final IllegalArgumentException e) {
			throw new OperationException("Illegal argument value for parameter 'queryLanguage' : " + queryLanguage, e);
		}
	}

	protected JsonAdapter runNxqlSearch() throws OperationException {

		final NxQueryBuilder builder = new TTCNxQueryBuilder(session).nxql(SQLHelper.getInstance().escape(query));
		if (null != currentPageIndex && null != pageSize) {
			builder.offset((0 <= currentPageIndex ? currentPageIndex : 0) * pageSize);
			builder.limit(pageSize);
		} else {
			builder.limit(DEFAULT_MAX_RESULT_SIZE);
		}

		elasticSearchService.query(builder);
		final SearchResponse esResponse = ((TTCNxQueryBuilder) builder).getSearchResponse();

		return new DefaultJsonAdapter(new TTCSearchResponse(esResponse, pageSize, currentPageIndex, getSchemas(nxProperties)));
	}

	private List<String> getSchemas(String nxProperties) {
		final List<String> schemas = new ArrayList<String>();

		if (StringUtils.isNotBlank(nxProperties)) {
			final String[] schemasList = nxProperties.split(",");
			for (final String schema : schemasList) {
				final Schema sch = schemaManager.getSchema(StringUtils.trim(schema));
				if (null != sch) {
					final String prefix = sch.getNamespace().prefix;
					schemas.add(StringUtils.isNotBlank(prefix) ? prefix : sch.getName());
				} else {
					LOG.warn("Unknown schema '" + schema + "' (query='" + query + "')");
				}
			}
		}

		return schemas;
	}

	protected JsonAdapter runEsSearch() throws OperationException {

		final SearchRequestBuilder request = elasticSearchAdmin.getClient().prepareSearch(elasticSearchAdmin.getIndexNameForRepository(session.getRepositoryName())).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		request.setSource(getESRequestPayload());

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Search query: curl -XGET 'http://localhost:9200/%s/_search?pretty' -d '%s'", elasticSearchAdmin.getIndexNameForRepository(session.getRepositoryName()), request.toString()));
		}
		try {
			final SearchResponse esResponse = request.get();

			if (LOG.isDebugEnabled()) {
				LOG.debug("Result: " + esResponse.toString());
			}

			return new DefaultJsonAdapter(esResponse);
		} catch (final ElasticsearchException e) {
			throw new OperationException("Error while executing the ElasticSearch request", e);
		}
	}

	public String getESRequestPayload() {
		final Principal principal = session.getPrincipal();
		if (principal == null || (principal instanceof NuxeoPrincipal && ((NuxeoPrincipal) principal).isAdministrator())) {
			return query;
		}

		final String[] principals = SecurityService.getPrincipalsToCheck(principal);
		final JSONObject payloadJson = JSONObject.fromObject(query);
		JSONObject query;
		if (payloadJson.has("query")) {
			query = payloadJson.getJSONObject("query");
			payloadJson.remove("query");
		} else {
			query = JSONObject.fromObject("{\"match_all\":{}}");
		}
		final JSONObject filterAcl = new JSONObject().element("terms", new JSONObject().element("ecm:acl", principals));
		final JSONObject newQuery = new JSONObject().element("filtered", new JSONObject().element("query", query).element("filter", filterAcl));
		payloadJson.put("query", newQuery);
		final String filteredPayload = payloadJson.toString();

		return filteredPayload;
	}

}
