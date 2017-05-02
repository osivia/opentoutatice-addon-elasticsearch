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
package fr.toutatice.ecm.elasticsearch.automation;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.SearchResponse;
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
import org.nuxeo.ecm.core.schema.SchemaManager;
import org.nuxeo.ecm.core.schema.types.Schema;
import org.nuxeo.elasticsearch.api.ElasticSearchService;
import org.nuxeo.elasticsearch.query.NxQueryBuilder;

import fr.toutatice.ecm.elasticsearch.helper.SQLHelper;
import fr.toutatice.ecm.elasticsearch.query.TTCNxQueryBuilder;
import fr.toutatice.ecm.elasticsearch.search.TTCSearchResponse;

@Operation(id = QueryES.ID, category = Constants.CAT_FETCH, label = "Query via ElasticSerach", description = "Perform a query on ElasticSerach instead of Repository")
public class QueryES {

	private static final Log log = LogFactory.getLog(QueryES.class);
	private static final int DEFAULT_MAX_RESULT_SIZE = 10000;
	public static final String ID = "Document.QueryES";

	@Context
	CoreSession session;
	
	@Context
    OperationContext ctx;

    @Context
	ElasticSearchService elasticSearchService;

	@Context
	SchemaManager schemaManager;

	@Param(name = "query", required = true)
	protected String query;

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
        // Compat mode
        Integer currentPageIndex = this.currentPageIndex;
        if (this.currentPageIndex == null) {
            currentPageIndex = this.page;
        }

        NxQueryBuilder builder = new TTCNxQueryBuilder(this.session).nxql(SQLHelper.getInstance().escape(this.query));
        if (null != currentPageIndex && null != this.pageSize) {
            builder.offset((0 <= currentPageIndex ? currentPageIndex : 0) * this.pageSize);
            builder.limit(this.pageSize);
		} else {
			builder.limit(DEFAULT_MAX_RESULT_SIZE);
		}

        this.elasticSearchService.query(builder);
		SearchResponse esResponse = ((TTCNxQueryBuilder) builder).getSearchResponse();
		
		// Compat mode
		String schemas = this.nxProperties;
		if(this.nxProperties == null){
            schemas = getSchemasFromHeader(this.ctx);
		}


        return new DefaultJsonAdapter(new TTCSearchResponse(esResponse, this.pageSize, currentPageIndex, formatSchemas(schemas)));
	}

    /**
     * Gets schemas from Header.
     * 
     * @param ctx
     * @return schemas
     */
    // TODO: to remove when client ES query will send schema in header
    public String getSchemasFromHeader(OperationContext ctx) {
        HttpServletRequest httpRequest = (HttpServletRequest) ctx.get("request");
        String schemas = httpRequest.getHeader(JsonDocumentWriter.DOCUMENT_PROPERTIES_HEADER);
        return !StringUtils.equals("*", schemas) ? schemas : null;
    }

    private List<String> formatSchemas(String nxProperties) {
		List<String> schemas = new ArrayList<String>();

		if (StringUtils.isNotBlank(nxProperties)) {
			String[] schemasList = nxProperties.split(",");
			for (String schema : schemasList) {
				Schema sch = schemaManager.getSchema(StringUtils.trim(schema));
				if (null != sch) {
					String prefix = sch.getNamespace().prefix;
					schemas.add(StringUtils.isNotBlank(prefix) ? prefix : sch.getName());
				} else {
					log.warn("Unknown schema '" + schema + "' (query='" + query + "')");
				}
			}
		}
		
		return schemas;
	}
	
}
