package fr.toutatice.ecm.elasticsearch.automation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.nuxeo.ecm.automation.OperationException;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.api.impl.DocumentModelListImpl;
import org.nuxeo.ecm.core.api.impl.SimpleDocumentModel;
import org.nuxeo.elasticsearch.api.ElasticSearchService;
import org.nuxeo.elasticsearch.query.NxQueryBuilder;

import fr.toutatice.ecm.elasticsearch.query.TTCNxQueryBuilder;

@Operation(id = QueryES.ID, category = Constants.CAT_FETCH, label = "Query  via ElasticSerach", description = "Perform a query on ElasticSerach instead of Repository")
public class QueryES {

	private static final Log log = LogFactory.getLog(QueryES.class);

    public static final String ID = "Document.QueryES";

//    @HeaderParam(value = "X-NXDocumentProperties")
//    String schemas;
    
    @Context
    CoreSession session;

    @Context
    ElasticSearchService elasticSearchService;
    
    @Param(name = "query", required = false)
    protected String query;

    @Param(name = "page", required = false)
    protected Integer page;

    @Param(name = "currentPageIndex", required = false)
    protected Integer currentPageIndex;

    @Param(name = "pageSize", required = false)
    protected Integer pageSize;

    @OperationMethod
    public DocumentModelList run() throws OperationException {
    	
    	DocumentModelList list = new DocumentModelListImpl();

		long startTime = System.currentTimeMillis();

    	NxQueryBuilder builder = new TTCNxQueryBuilder(session).nxql(query).limit(-1);
    	elasticSearchService.query(builder);
    	SearchResponse esResponse = ((TTCNxQueryBuilder) builder).getSearchResponse();
    	
    	Map<String, Object> entityMap = new HashMap<String, Object>();
    	List<Map<String, Object>> entries = new ArrayList<Map<String, Object>>();
    	entityMap.put("entity-type", "documents");
    	entityMap.put("entries", entries);
    	
    	SearchHit[] hits = esResponse.getHits().getHits();
//    	for (SearchHit hit : hits) {
//    		Map<String, Object> source = hit.getSource();
//    		SimpleDocumentModel sd = new SimpleDocumentModel();
//    		for (String key : source.keySet()) {
//    			if (!key.matches("ecm:.*")) {
//    				sd.setPropertyValue(key, source.get);
//    			}
//    		}
//    	}
		
    	for (SearchHit hit : hits) {
    		Map<String, Object> source = hit.getSource();
    		Map<String, Object> entryMap = new HashMap<String, Object>();
    		
    		// convert ES JSON mapping into Nuxeo automation mapping
    		entryMap.put("entity-type", "document");
    		entryMap.put("repository", source.get("ecm:repository"));
    		entryMap.put("uid", source.get("ecm:uuid"));
    		entryMap.put("path", source.get("ecm:path"));
    		entryMap.put("type", source.get("ecm:primaryType"));
    		entryMap.put("state", source.get("ecm:currentLifeCycleState"));
    		entryMap.put("parentRef", source.get("ecm:parentId"));
    		entryMap.put("versionLabel", source.get("ecm:versionLabel"));
    		entryMap.put("isCheckedOut", "");
    		entryMap.put("title", source.get("dc:title"));
    		entryMap.put("lastModified", source.get("dc:modified"));
    		entryMap.put("facets", source.get("ecm:mixinType"));
    		entryMap.put("changeToken", source.get("ecm:changeToken"));
//    		entryMap.put("contextParameters", "");
    		
    		Map<String, Object> propertiesMap = new HashMap<String, Object>();
    		for (String key : source.keySet()) {
    			if (!key.matches("ecm:.*")) {
    				propertiesMap.put(key, source.get(key));
    			}
    		}
    		entryMap.put("properties", propertiesMap);
    		entries.add(entryMap);
    	}

		long stopTime = System.currentTimeMillis();
		String execTime = String.format("%d s %d ms", (((stopTime - startTime) / 1000) % 60), ((stopTime - startTime) % 1000));
		log.info("> QueryES::run(): " + execTime);

//    	JSONObject json = new JSONObject();
//        json.putAll(entityMap);
//        String jsonStrg = json.toString();
//        log.info(jsonStrg);
                
    	return list;
    }
    
}
