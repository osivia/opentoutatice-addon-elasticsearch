package fr.toutatice.ecm.elasticsearch.codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonGenerator;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.nuxeo.ecm.automation.io.services.codec.ObjectCodec;

public class TTCEsCodec extends ObjectCodec<SearchResponse> {

	private static final Log log = LogFactory.getLog(TTCEsCodec.class);

    public TTCEsCodec() {
        super(SearchResponse.class);
    }

    @Override
    public String getType() {
        return "esresponse";
    }
    
    public void write(JsonGenerator jg, SearchResponse value) throws IOException {

		long startTime = System.currentTimeMillis();

    	Map<String, Object> entityMap = new HashMap<String, Object>();
    	List<Map<String, Object>> entries = new ArrayList<Map<String, Object>>();
    	entityMap.put("entries", entries);
    	
    	SearchHit[] hits = value.getHits().getHits();
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

    	entityMap.put("isPaginable", Boolean.TRUE);
    	entityMap.put("entity-type", "documents");

		long stopTime = System.currentTimeMillis();
		String execTime = String.format("%d s %d ms", (((stopTime - startTime) / 1000) % 60), ((stopTime - startTime) % 1000));
		log.info("> TTCEsCodec::write(): " + execTime);

        jg.writeObject(entityMap);
    }

}
