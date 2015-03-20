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
package fr.toutatice.ecm.elasticsearch.codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.codehaus.jackson.JsonGenerator;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.nuxeo.ecm.automation.io.services.codec.ObjectCodec;

import fr.toutatice.ecm.elasticsearch.search.TTCSearchResponse;

public class TTCEsCodec extends ObjectCodec<TTCSearchResponse> {

//	private static final Log log = LogFactory.getLog(TTCEsCodec.class);
	
	@SuppressWarnings("serial")
	private static final List<String> keys = new ArrayList<String>() {{
	    add("entity-type");
	    add("isPaginable");
	}};
	
    public TTCEsCodec() {
        super(TTCSearchResponse.class);
    }
    
    @Override
    public String getType() {
        return "esresponse";
    }
    
    public void write(JsonGenerator jg, TTCSearchResponse value) throws IOException {

		SearchHits upperhits = value.getSearchResponse().getHits();
		SearchHit[] searchhits = upperhits.getHits();
    	Map<String, Object> entityMap = new TreeMap<String, Object>(new keyComparator());
		entityMap.put("entity-type", "documents");
		/**
		 * Due to bug about client unmarshalling, it is not possible to return a result of type "Documents"
		 * but only "PaginableDocuments". 
		 * 
		 * (Nuxeo reference: SUPNXP-12954)
		 */
		// entityMap.put("isPaginable", value.isPaginable());
		entityMap.put("isPaginable", true);
		if (value.isPaginable()) {
			entityMap.put("resultsCount", searchhits.length);
			entityMap.put("totalSize", upperhits.getTotalHits());
			entityMap.put("pageSize", value.getPageSize());
			entityMap.put("pageCount", upperhits.getTotalHits() / value.getPageSize() + ((0 < upperhits.getTotalHits() % value.getPageSize()) ? 1 : 0));
			entityMap.put("currentPageIndex", value.getCurrentPageIndex());
		} else {
			entityMap.put("resultsCount", searchhits.length);
			entityMap.put("totalSize", upperhits.getTotalHits());
			entityMap.put("pageSize", upperhits.getTotalHits());
			entityMap.put("pageCount", 1);
			entityMap.put("currentPageIndex", 1);			
		}
		
		List<Map<String, Object>> entries = new ArrayList<Map<String, Object>>();
    	entityMap.put("entries", entries);
    	for (SearchHit hit : searchhits) {
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
    		
    		Map<String, Object> propertiesMap = new HashMap<String, Object>();
    		for (String key : source.keySet()) {
    			if (!key.matches("ecm:.*")) {
    				propertiesMap.put(key, source.get(key));
    			}
    		}
    		entryMap.put("properties", propertiesMap);
    		entries.add(entryMap);
    	}
    	
        jg.writeObject(entityMap);
    }
    
    private class keyComparator implements Comparator<String> {

		@Override
		public int compare(String key1, String key2) {
			int i1 = keys.indexOf(key1);
			int i2 = keys.indexOf(key2);

			i1 = (0 > i1) ? keys.size() : i1;
			i2 = (0 > i2) ? keys.size() : i2;
			return (i1 != i2) ? i1 - i2 : 1;
		}
    	
    }

}
