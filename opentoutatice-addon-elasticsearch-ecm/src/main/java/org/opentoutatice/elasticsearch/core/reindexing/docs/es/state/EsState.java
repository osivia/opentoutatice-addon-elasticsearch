/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.es.state;

import static org.nuxeo.elasticsearch.ElasticSearchConstants.DOC_TYPE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.nuxeo.ecm.core.api.CoreInstance;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.IterableQueryResult;
import org.nuxeo.elasticsearch.api.ElasticSearchAdmin;
import org.nuxeo.elasticsearch.api.ElasticSearchService;
import org.nuxeo.elasticsearch.api.EsResult;
import org.nuxeo.elasticsearch.query.NxQueryBuilder;
import org.nuxeo.runtime.api.Framework;

/**
 * @author david
 *
 */
public class EsState {
	
	private static final Log log = LogFactory.getLog(EsState.class);

	private List<String> indices;

	private Map<String, List<String>> aliases;

	public EsState() {
		this.indices = new ArrayList<String>();
		this.aliases = new HashMap<String, List<String>>();
	}

	public void addIndex(String index) {
		this.indices.add(index);
	}

	public void addAlias(String name, List<String> indices) {
		this.aliases.put(name, indices);
	}

	public int getNbAliases() {
		return this.aliases.keySet().size();
	}

	public int getNbIndices() {
		return this.indices.size();
	}

	public Map<String, Long> getNbDocsByIndicesOn(String repository, boolean esPassThrought) {
		Map<String, Long> nbDocsByIndices = new HashMap<String, Long>();
		
		if(esPassThrought) {
			ElasticSearchAdmin esAdmin = Framework.getService(ElasticSearchAdmin.class);
			SearchRequestBuilder requestBuilder = esAdmin.getClient().prepareSearch("nxutest-alias").setTypes(DOC_TYPE).setSearchType(
	                SearchType.DFS_QUERY_THEN_FETCH);
			SearchResponse searchResponse = requestBuilder.get();
			SearchHits hits = searchResponse.getHits();
			
			for(SearchHit hit : hits) {
				String index = hit.getIndex();
				Long nbBy = nbDocsByIndices.get(index);
				if(nbBy == null) {
					nbDocsByIndices.put(index, Long.valueOf(0));
				} else {
					nbDocsByIndices.put(index, Long.valueOf(nbBy.longValue() + 1));
				}
			}
		} else {
			ElasticSearchService esService = Framework.getService(ElasticSearchService.class);

			CoreSession systemSession = null;
			try {
				systemSession = CoreInstance.openCoreSessionSystem(repository);
				NxQueryBuilder qBuilder = new NxQueryBuilder(systemSession);
				qBuilder.nxql("select ecm:uuid from Document order by ecm:uuid").limit(-1);
	
				EsResult esResult = esService.queryAndAggregate(qBuilder);
				IterableQueryResult rows = esResult.getRows();
				
				nbDocsByIndices.put("nxutest-alias", Long.valueOf(rows.size()));
			} finally {
				if (systemSession != null) {
					systemSession.close();
				}
			}
			
		}

		return nbDocsByIndices;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();

		sb.append(String.format("Indices [%s]: ", String.valueOf(this.indices.size())));
		Iterator<String> indicesIt = this.indices.iterator();
		while (indicesIt.hasNext()) {
			String index = indicesIt.next();
			sb.append(index);
			if (indicesIt.hasNext()) {
				sb.append(", ");
			}
		}

		sb.append(System.lineSeparator());

		sb.append(String.format("Aliases [%s]: ", String.valueOf(this.aliases.keySet().size())));
		if (this.aliases.keySet().size() > 0) {
			sb.append(System.lineSeparator());
		}
		for (Entry<String, List<String>> aliasEntry : this.aliases.entrySet()) {
			sb.append(String.format("[%s] on indices: ", aliasEntry.getKey()));

			Iterator<String> indicesOfIt = aliasEntry.getValue().iterator();
			while (indicesOfIt.hasNext()) {
				String index = indicesOfIt.next();
				sb.append(index);
				if (indicesOfIt.hasNext()) {
					sb.append(", ");
				}
			}
			sb.append(System.lineSeparator());
		}

		return sb.toString();
	}

	public List<String> getIndices() {
		return indices;
	}

	public void setIndices(List<String> indices) {
		this.indices = indices;
	}

	public Map<String, List<String>> getAliases() {
		return aliases;
	}

	public void setAliases(Map<String, List<String>> aliases) {
		this.aliases = aliases;
	}

}
