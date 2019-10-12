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
package fr.toutatice.ecm.elasticsearch.codec;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.nuxeo.ecm.automation.io.services.codec.ObjectCodec;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.IndexNAliasManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.query.filter.ReIndexingTransientAggregate;
import org.opentoutatice.elasticsearch.core.reindexing.docs.transitory.TransitoryIndexUse;

import fr.toutatice.ecm.elasticsearch.search.TTCSearchResponse;

public class TTCEsCodec extends ObjectCodec<TTCSearchResponse> {

    private static final Log log = LogFactory.getLog(TTCEsCodec.class);

    public TTCEsCodec() {
        super(TTCSearchResponse.class);
    }

    @Override
    public String getType() {
        return "esresponse";
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(JsonGenerator jg, TTCSearchResponse value) throws IOException {
        // For logs
        long startTime = System.currentTimeMillis();

        SearchHits upperhits = value.getSearchResponse().getHits();
        String schemasRegex = value.getSchemasRegex();

        SearchHit[] searchhits = upperhits.getHits();

        jg.writeStartObject();
        jg.writeStringField("entity-type", "documents");
        if (value.isPaginable()) {
            jg.writeBooleanField("isPaginable", value.isPaginable());
            jg.writeNumberField("resultsCount", searchhits.length);
            jg.writeNumberField("totalSize", upperhits.getTotalHits());
            jg.writeNumberField("pageSize", value.getPageSize());
            // Take empty response into account
            long pageCount = 0;
            if (value.getPageSize() > 0) {
                pageCount = (upperhits.getTotalHits() / value.getPageSize()) + ((0 < (upperhits.getTotalHits() % value.getPageSize())) ? 1 : 0);
            }
            jg.writeNumberField("pageCount", pageCount);
            jg.writeNumberField("currentPageIndex", value.getCurrentPageIndex());
        }

        jg.writeArrayFieldStart("entries");
        
        if (this.hasToFilterDuplicate(value.getSearchResponse())) {
            this.writeDuplicateFilteredEntries(jg, schemasRegex, value.getSearchResponse());
        } else {
            this.writeEntries(jg, schemasRegex, searchhits);
        }
        jg.writeEndArray();

        jg.writeEndObject();
        jg.flush();
        
        if(log.isDebugEnabled()) {
            long duration = System.currentTimeMillis() - startTime;
            log.debug(String.format("Json written: [TJ_%s_TJ] ms", String.valueOf(duration)));
        }
    }

    protected boolean hasToFilterDuplicate(SearchResponse searchResponse) {
        // Indicates if this response comes from a request built during zero down time re-indexing
        // (we do not use ReIndexingRunnerManager.get()#isReIndexingInProgress here)
        return searchResponse.getAggregations() != null ? searchResponse.getAggregations().get(ReIndexingTransientAggregate.DUPLICATE_AGGREGATE_NAME) != null
                : false;
    }

    /**
     * @param jg
     * @param schemasRegex
     * @param searchhits
     * @throws IOException
     * @throws JsonGenerationException
     * @throws JsonProcessingException
     */
    protected void writeEntries(JsonGenerator jg, String schemasRegex, SearchHit[] searchhits)
            throws IOException, JsonGenerationException, JsonProcessingException {
        // For logs
        long startTime = System.currentTimeMillis();
        
        for (SearchHit hit : searchhits) {
            this.writeEntry(jg, schemasRegex, hit.getSource());
        }
        
        if(log.isDebugEnabled()) {
            long duration = System.currentTimeMillis() - startTime;
            log.debug(String.format("#writeEntries: [%s] ms", String.valueOf(duration)));
        }
    }

    protected void writeDuplicateFilteredEntries(JsonGenerator jg, String schemasRegex, SearchResponse searchResponse)
            throws JsonGenerationException, JsonProcessingException, IOException {
        // For logs
        long startTime = System.currentTimeMillis();
        
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        StringTerms duplicateAggs = searchResponse.getAggregations().get(ReIndexingTransientAggregate.DUPLICATE_AGGREGATE_NAME);

        // Build duplicate list
        List<String> duplicateIds = new LinkedList<String>();

        for (Bucket bucket : duplicateAggs.getBuckets()) {
            if (bucket.getDocCount() > 1) {
                duplicateIds.add(bucket.getKey());
            }
        }

        if (log.isTraceEnabled()) {
            StringBuffer sb = new StringBuffer();
            Iterator<String> duplicatesIt = duplicateIds.iterator();
            while (duplicatesIt.hasNext()) {
                String dupId = duplicatesIt.next();
                sb.append(dupId);
                if (duplicatesIt.hasNext()) {
                    sb.append(", ");
                }
            }
            log.trace(String.format("List of duplicates: [%s]", sb.toString()));
        }

        if (duplicateIds.size() == 0) {
            this.writeEntries(jg, schemasRegex, searchHits);
        } else {
            // Write filtering duplicate:
            // index from which duplicates must be kept (index pointed by transient write alias)
            // TODO: response can be managed few later time after re-indexing and write alias can not exist anymore
            String newIdx = IndexNAliasManager.get().getIndexOfAlias(TransitoryIndexUse.Write.getAlias());

            for (SearchHit hit : searchHits) {
                // Check duplicate
                Map<String, Object> source = hit.getSource();
                String uuid = (String) source.get(ReIndexingTransientAggregate.DUPLICATE_AGGREGATE_FIELD);

                if (duplicateIds.contains(uuid)) {
                    // keep duplicate from new (re-indexing) index
                    if (StringUtils.equals(newIdx, hit.getIndex())) {
                        if (log.isTraceEnabled()) {
                            log.trace(String.format("Keeping duplicate [%s] from index [%s]", uuid, hit.getIndex()));
                        }
                        this.writeEntry(jg, schemasRegex, source);
                    }
                } else {
                    this.writeEntry(jg, schemasRegex, source);
                }
            }
        }
        
        if(log.isDebugEnabled()) {
            long duration = System.currentTimeMillis() - startTime;
            log.debug(String.format("#writeDuplicateFilteredEntries done: [%s] ms", String.valueOf(duration)));
        }

    }

    /**
     * @param jg
     * @param schemasRegex
     * @param hit
     * @throws IOException
     * @throws JsonGenerationException
     * @throws JsonProcessingException
     */
    private void writeEntry(JsonGenerator jg, String schemasRegex, Map<String, Object> source)
            throws IOException, JsonGenerationException, JsonProcessingException {
        jg.writeStartObject();

        // convert ES JSON mapping into Nuxeo automation mapping
        jg.writeStringField("entity-type", "document");
        jg.writeStringField("repository", (String) source.get("ecm:repository"));
        jg.writeStringField("uid", (String) source.get("ecm:uuid"));
        jg.writeStringField("path", (String) source.get("ecm:path"));
        jg.writeStringField("type", (String) source.get("ecm:primaryType"));
        jg.writeStringField("state", (String) source.get("ecm:currentLifeCycleState"));
        jg.writeStringField("parentRef", (String) source.get("ecm:parentId"));
        jg.writeStringField("versionLabel", (String) source.get("ecm:versionLabel"));
        jg.writeStringField("isCheckedOut", StringUtils.EMPTY);
        jg.writeStringField("title", (String) source.get("dc:title"));
        jg.writeStringField("lastModified", (String) source.get("dc:modified"));
        jg.writeObjectField("facets", source.get("ecm:mixinType"));
        jg.writeStringField("changeToken", (String) source.get("ecm:changeToken"));
        // jg.writeStringField("ancestorId", (String) source.get("ecm:ancestorId"));

        jg.writeObjectFieldStart("properties");
        for (String key : source.keySet()) {
            if (!key.matches("ecm:.+") && key.matches(schemasRegex)) {
                jg.writeObjectField(key, source.get(key));
            }
        }
        jg.writeEndObject();
        jg.writeEndObject();
        jg.flush();
    }

}
