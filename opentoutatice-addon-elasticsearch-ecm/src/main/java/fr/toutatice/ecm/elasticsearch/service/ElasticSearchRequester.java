/**
 * 
 */
package fr.toutatice.ecm.elasticsearch.service;

import java.util.List;

import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.elasticsearch.core.ElasticSearchServiceImpl;
import org.nuxeo.elasticsearch.query.NxQueryBuilder;



/**
 * New ElasticSearchService encapsuling new ElasticSearchAdmin to override 
 * its getIncludeSourceFields method.
 * So we can pass sources files as arguments instead of setting configuration.
 * 
 * @author david
 *
 */
public class ElasticSearchRequester extends ElasticSearchServiceImpl {
    
    /** Request header to manage schema. */
    private ElasticSearchRequestHeader header;
    
    /** Singleton. */
    private static ElasticSearchRequester esr;
    
    private ElasticSearchRequester() {
        super(ElasticSearchRequestHeader.getInstance());
    }
    
    public static  synchronized ElasticSearchRequester getInstance(){
        if(esr == null){
            esr = new ElasticSearchRequester();
        }
        return esr;
    }
    
    /**
     * @return the header
     */
    public ElasticSearchRequestHeader getHeader() {
        return header;
    }
    
    /**
     * @param header the header to set
     */
    public void setHeader(ElasticSearchRequestHeader header) {
        this.header = header;
    }
    
    public DocumentModelList query(NxQueryBuilder queryBuilder, List<String> schemas) throws ClientException{
        this.header.setSchemas(schemas);
        return super.query(queryBuilder);
    }
    

}
