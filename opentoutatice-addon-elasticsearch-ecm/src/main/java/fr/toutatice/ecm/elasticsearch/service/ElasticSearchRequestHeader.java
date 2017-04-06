/**
 * 
 */
package fr.toutatice.ecm.elasticsearch.service;

import java.util.List;
import java.util.Map;

import org.nuxeo.elasticsearch.api.ElasticSearchAdmin;
import org.nuxeo.elasticsearch.config.ElasticSearchIndexConfig;
import org.nuxeo.elasticsearch.config.ElasticSearchLocalConfig;
import org.nuxeo.elasticsearch.config.ElasticSearchRemoteConfig;
import org.nuxeo.elasticsearch.core.ElasticSearchAdminImpl;
import org.nuxeo.runtime.api.Framework;


/**
 * @author david
 *
 */
public class ElasticSearchRequestHeader extends ElasticSearchAdminImpl {
    
    /** Schema Header. */
    private List<String> schemas;
    
    private static ElasticSearchAdmin esa;
    
    private static ElasticSearchRequestHeader instance;
    
    public static ElasticSearchRequestHeader getInstance(){
        if(instance == null){
                // Connect
                esa = Framework.getService(ElasticSearchAdmin.class);
                instance = new ElasticSearchRequestHeader(null, null, null);
        }
        return instance;
    }
    
    /**
     * @param localConfig
     * @param remoteConfig
     * @param indexConfig
     */
    public ElasticSearchRequestHeader(ElasticSearchLocalConfig localConfig, ElasticSearchRemoteConfig remoteConfig,
            Map<String, ElasticSearchIndexConfig> indexConfig) {
            super(localConfig, remoteConfig, indexConfig);
    }

    /**
     * @return the schemas
     */
    public List<String> getSchemas() {
        return schemas;
    }

    /**
     * @param schemas the schemas to set
     */
    public void setSchemas(List<String> schemas) {
        this.schemas = schemas;
    }
    
    
    
    protected String[] getIncludeSourceFields() {
        String[] src = null;
        
        if(this.schemas != null){
            src = new String[this.schemas.size()];
            for(int index = 0; index < this.schemas.size(); index++){
                src[index] = this.schemas.get(index) + ":*";
            }
        }
        
        return src;
    }
    
//    @Override
//    String[] getExcludeSourceFields() {
//        return excludeSourceFields;
//    }
    

}
