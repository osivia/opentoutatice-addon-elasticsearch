/**
 * 
 */
package org.opentoutatice.elasticsearch.web.admin;

import static org.jboss.seam.ScopeType.EVENT;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.seam.annotations.Install;
import org.jboss.seam.annotations.Name;
import org.jboss.seam.annotations.Scope;
import org.nuxeo.elasticsearch.web.admin.ElasticSearchManager;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchAdmin;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception.ReIndexingStateException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception.ReIndexingStatusException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;

import fr.toutatice.ecm.platform.core.constants.ExtendedSeamPrecedence;


/**
 * @author david
 */
@Name("esAdmin")
@Scope(EVENT)
@Install(precedence = ExtendedSeamPrecedence.TOUTATICE)
public class OttcElasticSearchManager extends ElasticSearchManager {
    
    private static final Log log = LogFactory.getLog(OttcElasticSearchManager.class);
    
    public boolean isAliasModeEnabled(String repositoryName) {
        return ((OttcElasticSearchAdmin) this.esa).aliasConfigured(repositoryName);
    }
    
    @Override
    public void startReindexAll() {
        if(!isAliasModeEnabled(getRepositoryName())) {
            super.startReindexAll();
        } else {
            log.warn(String.format("Re-indexing the entire repository [%s] with ZDT", getRepositoryName()));
            try {
                ((OttcElasticSearchIndexing) super.esi).reIndexAllDocumentsWithZeroDownTime(getRepositoryName());
            } catch (ReIndexingStatusException | ReIndexingStateException | ReIndexingException e) {
               log.fatal("Error during ZDT full re-indexing: process aborted");
               //TODO: to show in info UI?
            }
        }
    }
    
    protected static final String ALIAS_MODE_LABEL = "%s (alias mode enabled)"; 
    
    public Map<String, String> getRepositoryNamesWithAliasMode(){
        Map<String, String> reposWith = new HashMap<>(getRepositoryNames().size());
        for(String repo: getRepositoryNames()) {
            if(isAliasModeEnabled(repo)) {
                reposWith.put(String.format(ALIAS_MODE_LABEL, repo), repo);
            } else {
                reposWith.put(repo, repo);
            }
        }
        return reposWith;
    }

}
