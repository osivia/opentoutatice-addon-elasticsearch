/**
 * 
 */
package org.opentoutatice.elasticsearch.web.admin;

import static org.jboss.seam.ScopeType.EVENT;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.seam.annotations.In;
import org.jboss.seam.annotations.Install;
import org.jboss.seam.annotations.Name;
import org.jboss.seam.annotations.Scope;
import org.jboss.seam.faces.FacesMessages;
import org.jboss.seam.international.StatusMessage;
import org.nuxeo.ecm.automation.jsf.OperationActionBean;
import org.nuxeo.elasticsearch.web.admin.ElasticSearchManager;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchAdmin;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.core.reindexing.docs.automation.CleanESIndices;
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
    
    @In(create = true, required = true)
    protected OperationActionBean operationActionBean;
    
    @In(create = true)
    protected transient FacesMessages facesMessages;
    
    @In(create = true)
    protected Map<String, String> messages;
    
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
               this.facesMessages.add(StatusMessage.Severity.ERROR, "Erreur durant le processus de ré-indexation: le processus a été arrêté");
            }
        }
    }
    
    public void cleanIndices() {
        if(isAliasModeEnabled(getRepositoryName())) {
            try {
                this.operationActionBean.doOperation(CleanESIndices.ID);
            } catch (Exception e) {
                log.fatal("Error during indexes cleaning: process aborted");
                this.facesMessages.add(StatusMessage.Severity.ERROR, "Erreur durant le nettoyage des index: le processus a été arrêté");
            }
            this.facesMessages.add(StatusMessage.Severity.INFO, "Nettoyage des index terminé");
        } else {
            this.facesMessages.add(StatusMessage.Severity.INFO, "Le dépôt n'est pas configuré en mode alias: aucune action effectuée");
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
