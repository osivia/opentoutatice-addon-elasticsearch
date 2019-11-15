/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.automation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.core.api.impl.blob.StringBlob;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsState;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.EsStateChecker;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.IndexNAliasManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.utils.MessageUtils;

/**
 * @author david
 */
@Operation(id = CleanESIndices.ID, category = Constants.CAT_SERVICES)
public class CleanESIndices {

    public static final String ID = "Documents.CleanESIndices";

    private static final Log log = LogFactory.getLog(CleanESIndices.class);

    @OperationMethod
    public StringBlob run() throws Exception {
        String cleaningStatus = null;

        // Avoid at least two cleaning request at the same time
        // (Operation are singleton - cf org.nuxeo.ecm.automation.OperationType comments)
        synchronized (this) {
            if (log.isDebugEnabled()) {
                log.debug("Es indices [CLEANING] started ...");
            }

            // Check no re-indexing in progress
            boolean reIndexingInProgress = ReIndexingRunnerManager.get().isReIndexingInProgress();

            if (!reIndexingInProgress) {
                // Get orphan indices (i.e. not associated to any alias)
                EsState esState = EsStateChecker.get().getEsState();

                if (log.isInfoEnabled()) {
                    log.info(esState.toString());
                    log.info("Cleaning orphan indices ...");
                }

                // Indices mapped to aliases
                Map<String, List<String>> aliases = esState.getAliases();

                List<String> linkedIndices = new ArrayList<String>();
                for (Entry<String, List<String>> alias : aliases.entrySet()) {
                    linkedIndices.addAll(alias.getValue());
                }

                // Indices
                List<String> indices = esState.getIndices();

                Collection<String> orphanIndices = CollectionUtils.disjunction(indices, linkedIndices);

                // Clean
                if (orphanIndices.size() > 0) {
                    if (log.isInfoEnabled()) {
                        log.info(
                                String.format("Found [%s] orphan indices: [%s]: cleaning ...", orphanIndices.size(), MessageUtils.listToString(orphanIndices)));
                    }

                    for (String orphanIndex : orphanIndices) {
                        IndexNAliasManager.get().deleteIndex(orphanIndex);

                        if (log.isInfoEnabled()) {
                            log.info(String.format("Index [%s] deleted", orphanIndex));
                        }
                    }

                    // Returned info
                    EsState newEsState = EsStateChecker.get().getEsState();
                    cleaningStatus = String.format("[%s] orphan indices deleted: [%s] %s New %s", orphanIndices.size(),
                            MessageUtils.listToString(orphanIndices), System.lineSeparator(), newEsState.toString());
                } else {
                    cleaningStatus = "Found no orphan indices to clean.";
                }

            } else {
                cleaningStatus = "One Zero Down Time Re-Indexing process is in progress: you can not clean indicies for the moment.";
            }


            if (log.isInfoEnabled()) {
                log.info(cleaningStatus);
            }

        }

        return new StringBlob(cleaningStatus);
    }

}
