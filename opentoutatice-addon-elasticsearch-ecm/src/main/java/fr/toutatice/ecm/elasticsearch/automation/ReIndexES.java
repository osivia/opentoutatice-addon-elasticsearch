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
package fr.toutatice.ecm.elasticsearch.automation;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.automation.OperationException;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.core.api.CoreInstance;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentRef;
import org.nuxeo.ecm.core.api.IdRef;
import org.nuxeo.ecm.core.event.EventContext;
import org.nuxeo.ecm.core.event.impl.EventContextImpl;
import org.nuxeo.elasticsearch.api.ElasticSearchAdmin;
import org.nuxeo.elasticsearch.api.ElasticSearchIndexing;
import org.nuxeo.elasticsearch.commands.IndexingCommand;
import org.nuxeo.elasticsearch.commands.IndexingCommand.Type;

import fr.toutatice.ecm.platform.core.helper.ToutaticeNotifyEventHelper;

@Operation(id = ReIndexES.ID, category = Constants.CAT_DOCUMENT, label = "Re-index documents via ElasticSerach", description = "Re-build the ElasticSerach indice of either the whole repositry, a document (and its sub-hierarchy), documents selected based on a Nxql query")
public class ReIndexES {

	private static final Log log = LogFactory.getLog(QueryES.class);
	public static final String ID = "Document.ReIndexES";

    private static final String JSON_DELETE_CMD = "{\"id\":\"IndexingCommand-reindex\",\"type\":\"DELETE\",\"docId\":\"%s\",\"repo\":\"%s\",\"recurse\":true,\"sync\":true}";

	public static final String ENUM_REINDEX_TYPE_ALL = "ALL";
	public static final String ENUM_REINDEX_TYPE_ROOT = "ROOT";
	public static final String ENUM_REINDEX_TYPE_QUERY = "QUERY";
	public static final String ELASTICSEARCH_AUDIT_EVENT_NAME = "opentoutatice.addon.elasticsearch.audit.event";
	public static final String ELASTICSEARCH_AUDIT_EVENT_CATEGORY = "opentoutatice.addon.elasticsearch.audit.category";

	@Context
	CoreSession session;

	@Context
    protected ElasticSearchAdmin esa;

	@Context
    protected ElasticSearchIndexing esi;

	@Param(name = "type", required = true, values = {ENUM_REINDEX_TYPE_ALL, ENUM_REINDEX_TYPE_ROOT, ENUM_REINDEX_TYPE_QUERY})
	protected String type;

	@Param(name = "repositoryName", required = true)
	protected String repositoryName;

	@Param(name = "query", required = false)
	protected String query;

	@Param(name = "rootID", required = false)
	protected String rootID;

	@OperationMethod
	public void run() throws OperationException {
			
		// Re-index according to the requested type
		if (ENUM_REINDEX_TYPE_ALL.equals(type)) {
			logAudit(session, String.format("Re-index all the repository '%s'", repositoryName));
			startReindexAll();
		} else if (ENUM_REINDEX_TYPE_ROOT.equals(type)) {
			if (StringUtils.isNotBlank(rootID)) {
				logAudit(session, String.format("Re-index the document (and sub-hierarchy) with id '%s'", rootID));
				startReindexFrom();
			} else {
				log.warn("Re-indexation of a root document is requested with not root specified");
			}
		} else {
			if (StringUtils.isNotBlank(query)) {
				logAudit(session, String.format("Re-index the documents selected from the query '%s'", query));
				startReindexNxql();
			} else {
				log.warn("Re-indexation of documents is requested with not query specified");
			}
		}
		
	}
	
    private void startReindexAll() {
        log.warn("Re-indexing the entire repository: " + repositoryName);
        esa.dropAndInitRepositoryIndex(repositoryName);
        esi.runReindexingWorker(repositoryName, "SELECT ecm:uuid FROM Document");
    }

    private void startReindexNxql() {
        log.warn(String.format("Re-indexing from a NXQL query: %s on repository: %s", query, repositoryName));
        esi.runReindexingWorker(repositoryName, query);
    }

    private void startReindexFrom() {
        try (CoreSession session = CoreInstance.openCoreSessionSystem(repositoryName)) {
            String jsonCmd = String.format(JSON_DELETE_CMD, rootID, repositoryName);
            IndexingCommand rmCmd = IndexingCommand.fromJSON(jsonCmd);
            esi.indexNonRecursive(rmCmd);

            DocumentRef ref = new IdRef(rootID);
            if (session.exists(ref)) {
                DocumentModel doc = session.getDocument(ref);
                log.warn(String.format("Re-indexing document: %s and its children on repository: %s", doc,
                        repositoryName));
                IndexingCommand cmd = new IndexingCommand(doc, Type.INSERT, false, true);
                esi.runIndexingWorker(Arrays.asList(cmd));
            }
        }
    }
    
    private void logAudit(CoreSession session, String comment) {
    	EventContext ctx = new EventContextImpl(session, session.getPrincipal());
    	ToutaticeNotifyEventHelper.notifyAuditEvent(ctx, 
    			ELASTICSEARCH_AUDIT_EVENT_CATEGORY, 
    			ELASTICSEARCH_AUDIT_EVENT_NAME, 
    			comment);
    }

}
