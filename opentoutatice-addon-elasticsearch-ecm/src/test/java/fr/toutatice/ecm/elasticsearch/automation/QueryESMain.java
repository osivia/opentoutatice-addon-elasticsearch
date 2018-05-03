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
package fr.toutatice.ecm.elasticsearch.automation;

import org.nuxeo.ecm.automation.client.Constants;
import org.nuxeo.ecm.automation.client.OperationRequest;
import org.nuxeo.ecm.automation.client.Session;
import org.nuxeo.ecm.automation.client.jaxrs.impl.HttpAutomationClient;
import org.nuxeo.ecm.automation.client.jaxrs.spi.JsonMarshalling;
import org.nuxeo.ecm.automation.client.model.Document;
import org.nuxeo.ecm.automation.client.model.Documents;

import fr.toutatice.ecm.elasticsearch.marshaller.EsMarshaller;
import junit.framework.Assert;

public class QueryESMain {

    private static final String host = "vm-dch-mig8";
    private static final String user = "admin";
    private static final String pwd = "osivia";

    private static final String url = "http://%s:8081/nuxeo/site/automation";

    private static final String query = "select * from Document where ecm:mixinType = 'Folderish' "
            + "and ecm:ancestorId = '%s' and ecm:isVersion = 0 and ecm:currentLifeCycleState = 'deleted' and ecm:isProxy = 0";
    // private static final String query = "select * from Document where ecm:mixinType = \"OttcDraft\" "
    // + "and ottcDft:checkinedDocId = \"\" and ottcDft:checkoutParentId = \"%s\" and ecm:isProxy = 0 and ecm:isVersion = 0";

    public static void main(String[] args) throws Exception {

        HttpAutomationClient client = new HttpAutomationClient(String.format(url, host));

        try {
            Session session = client.getSession(user, pwd);
            Assert.assertNotNull(session);

            OperationRequest fecthDoc = session.newRequest("Document.FetchLiveDocument").set("value", "b7ec0e1a-4466-419e-b058-7f8481652518");
            Document parent = (Document) fecthDoc.execute();

            OperationRequest request = session.newRequest(QueryES.ID);
            request.set("query", String.format(query, "b7ec0e1a-4466-419e-b058-7f8481652518"));
            request.setHeader(Constants.HEADER_NX_SCHEMAS, "*");

            // if (results instanceof Documents) {
            JsonMarshalling.addMarshaller(new EsMarshaller());
            Documents documents = (Documents) request.execute();
            System.out.println("Nb results: " + documents.size());

            Assert.assertTrue(null != documents);
            for (Document document : documents) {
                System.out.println(document.getTitle());
            }
            // } else if (results instanceof IterableQueryResult) {
            // JsonMarshalling.addMarshaller(new EsMarshaller());
            // IterableQueryResult rows = (IterableQueryResult) results;
            // System.out.println("Nb results: " + rows.size());
            //
            // Iterator<Map<String, Serializable>> iterator = rows.iterator();
            // while (iterator.hasNext()) {
            // Map<String, Serializable> row = iterator.next();
            // for (Entry<String, Serializable> entry : row.entrySet()) {
            // System.out.println(entry.getKey() + " | " + entry.getValue());
            // }
            // }
            // }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            if (null != client) {
                client.shutdown();
            }
        }
    }

}