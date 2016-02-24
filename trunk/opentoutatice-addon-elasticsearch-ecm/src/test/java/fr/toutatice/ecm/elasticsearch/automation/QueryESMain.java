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

import junit.framework.Assert;

import org.nuxeo.ecm.automation.client.Constants;
import org.nuxeo.ecm.automation.client.OperationRequest;
import org.nuxeo.ecm.automation.client.Session;
import org.nuxeo.ecm.automation.client.jaxrs.impl.HttpAutomationClient;
import org.nuxeo.ecm.automation.client.jaxrs.spi.JsonMarshalling;
import org.nuxeo.ecm.automation.client.model.Document;
import org.nuxeo.ecm.automation.client.model.Documents;

import fr.toutatice.ecm.elasticsearch.marshaller.EsMarshaller;

public class QueryESMain {

	public static void main(String[] args) throws Exception {
		HttpAutomationClient client = new HttpAutomationClient("http://localhost:8080/nuxeo/site/automation");

		try {
			Session session = client.getSession("nxberhaut", "BERHAUT");
			Assert.assertNotNull(session);

			OperationRequest request = session.newRequest(QueryES.ID);
			request.set("query", "SELECT * FROM Document WHERE dc:title LIKE 'l'arche%'");
			request.setHeader(Constants.HEADER_NX_SCHEMAS, "Dublincore");

			JsonMarshalling.addMarshaller(new EsMarshaller());			
			Documents documents = (Documents) request.execute();
			Assert.assertTrue(null != documents);
			for (Document document : documents) {
				System.out.println(document.getTitle());
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			if (null != client) {
				client.shutdown();
			}
		}
	}

}