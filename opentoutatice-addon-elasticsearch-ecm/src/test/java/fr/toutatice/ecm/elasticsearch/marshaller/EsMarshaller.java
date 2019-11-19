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
package fr.toutatice.ecm.elasticsearch.marshaller;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.nuxeo.ecm.automation.client.jaxrs.spi.JsonMarshaller;
import org.nuxeo.ecm.automation.client.jaxrs.spi.JsonMarshalling;
import org.nuxeo.ecm.automation.client.model.Documents;

public class EsMarshaller implements JsonMarshaller<Documents> {

    @Override
    public String getType() {
        return "esresponse";
    }

    @Override
    public Class<Documents> getJavaType() {
        return Documents.class;
    }

    @Override
    public Documents read(JsonParser jp) throws Exception {
        jp.nextToken();
        String key = jp.getCurrentName();
        if ("value".equals(key)) {
            jp.nextToken(); // '{'
            jp.nextToken(); // hopefully "entity-type"
            jp.nextToken(); // its value
            String etype = jp.getText();
            JsonMarshaller<?> jm = JsonMarshalling.getMarshaller(etype);
            if (null != jm) {
                return (Documents) jm.read(jp);
            }
        } else {
            throw new Exception("missing 'value' filed");
        }

        return null;
    }

    @Override
    public void write(JsonGenerator jg, Documents value) throws Exception {
        // TODO Auto-generated method stub

    }

}