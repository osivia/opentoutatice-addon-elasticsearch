/*
 * (C) Copyright 2014 Académie de Rennes (http://www.ac-rennes.fr/), OSIVIA (http://www.osivia.com) and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * Contributors:
 * mberhaut1
 */
package fr.toutatice.ecm.elasticsearch.codec;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.elasticsearch.action.search.SearchResponse;
import org.nuxeo.ecm.automation.io.services.codec.ObjectCodec;

public class TTCRawEsCodec extends ObjectCodec<SearchResponse> {

	// private static final Log log = LogFactory.getLog(TTCEsCodec.class);

	public TTCRawEsCodec() {
		super(SearchResponse.class);
	}

	@Override
	public String getType() {
		return "rawesresponse";
	}

	@Override
	public void write(JsonGenerator jg, SearchResponse searchResponse) throws IOException {
		jg.writeRawValue(searchResponse.toString());
		jg.flush();
	}

}
