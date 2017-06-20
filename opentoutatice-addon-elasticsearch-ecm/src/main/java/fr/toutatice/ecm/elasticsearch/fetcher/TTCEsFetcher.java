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
package fr.toutatice.ecm.elasticsearch.fetcher;

import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.impl.DocumentModelListImpl;
import org.nuxeo.elasticsearch.fetcher.Fetcher;

public class TTCEsFetcher extends Fetcher {

	public TTCEsFetcher(CoreSession session, SearchResponse response, Map<String, String> repoNames) {
		super(session, response, repoNames);
	}

	@Override
	public DocumentModelListImpl fetchDocuments() {
        // No DocumentModel fetch is desired anymore in automation call case
        return new DocumentModelListImpl();
	}
	
    public SearchResponse getResponse() {
        return super.getResponse();
    }
    
}
