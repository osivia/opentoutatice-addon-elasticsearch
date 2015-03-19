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
package fr.toutatice.ecm.elasticsearch.search;

import org.elasticsearch.action.search.SearchResponse;

public class TTCSearchResponse {

	private int pageSize;
	private int currentPageIndex;
	private SearchResponse searchResponse;

	public TTCSearchResponse(SearchResponse searchResponse, int pageSize, int currentPageIndex) {
		this.pageSize = pageSize;
		this.searchResponse = searchResponse;
		this.currentPageIndex = currentPageIndex;
	}
	
	public int getPageSize() {
		return pageSize;
	}
	
	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getCurrentPageIndex() {
		return currentPageIndex;
	}

	public void setCurrentPageIndex(int currentPageIndex) {
		this.currentPageIndex = currentPageIndex;
	}

	public SearchResponse getSearchResponse() {
		return searchResponse;
	}

	public void setSearchResponse(SearchResponse searchResponse) {
		this.searchResponse = searchResponse;
	}

}
