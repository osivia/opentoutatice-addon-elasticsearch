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
package fr.toutatice.ecm.elasticsearch.search;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;

public class TTCSearchResponse {

    private Integer pageSize;
    private Integer currentPageIndex;
    /** Sroll API case. */
    private Set<SearchResponse> searchResponses;
    private List<String> schemas;

    public TTCSearchResponse(Set<SearchResponse> searchResponses, Integer pageSize, Integer currentPageIndex, List<String> schemas) {
        this.pageSize = pageSize;
        this.searchResponses = searchResponses;
        this.currentPageIndex = currentPageIndex;
        this.schemas = schemas;
    }

    public int getPageSize() {
        return pageSize.intValue();
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getCurrentPageIndex() {
        return currentPageIndex.intValue();
    }

    public void setCurrentPageIndex(int currentPageIndex) {
        this.currentPageIndex = currentPageIndex;
    }

    public Set<SearchResponse> getSearchResponses() {
        return searchResponses;
    }

    public void setSearchResponses(Set<SearchResponse> searchResponses) {
        this.searchResponses = searchResponses;
    }

    public List<String> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<String> schemas) {
        this.schemas = schemas;
    }

    public boolean isPaginable() {
        return ((null != pageSize) && (null != currentPageIndex));
    }

    public String getSchemasRegex() {
        return (null != schemas && 0 < schemas.size()) ? "^(" + StringUtils.join(schemas, "|") + "):.+$" : ".*";
    }

}
