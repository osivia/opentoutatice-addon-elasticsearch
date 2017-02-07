/*
 * (C) Copyright 2016 Nuxeo SA (http://nuxeo.com/) and others.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Contributors:
 * "Guillaume Renard"
 */

package fr.toutatice.ecm.elasticsearch.servlet;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.nuxeo.elasticsearch.listener.ElasticSearchInlineListener;
import org.nuxeo.runtime.api.Framework;

/**
 * Filter to control mode of ES use:
 * <ul>
 * <li>always or not when querying</li>
 * <li>in a synchronous way or not</li>
 * </ul>
 */
public class ElasticSearchModeFilter implements Filter {

    /** Mode of treatment of ES command: always synchronous or not. */
    public static final String ES_INDEXING_SYNC_FORCE = "ottc.es.indexing.sync.force";
    /** Exception of ES_INDEXING_SYNC_FORCE: if set to true, force asynchronous indexing. */
    public static final String ES_INDEXING_ASYNC_FORCE_FLAG = "nx_es_indexing_async_force";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        // Case of not HttpServeltRequest
        if (request instanceof HttpServletRequest == false) {
            chain.doFilter(request, response);
            return;
        }

        // Get ElasticSearch indexing mode (synchronous) according to configuration
        final boolean syncEsFromConfig = Boolean.valueOf(Framework.getProperty(ES_INDEXING_SYNC_FORCE, "true"));
        
        // Get ElasticSearch indexing mode (synchronous) according to header flag
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        final boolean asyncEsFromHeader = Boolean.valueOf(httpRequest.getHeader(ES_INDEXING_ASYNC_FORCE_FLAG));
        
        // Set indexing status: if forced from configuration, check header
        boolean syncEs = syncEsFromConfig && asyncEsFromHeader ? asyncEsFromHeader : syncEsFromConfig;

        ElasticSearchInlineListener.useSyncIndexing.set(syncEs);
        try {
            chain.doFilter(request, response);
        } finally {
            // false or not ...
            ElasticSearchInlineListener.useSyncIndexing.set(false);
        }

    }

    @Override
    public void destroy() {
    }

}
