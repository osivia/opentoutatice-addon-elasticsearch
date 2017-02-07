/**
 * 
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

import org.apache.commons.lang.StringUtils;
import org.nuxeo.runtime.api.Framework;


/**
 * @author david
 *
 */
public class QueryModeFilter implements Filter {

    /** Querying using always ES. */
    public static final String QUERYING_ES_FORCE = "ottc.querying.es.force";
    /** Exception of QUERYING_FORCE_ES: if set to true, force given request in VCS. */
    public static final String QUERYING_VCS_FORCE_FLAG = "nx_querying_vcs_force";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // Nothing
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        // Case of not HttpServeltRequest
        if (request instanceof HttpServletRequest == false) {
            chain.doFilter(request, response);
            return;
        }

        // Check operation
        HttpServletRequest httpReq = (HttpServletRequest) request;
        String opId = StringUtils.substringAfterLast(httpReq.getPathInfo(), "/");

        if ("Document.Query".equals(opId)) {
            // Get ElasticSearch querying mode from configuration
            final boolean queryingEsFromConfig = Boolean.valueOf(Framework.getProperty(QUERYING_ES_FORCE, "true"));

            // Get ElasticSearch querying mode from header
            final boolean queryingVcs = Boolean.valueOf(httpReq.getHeader(QUERYING_VCS_FORCE_FLAG));

            if (queryingEsFromConfig) {
                if (!queryingVcs) {
                    // Redirect all Document.Query by Document.QueryES
                    httpReq.getRequestDispatcher("/site/automation/Document.QueryES").forward(request, response);
                }
            }

        } else {
            chain.doFilter(request, response);
        }

    }

    @Override
    public void destroy() {
        // Nothing
    }

}
