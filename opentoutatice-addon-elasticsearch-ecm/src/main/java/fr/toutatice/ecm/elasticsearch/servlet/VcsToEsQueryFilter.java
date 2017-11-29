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
import org.nuxeo.ecm.automation.core.operations.services.DocumentPageProviderOperation;
import org.nuxeo.ecm.automation.core.operations.services.query.DocumentPaginatedQuery;

/**
 * @author david
 *
 */
public class VcsToEsQueryFilter implements Filter {

    /**
     * Exception of QUERYING_FORCE_ES: if set to true, force given request in VCS.
     */
    public static final String QUERYING_VCS_FORCE_FLAG = "nx_querying_vcs_force";

    /** Operation resource of Document.QueryES. */
    private static final String QUERY_ES_OP_RESOURCE = "/site/automation/Document.QueryES";

    /**
     * Document.QueryES compatibility mode (set schema in parameter is deprecated).
     */
    public static final String QUERY_ES_COMPAT_MODE = "qEsCompat";

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

        HttpServletRequest httpReq = (HttpServletRequest) request;

        // Get ElasticSearch querying mode from header
        final boolean queryingVcs = Boolean.valueOf(httpReq.getHeader(QUERYING_VCS_FORCE_FLAG));

        // Called operation
        String opId = StringUtils.substringAfterLast(httpReq.getPathInfo(), "/");

        if (!queryingVcs) {
            // Check operation

            if (DocumentPaginatedQuery.ID.equals(opId) || DocumentPageProviderOperation.ID.equals(opId)) {
                // Redirect all Document.Query by Document.QueryES
                httpReq.getRequestDispatcher(QUERY_ES_OP_RESOURCE).forward(request, response);
            } else {
                chain.doFilter(request, response);
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
