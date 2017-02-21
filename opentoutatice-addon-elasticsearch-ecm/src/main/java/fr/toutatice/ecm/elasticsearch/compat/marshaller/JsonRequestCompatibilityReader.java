/**
 * 
 */
package fr.toutatice.ecm.elasticsearch.compat.marshaller;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.automation.client.Constants;
import org.nuxeo.ecm.automation.core.operations.services.DocumentPageProviderOperation;
import org.nuxeo.ecm.automation.core.operations.services.query.DocumentPaginatedQuery;
import org.nuxeo.ecm.automation.jaxrs.io.operations.ExecutionRequest;
import org.nuxeo.ecm.automation.jaxrs.io.operations.JsonRequestReader;

import fr.toutatice.ecm.elasticsearch.servlet.VcsToEsQueryFilter;


/**
 * Assures compatibility of Document.QueryES used with schemas set in parameter (deprecated)
 * with Document.QueryES used with schemas set in request header.
 * 
 * @author david
 *
 */
@Provider
@Consumes({"application/json", "application/json+nxrequest"})
public class JsonRequestCompatibilityReader extends JsonRequestReader {

    /** Log. */
    private static final Log log = LogFactory.getLog(JsonRequestCompatibilityReader.class);

    @Context
    private HttpServletRequest request;

    /**
     * Checks that request is a forward of QueryModeFilter.
     */
    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return super.isReadable(type, genericType, annotations, mediaType);
    }

    @Override
    public ExecutionRequest readFrom(Class<ExecutionRequest> type, Type genericType, Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders, InputStream entityStream) throws IOException, WebApplicationException {
        // Build ExecutionRequest
        ExecutionRequest xReq = super.readFrom(type, genericType, annotations, mediaType, httpHeaders, entityStream);

        // Check if marked as compatible request
        String opId = (String) this.request.getAttribute(VcsToEsQueryFilter.QUERY_ES_COMPAT_MODE);

        if (opId != null) {

            if (log.isDebugEnabled()) {
                log.debug("[" + opId + "]: to adapt");
            }

            switch (opId) {
                case DocumentPaginatedQuery.ID:
                    // Set schemas from header to parameter
                    xReq = setSchemasFromHeaderToParam(xReq);

                    if (log.isDebugEnabled()) {
                        log.debug("[" + opId + "]: adapted");
                    }

                    break;

                case DocumentPageProviderOperation.ID:
                    // Set schemas from header to parameter
                    xReq = setSchemasFromHeaderToParam(xReq);

                    // Adapt Document.PageProvider parameters to Document.QueryES parameters
                    Map<String, Object> params = xReq.getParams();
                    xReq.setParam("currentPageIndex", params.get("page"));

                    if (log.isDebugEnabled()) {
                        log.debug("[" + opId + "]: adapted");
                    }

                    break;
            }

        }

        return xReq;
    }

    /**
     * Set schemas from header to parameter
     * 
     * @param xReq
     * @return xReq
     */
    public ExecutionRequest setSchemasFromHeaderToParam(ExecutionRequest xReq) {
        String schemas = this.request.getHeader(Constants.HEADER_NX_SCHEMAS);
        xReq.setParam(Constants.HEADER_NX_SCHEMAS, schemas);

        return xReq;
    }


}
