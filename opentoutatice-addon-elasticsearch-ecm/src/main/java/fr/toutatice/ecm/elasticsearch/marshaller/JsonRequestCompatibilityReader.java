/**
 * 
 */
package fr.toutatice.ecm.elasticsearch.marshaller;

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

import org.apache.commons.lang.StringUtils;
import org.nuxeo.ecm.automation.client.Constants;
import org.nuxeo.ecm.automation.jaxrs.io.operations.ExecutionRequest;
import org.nuxeo.ecm.automation.jaxrs.io.operations.JsonRequestReader;

import fr.toutatice.ecm.elasticsearch.servlet.QueryModeFilter;


/**
 * Assures compatibility of Document.QueryES used with schemas set in parameter (deprecated)
 * with Document.QueryES used with schemas set in request header.
 * 
 * @author david
 *
 */
@Provider
@Consumes({ "application/json+nxrequest" })
public class JsonRequestCompatibilityReader extends JsonRequestReader {
    
    @Context
    private HttpServletRequest request;
    
    /**
     * Checks that request is a forward of QueryModeFilter.
     */
    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        // Check compatibility mode in request
        boolean makeCompatible = StringUtils.isNotBlank((String) this.request.getAttribute(QueryModeFilter.QUERY_ES_COMPAT_MODE));
        
        // Just for robustness (must be true)
        boolean isRequest = JsonRequestReader.targetMediaTypeNXReq.isCompatible(mediaType) && ExecutionRequest.class.isAssignableFrom(type);
        
        return makeCompatible && isRequest;
    }

    @Override
    public ExecutionRequest readFrom(Class<ExecutionRequest> type, Type genericType, Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders, InputStream entityStream) throws IOException, WebApplicationException {
        // Build ExecutionRequest
        ExecutionRequest xReq = super.readFrom(type, genericType, annotations, mediaType, httpHeaders, entityStream);
        
        // Set schemas from header to parameter
        String schemas = this.request.getHeader(Constants.HEADER_NX_SCHEMAS);
        xReq.setParam(Constants.HEADER_NX_SCHEMAS, schemas);
        
        // Adapt Document.PageProvider parameters to Document.QueryES parameters
        Map<String, Object> params = xReq.getParams();
        xReq.setParam("currentPageIndex", params.get("page"));
        
        return xReq;
    }


}
