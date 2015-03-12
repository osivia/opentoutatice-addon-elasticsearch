package fr.toutatice.ecm.elasticsearch.automation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.automation.OperationException;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.core.api.Blob;

@Operation(id = QueryES.ID, category = Constants.CAT_FETCH, label = "Query  via ElasticSerach", description = "Perform a query on ElasticSerach instead of Repository")
public class QueryES {

	private static final Log log = LogFactory.getLog(QueryES.class);

    public static final String ID = "Document.QueryES";

    @Param(name = "query", required = false)
    protected String query;

    @Param(name = "page", required = false)
    protected Integer page;

    @Param(name = "currentPageIndex", required = false)
    protected Integer currentPageIndex;

    @Param(name = "pageSize", required = false)
    protected Integer pageSize;

    @OperationMethod
    public Blob run() throws OperationException {
    	return null;
    }
    
}
