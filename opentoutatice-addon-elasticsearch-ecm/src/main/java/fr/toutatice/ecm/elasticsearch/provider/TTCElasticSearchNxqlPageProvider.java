/**
 *
 */
package fr.toutatice.ecm.elasticsearch.provider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.ClientRuntimeException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.query.QueryParseException;
import org.nuxeo.ecm.platform.query.api.Aggregate;
import org.nuxeo.ecm.platform.query.api.AggregateDefinition;
import org.nuxeo.ecm.platform.query.api.Bucket;
import org.nuxeo.elasticsearch.aggregate.AggregateEsBase;
import org.nuxeo.elasticsearch.aggregate.AggregateFactory;
import org.nuxeo.elasticsearch.api.ElasticSearchService;
import org.nuxeo.elasticsearch.api.EsResult;
import org.nuxeo.elasticsearch.provider.ElasticSearchNxqlPageProvider;
import org.nuxeo.elasticsearch.query.NxQueryBuilder;
import org.nuxeo.runtime.api.Framework;

import fr.toutatice.ecm.elasticsearch.query.TTCNxQueryBuilder;


/**
 * @author david
 *
 */
public class TTCElasticSearchNxqlPageProvider extends ElasticSearchNxqlPageProvider {

    private static final long serialVersionUID = 5785692705211116081L;

    // FIXME: just to use TTCNxQueryBuilder ...
    @Override
    public List<DocumentModel> getCurrentPage() {
        // use a cache
        if (this.currentPageDocuments != null) {
            return this.currentPageDocuments;
        }
        this.error = null;
        this.errorMessage = null;
        if (log.isDebugEnabled()) {
            log.debug(String.format("Perform query for provider '%s': with pageSize=%d, offset=%d", this.getName(), this.getMinMaxPageSize(),
                    this.getCurrentPageOffset()));
        }
        this.currentPageDocuments = new ArrayList<DocumentModel>();
        CoreSession coreSession = this.getCoreSession();
        if (this.query == null) {
            this.buildQuery(coreSession);
        }
        if (this.query == null) {
            throw new ClientRuntimeException(String.format("Cannot perform null query: check provider '%s'", this.getName()));
        }
        // Build and execute the ES query
        ElasticSearchService ess = Framework.getLocalService(ElasticSearchService.class);
        try {
            NxQueryBuilder nxQuery = new TTCNxQueryBuilder(this.getCoreSession()).setAutomationCall(false).nxql(this.query)
                    .offset((int) this.getCurrentPageOffset()).limit(this.getLimit()).addAggregates(this.buildAggregates());

            if (this.searchOnAllRepositories()) {
                nxQuery.searchOnAllRepositories();
            }
            EsResult ret = ess.queryAndAggregate(nxQuery);
            DocumentModelList dmList = ret.getDocuments();
            this.currentAggregates = new HashMap<>(ret.getAggregates().size());
            for (Aggregate<Bucket> agg : ret.getAggregates()) {
                this.currentAggregates.put(agg.getId(), agg);
            }
            this.setResultsCount(dmList.totalSize());
            this.currentPageDocuments = dmList;
        } catch (ClientException | QueryParseException e) {
            this.error = e;
            this.errorMessage = e.getMessage();
            log.warn(e.getMessage(), e);
        }
        return this.currentPageDocuments;
    }

    private List<AggregateEsBase<? extends Bucket>> buildAggregates() {
        ArrayList<AggregateEsBase<? extends Bucket>> ret = new ArrayList<>(this.getAggregateDefinitions().size());
        for (AggregateDefinition def : super.getAggregateDefinitions()) {
            ret.add(AggregateFactory.create(def, this.getSearchDocumentModel()));
        }
        return ret;
    }
}
