/**
 *
 */
package org.opentoutatice.elasticsearch.reindexing.docs.feature;

import org.junit.runners.model.FrameworkMethod;
import org.nuxeo.elasticsearch.test.RepositoryElasticSearchFeature;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.transaction.TransactionHelper;

/**
 * @author david
 *
 */
public class RepositoryElasticSearchDeletingNodeFeature extends RepositoryElasticSearchFeature {

    @Override
    public void afterMethodRun(FeaturesRunner runner, FrameworkMethod method, Object test) throws Exception {
        // make sure there is an active Tx to do the cleanup, so we don't hide previous assertion
        if (!TransactionHelper.isTransactionActive()) {
            TransactionHelper.startTransaction();
        }
        super.afterMethodRun(runner, method, test);
    }

}
