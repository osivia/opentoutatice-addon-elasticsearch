/**
 *
 */
package org.opentoutatice.elasticsearch.reindexing.docs.config;

import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.SimpleFeature;
import org.opentoutatice.elasticsearch.core.reindexing.docs.constant.ReIndexingConstants;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.IndexNAliasManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.ReIndexingRunnerManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.test.constant.ReIndexingTestConstants;

/**
 * @author david
 *
 */
public class ZeroDownTimeConfigFeature extends SimpleFeature {
    
    @Override
    public void initialize(FeaturesRunner runner) throws Exception {
        System.setProperty(Framework.NUXEO_TESTING_SYSTEM_PROP, String.valueOf(true));

        System.setProperty(ReIndexingTestConstants.CREATE_ALIAS_N_INDEX_ON_STARTUP_TEST, "true");

        System.setProperty("elasticsearch.reindex.bucketReadSize", "1");
        System.setProperty("elasticsearch.reindex.bucketWriteSize", "1");

        // To "suspend" re-indexing process
        System.setProperty(ReIndexingConstants.REINDEXING_WAIT_LOOP_TIME, "3");
        
    }
    
    @Override
    public void afterRun(FeaturesRunner runner) throws Exception {
        IndexNAliasManager.reset();
        ReIndexingRunnerManager.reset();
    }

}
