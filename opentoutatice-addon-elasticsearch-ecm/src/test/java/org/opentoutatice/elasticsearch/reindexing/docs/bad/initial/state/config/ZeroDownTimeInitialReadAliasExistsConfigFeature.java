/**
 *
 */
package org.opentoutatice.elasticsearch.reindexing.docs.bad.initial.state.config;

import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.opentoutatice.elasticsearch.core.reindexing.docs.test.constant.ReIndexingTestConstants;
import org.opentoutatice.elasticsearch.reindexing.docs.config.ZeroDownTimeConfigFeature;

/**
 * @author david
 *
 */
public class ZeroDownTimeInitialReadAliasExistsConfigFeature extends ZeroDownTimeConfigFeature {

    @Override
    public void initialize(FeaturesRunner runner) throws Exception {
        super.initialize(runner);
        System.setProperty(ReIndexingTestConstants.CREATE_READ_ALIAS_ON_STARTUP_TEST, "true");
    }
    
    @Override
    public void stop(FeaturesRunner runner) throws Exception {
        System.setProperty(ReIndexingTestConstants.CREATE_READ_ALIAS_ON_STARTUP_TEST, "false");
    }

}
