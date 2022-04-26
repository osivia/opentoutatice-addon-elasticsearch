/**
 *
 */
package org.opentoutatice.elasticsearch.reindexing.docs.config;

import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.opentoutatice.elasticsearch.core.reindexing.docs.constant.ReIndexingConstants;
import org.opentoutatice.elasticsearch.core.reindexing.docs.test.constant.ReIndexingTestConstants;

/**
 * @author david
 *
 */
public class ZeroDownTimeErrorsConfigFeature extends ZeroDownTimeConfigFeature {

    @Override
    public void initialize(FeaturesRunner runner) throws Exception {
        super.initialize(runner);
        System.setProperty(ReIndexingConstants.REINDEXING_WAIT_LOOP_TIME, "1");
    }
    
    @Override
    public void stop(FeaturesRunner runner) throws Exception {
        System.setProperty(ReIndexingTestConstants.FIRE_TEST_ERRORS_ON_STEP_PROP, "none");
        System.setProperty(ReIndexingTestConstants.FIRE_TEST_ERRORS_ON_STEP_RECOVERY_PROP, "none");
    }

}
