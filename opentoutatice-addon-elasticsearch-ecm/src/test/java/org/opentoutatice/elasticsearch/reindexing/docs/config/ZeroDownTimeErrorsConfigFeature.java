/**
 *
 */
package org.opentoutatice.elasticsearch.reindexing.docs.config;

import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.opentoutatice.elasticsearch.core.reindexing.docs.constant.ReIndexingConstants;

/**
 * @author david
 *
 */
public class ZeroDownTimeErrorsConfigFeature extends ZeroDownTimeConfigFeature {

    @Override
    public void initialize(FeaturesRunner runner) throws Exception {
        super.initialize(runner);
        System.setProperty(ReIndexingConstants.REINDEXING_WAIT_LOOP_TIME, "1500");
    }

}
