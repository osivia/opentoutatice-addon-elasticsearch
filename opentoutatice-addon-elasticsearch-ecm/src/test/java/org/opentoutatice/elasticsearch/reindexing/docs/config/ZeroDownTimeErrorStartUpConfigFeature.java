/**
 *
 */
package org.opentoutatice.elasticsearch.reindexing.docs.config;

import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.opentoutatice.elasticsearch.core.reindexing.docs.test.constant.ReIndexingTestConstants;

/**
 * @author david
 *
 */
public class ZeroDownTimeErrorStartUpConfigFeature extends ZeroDownTimeConfigFeature {

    @Override
    public void initialize(FeaturesRunner runner) throws Exception {
        // No alias and index creation at startup
        System.setProperty(ReIndexingTestConstants.CREATE_ALIAS_N_INDEX_ON_STARTUP_TEST, "false");
    }

}
