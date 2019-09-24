/**
 * 
 */
package org.opentoutatice.elasticsearch.reindexing.docs.config;

import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.SimpleFeature;
import org.opentoutatice.elasticsearch.core.reindexing.docs.constant.ReIndexingConstants;

/**
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
public class ZeroDownTimeConfigFeature extends SimpleFeature {

	@Override
	public void initialize(FeaturesRunner runner) throws Exception {
		System.setProperty(Framework.NUXEO_TESTING_SYSTEM_PROP, String.valueOf(true));
		
		System.setProperty("elasticsearch.reindex.bucketReadSize", "1");
		System.setProperty("elasticsearch.reindex.bucketWriteSize", "1");
		
		// In test mode, time out in millis
		System.setProperty(ReIndexingConstants.REINDEXING_WAIT_LOOP_TIME, "300");
	}

}
