package org.opentoutatice.elasticsearch.reindexing.docs.config;

import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.SimpleFeature;
import org.opentoutatice.elasticsearch.core.reindexing.docs.constant.ReIndexingConstants;

public class ZeroDownTimeReadWriteConfigFeature extends SimpleFeature {
	
	@Override
	public void initialize(FeaturesRunner runner) throws Exception {
		System.setProperty(Framework.NUXEO_TESTING_SYSTEM_PROP, String.valueOf(true));
		
		System.setProperty("elasticsearch.reindex.bucketReadSize", "1");
		System.setProperty("elasticsearch.reindex.bucketWriteSize", "1");
		
		// To "suspend" re-indexing process
		System.setProperty(ReIndexingConstants.REINDEXING_WAIT_LOOP_TIME, "5000");
	}

}
