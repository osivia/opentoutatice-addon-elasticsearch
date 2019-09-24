/**
 * 
 */
package org.opentoutatice.elasticsearch.api;

import java.util.Map;

import org.nuxeo.elasticsearch.api.ElasticSearchAdmin;

/**
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
public interface OttcElasticSearchAdmin extends ElasticSearchAdmin {
	
	Map<String, String> getIndexNames();

	Map<String, String> getRepoNames();
	
	boolean isZeroDownTimeReIndexingInProgress(String repository) throws InterruptedException;

}
