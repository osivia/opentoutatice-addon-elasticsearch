/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.constant;

/**
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
public interface ReIndexingConstants {
	
	/**
	 * Loop timeout for re-indexing waiting in seconds.
	 * In test mode, time unit is milliseconds.
	 */
	String REINDEXING_WAIT_LOOP_TIME = "ottc.reindexing.check.loop.period";
	
}
