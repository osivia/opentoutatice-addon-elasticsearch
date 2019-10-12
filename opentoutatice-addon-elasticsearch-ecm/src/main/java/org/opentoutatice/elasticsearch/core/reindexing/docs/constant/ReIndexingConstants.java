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
     */
    String REINDEXING_WAIT_LOOP_TIME = "ottc.reindexing.check.loop.period";
    
    String REINDEXING_MANEGR_QUEUE_ID = "zeroDownTimeEsReIndexingManager";
    
    String RENDEXING_QUEUE_ID = "zeroDownTimeEsReIndexing";

}
