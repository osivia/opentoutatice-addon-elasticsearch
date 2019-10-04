/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception;

/**
 * @author david
 *
 */
public class ReIndexingStatusException extends Exception {

    private static final long serialVersionUID = -3293521454398050334L;

    public ReIndexingStatusException(String msg) {
        super(msg);
    }

    public ReIndexingStatusException(Exception e) {
        super(e);
    }

}
