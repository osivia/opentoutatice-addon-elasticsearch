/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception;

/**
 * @author david
 *
 */
public class ReIndexingStateException extends Exception {

    private static final long serialVersionUID = -1509000314180524534L;

    public ReIndexingStateException(String msg) {
        super(msg);
    }

    public ReIndexingStateException(Exception e) {
        super(e);
    }

    public ReIndexingStateException(String msg, Exception e) {
        super(msg, e);
    }

}
