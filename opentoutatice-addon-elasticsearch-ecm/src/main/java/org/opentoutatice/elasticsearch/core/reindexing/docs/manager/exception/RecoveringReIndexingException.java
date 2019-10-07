/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception;

/**
 * @author david
 *
 */
public class RecoveringReIndexingException extends ReIndexingException {

    private static final long serialVersionUID = 1390974727392989214L;
    
    public RecoveringReIndexingException(String msg) {
        super(msg);
    }

    public RecoveringReIndexingException(Exception e) {
        super(e);
    }

    public RecoveringReIndexingException(String msg, Exception e) {
        super(msg, e);
    }

}
