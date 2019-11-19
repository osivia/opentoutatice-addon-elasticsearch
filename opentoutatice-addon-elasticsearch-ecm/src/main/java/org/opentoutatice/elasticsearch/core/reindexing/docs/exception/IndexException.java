/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.exception;

/**
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
public class IndexException extends Exception {

    private static final long serialVersionUID = 6231243942965978804L;

    /**
     * @param message
     */
    public IndexException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public IndexException(Throwable cause) {
        super(cause);
    }


}
