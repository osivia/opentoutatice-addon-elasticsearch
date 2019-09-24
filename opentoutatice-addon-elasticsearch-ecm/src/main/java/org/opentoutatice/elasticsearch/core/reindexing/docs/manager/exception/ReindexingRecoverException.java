/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception;

/**
 * @author david
 *
 */
public class ReindexingRecoverException extends Exception {

	private static final long serialVersionUID = 1390974727392989214L;
	
	public ReindexingRecoverException(Exception e) {
		super(e);
	}
	
	public ReindexingRecoverException(String msg, Exception e) {
		super(msg, e);
	}

}
