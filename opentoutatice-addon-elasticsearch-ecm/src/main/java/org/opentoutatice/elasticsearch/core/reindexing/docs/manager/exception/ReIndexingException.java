/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception;

/**
 * @author david
 *
 */
public class ReIndexingException extends Exception {

	private static final long serialVersionUID = 9167570665480723294L;
	
	public ReIndexingException(String msg) {
		super(msg);
	}
	
	public ReIndexingException(Exception e) {
		super(e);
	}
	
	public ReIndexingException(String msg, Exception e) {
		super(msg, e);
	}

}
