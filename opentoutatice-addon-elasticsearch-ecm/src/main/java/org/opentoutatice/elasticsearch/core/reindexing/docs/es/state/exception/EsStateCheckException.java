/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception;

/**
 * @author david
 *
 */
public class EsStateCheckException extends Exception {

	private static final long serialVersionUID = -3293521454398050334L;
	
	public EsStateCheckException(String msg) {
		super(msg);
	}

	public EsStateCheckException(Exception e) {
		super(e);
	}

}
