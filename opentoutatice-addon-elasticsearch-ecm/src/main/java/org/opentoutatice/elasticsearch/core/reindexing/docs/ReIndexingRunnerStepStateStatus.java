/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs;

import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;

/**
 * @author david
 *
 */
public enum ReIndexingRunnerStepStateStatus {
	
	successfull(null), inError(null);
	
	private ReIndexingException error;
	
	private ReIndexingRunnerStepStateStatus(ReIndexingException error) {
		this.error = error;
	}
	
	public ReIndexingException getError() {
		return this.error;
	}
	
	public ReIndexingRunnerStepStateStatus error(ReIndexingException error) {
		this.error = error;
		return this;
	}

}
