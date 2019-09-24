/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs;

/**
 * @author david
 *
 */
public enum ReIndexingRunnerStepState {
	
	notStarted(null), started(null), inProgress(null), done(null);
	
	private ReIndexingRunnerStepStateStatus stateStatus;
	
	private ReIndexingRunnerStepState(ReIndexingRunnerStepStateStatus stateStatus) {
		this.stateStatus = stateStatus;
	}

	public ReIndexingRunnerStepStateStatus getStepStatus() {
		return this.stateStatus;
	}
	
	public ReIndexingRunnerStepState stepStatus(ReIndexingRunnerStepStateStatus stateStatus) {
		this.stateStatus = stateStatus;
		return this;
	}
	 
}
