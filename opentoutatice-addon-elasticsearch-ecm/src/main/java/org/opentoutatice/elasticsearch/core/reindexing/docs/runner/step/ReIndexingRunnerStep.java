/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.runner.step;

/**
 * @author david
 *
 */
public enum ReIndexingRunnerStep {

    none(null), initialization(ReIndexingRunnerStepState.notStarted), indexing(ReIndexingRunnerStepState.notStarted), switching(
            ReIndexingRunnerStepState.notStarted);

    private ReIndexingRunnerStepState stepState;

    private ReIndexingRunnerStep(ReIndexingRunnerStepState stepState) {
        this.stepState = stepState;
    }

    public ReIndexingRunnerStepState getStepState() {
        return this.stepState;
    }

    public ReIndexingRunnerStep stepState(ReIndexingRunnerStepState stepState) {
        this.stepState = stepState;
        return this;
    }

}
