/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.runner.step;

import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;

/**
 * @author david
 *
 */
public enum ReIndexingRunnerStepStateStatus {

    successfull("OK", null), inError("KO", null);

    private String msg;
    private ReIndexingException error;

    private ReIndexingRunnerStepStateStatus(String msg, ReIndexingException error) {
        this.msg = msg;
        this.error = error;
    }

    private ReIndexingRunnerStepStateStatus(ReIndexingException error) {
        this.error = error;
    }

    public String getMessage() {
        return this.msg;
    }

    public ReIndexingException getError() {
        return this.error;
    }

    public ReIndexingRunnerStepStateStatus error(ReIndexingException error) {
        this.error = error;
        return this;
    }

}
