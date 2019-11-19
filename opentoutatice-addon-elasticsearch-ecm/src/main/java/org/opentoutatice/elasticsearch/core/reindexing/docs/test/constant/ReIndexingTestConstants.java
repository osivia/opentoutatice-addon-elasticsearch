/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.test.constant;

/**
 * For JUnit tests only.
 *
 * @author david
 *
 */
public interface ReIndexingTestConstants {

    String FIRE_TEST_ERRORS_ON_STEP_PROP = "ottc.reindexing.test.mode.fire.errors.step";
    String FIRE_TEST_ERRORS_ON_STEP_RECOVERY_PROP = "ottc.reindexing.test.mode.fire.errors.step.recovery";
    String CREATE_ALIAS_N_INDEX_ON_STARTUP_TEST = "ottc.reindexing.test.create.index.alias.on.startup";
    String CREATE_BAD_ALIAS_N_INDEX_ON_STARTUP_TEST = "ottc.reindexing.test.create.bad.index.alias.on.startup";
    String CREATE_BAD_FORMER_ALIAS_ON_STARTUP_TEST = "ottc.reindexing.test.create.bad.former.alias.on.startup";
    String CREATE_READ_ALIAS_ON_STARTUP_TEST = "ottc.reindexing.test.create.read.alias.on.startup";

}
