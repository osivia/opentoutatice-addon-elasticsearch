/**
 * 
 */
package org.tst.opentoutatice.elasticsearch.reindexing.docs;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.opentoutatice.elasticsearch.reindexing.docs.bad.initial.state.ZeroDownTimeReIndexingBadInitialAliasTst;
import org.opentoutatice.elasticsearch.reindexing.docs.bad.initial.state.ZeroDownTimeReIndexingBadInitialFormerAliasTst;
import org.opentoutatice.elasticsearch.reindexing.docs.bad.initial.state.ZeroDownTimeReIndexingInitialReadAliasExistsTst;
import org.opentoutatice.elasticsearch.reindexing.docs.bad.initial.state.ZeroDownTimeReIndexingNoAliasAtStartUpTst;

/**
 * @author david
 */
@RunWith(Suite.class)
@SuiteClasses({ZeroDownTimeReIndexingTst.class, ZeroDownTimeReIndexingErrorsCasesTst.class, ZeroDownTimeReIndexingBadInitialAliasTst.class,
    ZeroDownTimeReIndexingBadInitialFormerAliasTst.class, ZeroDownTimeReIndexingInitialReadAliasExistsTst.class, ZeroDownTimeReIndexingNoAliasAtStartUpTst.class})
public class SuitesTest {

}
