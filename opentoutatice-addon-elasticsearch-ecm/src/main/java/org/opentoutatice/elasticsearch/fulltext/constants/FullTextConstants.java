/**
 * 
 */
package org.opentoutatice.elasticsearch.fulltext.constants;

import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.nuxeo.runtime.api.Framework;

/**
 * @author david
 *
 */
public interface FullTextConstants {
	
	Pattern FULLTEXT_QUERY_INDICATOR = Pattern.compile(".+/\\*\\+ES: .*FULLTEXT\\((.+)\\) \\*/.*");
	Pattern FULLTEXT_QUERY_FIELDS = Pattern.compile(".+ +(WHERE|where) +/\\*\\+ES:( +FIELDS\\((.+)\\))? +FULLTEXT\\((.+)\\) +\\*/ +(and|AND|or|OR)? +(.*)");
    String COMMA = ",";
    String UPPER = "^";
    
    // Highlight
    String PRE_TAG = Framework.getProperty("ottc.fulltext.query.highlight.pre.tag", "<hltg class=\"highlight\">");
    String POST_TAG = Framework.getProperty("ottc.fulltext.query.highlight.post.tag", "</hltg>");
    // (Es default value: 100)
    String FGTS_SIZE_STR = Framework.getProperty("ottc.fulltext.query.highlight.fragments.size");
    Integer FGTS_SIZE = StringUtils.isNotBlank(FGTS_SIZE_STR) ? Integer.valueOf(FGTS_SIZE_STR) : 150;
    // (Es default value: 5)
    String FGTS_NB_STR = Framework.getProperty("ottc.fulltext.query.highlight.fragments.number");
    Integer FGTS_NB = StringUtils.isNotBlank(FGTS_NB_STR) ? Integer.valueOf(FGTS_NB_STR) : 3;
    
    // Fuzzyness
    String FUZZINESS_STR = Framework.getProperty("ottc.fulltext.query.fuzzyness");
    Integer FUZZINESS = StringUtils.isNotBlank(FUZZINESS_STR) ? Integer.valueOf(FUZZINESS_STR) : 1;
    String FUZZINESS_MAX_EXPANSIONS_STR = Framework.getProperty("ottc.fulltext.query.fuzzyness.max.expansions");
    Integer FUZZINESS_MAX_EXPANSIONS = StringUtils.isNotBlank(FUZZINESS_MAX_EXPANSIONS_STR) ? Integer.valueOf(FUZZINESS_MAX_EXPANSIONS_STR) : 10;
    String FUZZINESS_PREFIX_LENGTH_STR = Framework.getProperty("ottc.fulltext.query.fuzzyness.prefix.length");
    Integer FUZZINESS_PREFIX_LENGTH = StringUtils.isNotBlank(FUZZINESS_PREFIX_LENGTH_STR) ? Integer.valueOf(FUZZINESS_PREFIX_LENGTH_STR) : 3;
    
}
