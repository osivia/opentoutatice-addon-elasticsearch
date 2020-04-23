/**
 * 
 */
package org.opentoutatice.elasticsearch.fulltext.constants;

import java.util.regex.Pattern;

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
    String PRE_TAG = Framework.getProperty("ottc.fulltext.query.highlight.pre.tag", "<span class=\"highlight\">");
    String POST_TAG = Framework.getProperty("ottc.fulltext.query.highlight.post.tag", "</span>");
    // Es default value: 100
    String FGTS_SIZE_STR = Framework.getProperty("ottc.fulltext.query.highlight.fragments.size");
    Integer FGTS_SIZE = FGTS_SIZE_STR != null ? Integer.valueOf(FGTS_SIZE_STR) : null;
    // Es default value: 5
    String FGTS_NB_STR = Framework.getProperty("ottc.fulltext.query.highlight.fragments.number");
    Integer FGTS_NB = FGTS_NB_STR != null ? Integer.valueOf(FGTS_NB_STR) : null;

}
