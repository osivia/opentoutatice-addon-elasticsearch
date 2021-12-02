/**
 *
 */
package org.opentoutatice.elasticsearch.utils;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author david
 */
public class MessageUtils {

    /**
     * @param duplicateIds
     * @return
     */
    public static String listToString(Collection<String> duplicateIds) {
        StringBuffer sb = new StringBuffer();
        Iterator<String> duplicatesIt = duplicateIds.iterator();
        while (duplicatesIt.hasNext()) {
            String dupId = duplicatesIt.next();
            sb.append(dupId);
            if (duplicatesIt.hasNext()) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

}
