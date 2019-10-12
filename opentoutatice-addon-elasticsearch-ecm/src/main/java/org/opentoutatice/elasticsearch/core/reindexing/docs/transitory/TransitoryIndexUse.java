/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.transitory;

/**
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
public enum TransitoryIndexUse {

    Read("r-alias"), Write("w-alias");

    private String alias;

    private TransitoryIndexUse(String alias) {
        this.alias = alias;
    }

    public String getAlias() {
        return this.alias;
    }
}
