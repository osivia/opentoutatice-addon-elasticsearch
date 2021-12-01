/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.index;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;
import org.jsoup.helper.Validate;

/**
 * Nuxeo is configured with an Es index name <idx_name> but, for full
 * re-indexing with zero down time purpose, the real Es index name used by Nuxeo
 * (and which is defined in Es) is of the form: <idx_name>-<number> where number
 * is manage by a BDD sequence.
 *
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
public class IndexName implements Serializable, Cloneable {

    private static final long serialVersionUID = -8717225631014747357L;

    /**
     * index name configured in elasticsearch-config.xml.
     */
    private String namePart;

    /**
     * Suffix value "sequenced like". null if index created before
     * installation of this full re-index plugin.
     */
    private Long suffix;

    public static final String SEPARATOR = "__";

    public IndexName(String fullName) {
        String[] parts = StringUtils.split(fullName, SEPARATOR);
        this.namePart = parts[0];
        if (parts.length == 2) {
            this.suffix = Long.valueOf(parts[1]);
        }
    }

    public IndexName(String name, Long suffix) {
        this.namePart = name;
        this.suffix = suffix;
    }

    /**
     * @return <index-name> or <index-name>-<long_value>
     */
    public String value() {
        Validate.notNull(this.namePart);
        return this.suffix != null ? this.namePart.concat(SEPARATOR).concat(this.suffix.toString()) : this.namePart;
    }

    @Override
    public String toString() {
        return this.value();
    }

    @Override
    public IndexName clone() {
        IndexName clone = null;
        try {
            clone = (IndexName) super.clone();
        } catch (CloneNotSupportedException e) {
            clone = new IndexName(this.namePart, this.suffix);
        }

        return clone;
    }

    public String getNamePart() {
        return this.namePart;
    }

    public Long getSuffix() {
        return this.suffix;
    }

    public boolean hasSuffix() {
        return this.suffix != null;
    }

}
