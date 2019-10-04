/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.manager.cfg;

import org.apache.commons.lang.Validate;
import org.opentoutatice.elasticsearch.config.OttcElasticSearchIndexOrAliasConfig;
import org.opentoutatice.elasticsearch.core.reindexing.docs.index.IndexName;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.IndexNAliasManager;

/**
 * @author david
 *
 */
public class ReIndexingConfig {

    private OttcElasticSearchIndexOrAliasConfig nxAliasCfg;

    private IndexName initialIndex;
    private IndexName newIndex;

    public ReIndexingConfig(OttcElasticSearchIndexOrAliasConfig nxAliasCfg) {
        super();
        this.build(nxAliasCfg);
    }

    public synchronized void build(OttcElasticSearchIndexOrAliasConfig nxAliasCfg) {
        this.nxAliasCfg(nxAliasCfg);
        // Initialize indices attributes
        String indexName = IndexNAliasManager.get().getIndexOfAlias(nxAliasCfg.getAliasName());
        this.setInitialIndex(new IndexName(indexName));
        this.setNewIndex(this.buildNewIndexName(this.getInitialIndex()));
    }

    public IndexName buildNewIndexName(IndexName initialIndexName) {
        Validate.notNull(initialIndexName);
        return new IndexName(initialIndexName.getNamePart(), System.currentTimeMillis());
    }

    public OttcElasticSearchIndexOrAliasConfig getNxAliasCfg() {
        return this.nxAliasCfg;
    }

    public ReIndexingConfig nxAliasCfg(OttcElasticSearchIndexOrAliasConfig nxAliasCfg) {
        this.nxAliasCfg = nxAliasCfg;
        return this;
    }

    public IndexName getInitialIndex() {
        return this.initialIndex;
    }

    public void setInitialIndex(IndexName initialIndex) {
        this.initialIndex = initialIndex;
    }

    public IndexName getNewIndex() {
        return this.newIndex;
    }

    private void setNewIndex(IndexName newIndex) {
        this.newIndex = newIndex;
    }

}