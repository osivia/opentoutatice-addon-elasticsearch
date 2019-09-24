/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.manager.cfg;

import org.apache.commons.lang.Validate;
import org.opentoutatice.elasticsearch.config.OttcElasticSearchIndexOrAliasConfig;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.IndexNAliasManager;
import org.opentoutatice.elasticsearch.core.reindexing.docs.name.IndexName;

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
		build(nxAliasCfg);
	}

	public synchronized void build(OttcElasticSearchIndexOrAliasConfig nxAliasCfg) {
		nxAliasCfg(nxAliasCfg);
		// Initialize indices attributes
		String indexName = IndexNAliasManager.get().getIndexOfAlias(nxAliasCfg.getAliasName());
		setInitialIndex(new IndexName(indexName));
		setNewIndex(buildNewIndexName(getInitialIndex()));
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
		return initialIndex;
	}

	public void setInitialIndex(IndexName initialIndex) {
		this.initialIndex = initialIndex;
	}

	public IndexName getNewIndex() {
		return newIndex;
	}

	private void setNewIndex(IndexName newIndex) {
		this.newIndex = newIndex;
	}

}