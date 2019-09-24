/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.manager;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.UnmodifiableIterator;
import org.nuxeo.elasticsearch.api.ElasticSearchIndexing;
import org.nuxeo.runtime.api.Framework;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.config.OttcElasticSearchIndexOrAliasConfig;
import org.opentoutatice.elasticsearch.core.reindexing.docs.TransientIndexUse;
import org.opentoutatice.elasticsearch.core.reindexing.docs.exception.IndexException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.name.IndexName;

/**
 * 
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
// TODO: manage IndexManager instance lifecycle in OSGI way? At least, in more integrated way?
public class IndexNAliasManager {

	private static final Log log = LogFactory.getLog(IndexNAliasManager.class);

	private static final String FORMER_ALIAS_PREFIX = "former-";

	private OttcElasticSearchIndexing elasticSearchIndexing;

	/**
	 * Es Admin Transport client.
	 */
	private AdminClient adminClient;

	private static IndexNAliasManager instance;

	private IndexNAliasManager(AdminClient adminClient) {
		super();
		this.adminClient = adminClient;
	}
	
	public static synchronized void init(AdminClient adminClient) {
		Validate.isTrue(instance == null);
		instance = new IndexNAliasManager(adminClient);
	}

	// Must be called after OttcElasticSearchComponent#applicationStarted
	public static synchronized IndexNAliasManager get() {
		return instance;
	}

	public Boolean indexExists(String indexName) {
		return getAdminClient().indices().prepareExists(indexName).execute().actionGet().isExists();
	}

	public Boolean aliasExists(String aliasName) {
		return getAdminClient().indices().prepareAliasesExist(aliasName).execute().actionGet().isExists();
	}

	public String getIndexOfAlias(String alias) {
		return getAdminClient().indices().prepareGetAliases(alias).get().getAliases().keysIt().next();
	}
	
	public List<String> getIndicesOfAlias(String alias) throws NoSuchElementException {
		List<String> indices = null;
		
		ImmutableOpenMap<String,List<AliasMetaData>> aliases = getAdminClient().indices().prepareGetAliases(alias).get().getAliases();
		if(aliases != null && aliases.size() > 0) {
			indices = new ArrayList<String>();
			UnmodifiableIterator<String> keysIt = aliases.keysIt();
			
			while(keysIt.hasNext()) {
				String index = keysIt.next();
				if(index != null) {
					indices.add(index);
				}
			}
		} else {
			throw new NoSuchElementException(String.format("Alias [%s] does not exist.", alias));
		}
		
		return indices;
	}

	public synchronized String getTransientAlias(String repositoryName, TransientIndexUse use) {
		String alias = null;

		if (log.isTraceEnabled()) {
			log.trace(String.format("Getting %s alias for repository [%s] ", use.getAlias(), repositoryName));
		}

		if (TransientIndexUse.Read.equals(use)) {
			alias = TransientIndexUse.Read.getAlias();
		} else {
			alias = TransientIndexUse.Write.getAlias();
		}

		if (log.isTraceEnabled()) {
			log.trace(String.format("%s alias for repository [%s]: [%s] ", use.getAlias(), repositoryName, alias));
		}

		return alias;
	}

	public Boolean transientAliasesExists() {
		return aliasExists(TransientIndexUse.Read.getAlias()) && aliasExists(TransientIndexUse.Write.getAlias());
	}

	/**
	 * @param newIdxCfg
	 * @return
	 * @throws IndexException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws IndexExistenceException
	 */
	public OttcElasticSearchIndexOrAliasConfig createNewIndex(IndexName newIndexName, OttcElasticSearchIndexOrAliasConfig nxAliasCfg)
			throws IndexException, InterruptedException, ExecutionException {
		// Result
		OttcElasticSearchIndexOrAliasConfig transientCfg = null;

		if (log.isDebugEnabled()) {
			log.debug("About to create new index ...");
		}

		String newIndex = newIndexName.toString();

		if (!indexExists(newIndex)) {
			getAdminClient().indices().prepareCreate(newIndex).setSettings(nxAliasCfg.getSettings()).get();

			getAdminClient().indices().preparePutMapping(newIndex).setType(nxAliasCfg.getType())
					.setSource(nxAliasCfg.getMapping()).get();

			transientCfg = nxAliasCfg.clone();
			transientCfg.setName(newIndex);

		} else {
			throw new IndexException(String.format("Index [%s] yet exists.", transientCfg));
		}

		if (log.isDebugEnabled()) {
			log.debug(String.format("New index [%s] created.", transientCfg));
		}

		return transientCfg;
	}

	public void createAliasFor(String indexName, String aliasName) {
		getAdminClient().indices().prepareAliases().addAlias(indexName, aliasName);
	}

	/**
	 * @param initialIdxName
	 * @param newIdxName
	 * @throws ReIndexingException 
	 */
	public void createTransientAliases(IndexName initialIndex, IndexName newIndex) throws ReIndexingException {
		if (log.isDebugEnabled()) {
			log.debug(String.format("About to create transient aliases: [%s on (%s, %s) | %s on %s]...", TransientIndexUse.Read.getAlias(), initialIndex.toString(), newIndex.toString(),
					TransientIndexUse.Write.getAlias(), newIndex.toString()));
		}

		try {
			// FIXME: check atomicity!!!!!!
			getAdminClient().indices().prepareAliases()
					.addAlias(initialIndex.toString(), TransientIndexUse.Read.getAlias())
					.addAlias(newIndex.toString(), TransientIndexUse.Read.getAlias())
					.addAlias(newIndex.toString(), TransientIndexUse.Write.getAlias()).get();
		} catch (ElasticsearchException e) {
			throw new ReIndexingException(e);
		}

		if (log.isDebugEnabled()) {
			log.debug(String.format("Transient aliases: [%s on (%s, %s) | %s on %s] created.", TransientIndexUse.Read.getAlias(), initialIndex.toString(), newIndex.toString(),
					TransientIndexUse.Write.getAlias(), newIndex.toString()));
		}
	}

	public void updateEsAlias(String aliasName, IndexName initialIndex, IndexName newIndex) throws ReIndexingException {
		if (log.isDebugEnabled()) {
			log.debug(String.format("About to update [%s] alias: switch from [%s] index to [%s] index ...",
					aliasName, initialIndex.toString(), newIndex.toString()));
		}

		try {
			// FIXME: check atomicity!!!!!!
			getAdminClient().indices().prepareAliases().addAlias(newIndex.toString(), aliasName)
					.removeAlias(initialIndex.toString(), aliasName).get();
		} catch (ElasticsearchException e) {
			throw new ReIndexingException(e);
		}

		if (log.isDebugEnabled()) {
			log.debug(String.format("Alias [%s] updated: switched from [%s] index to [%s] index.",
					aliasName, initialIndex.toString(), newIndex.toString()));
		}
	}

	public void updateEsFormerAlias(String aliasName, IndexName initialIndex) throws ReIndexingException {
		String formerAlias = FORMER_ALIAS_PREFIX.concat(aliasName);

		if (log.isDebugEnabled()) {
			log.debug(String.format("About to update [%s] alias to [%s] index ...", formerAlias,
					initialIndex.toString()));
		}

		try {
			if(aliasExists(aliasName)) {
				String formerIndex = getIndexOfAlias(aliasName);
				getAdminClient().indices().prepareAliases().removeAlias(formerIndex, formerAlias);
			}
			// FIXME: check atomicity!!!!!!
			getAdminClient().indices().prepareAliases().addAlias(initialIndex.toString(), formerAlias).get();
		} catch (ElasticsearchException e) {
			throw new ReIndexingException(e);
		}

		if (log.isDebugEnabled()) {
			log.debug(String.format("Alias [%s] updated to [%s] index.", formerAlias, initialIndex.toString()));
		}
	}

	/**
	 * @param initialCfg
	 * @param newCfg
	 * @throws ReIndexingException
	 */
	public void deleteTransientAliases(IndexName initialIndex, IndexName newIndex) throws ReIndexingException {
		if (log.isDebugEnabled()) {
			log.debug(String.format("About to delete transient aliases: [%s on (%s, %s) | %s on %s] ...", TransientIndexUse.Read.getAlias(), initialIndex.toString(), newIndex.toString(),
					TransientIndexUse.Write.getAlias(), newIndex.toString()));
		}

		try {
			// FIXME: check atomicity!!!!!!
			getAdminClient().indices().prepareAliases()
					.removeAlias(initialIndex.toString(), TransientIndexUse.Read.getAlias())
					.removeAlias(newIndex.toString(), TransientIndexUse.Read.getAlias())
					.removeAlias(newIndex.toString(), TransientIndexUse.Write.getAlias()).get();
		} catch (ElasticsearchException e) {
			throw new ReIndexingException(e);
		}

		if (log.isDebugEnabled()) {
			log.debug(String.format("Transient aliases: [%s on (%s, %s) | %s on %s] deleted.", TransientIndexUse.Read.getAlias(), initialIndex.toString(), newIndex.toString(),
					TransientIndexUse.Write.getAlias(), newIndex.toString()));
		}
	}

	/**
	 * @param name
	 */
	public void deleteIndex(String name) {
		getAdminClient().indices().prepareDelete(name).get();
	}

	// Set ElasticSearchIndexing on fly cause difficult to instantiate at startup
	public OttcElasticSearchIndexing getElasticSearchIndexing() {
		if (this.elasticSearchIndexing == null) {
			setElasticSearchIndexing((OttcElasticSearchIndexing) Framework.getService(ElasticSearchIndexing.class));
		}
		return elasticSearchIndexing;
	}

	private void setElasticSearchIndexing(OttcElasticSearchIndexing elasticSearchIndexing) {
		this.elasticSearchIndexing = elasticSearchIndexing;
	}

	public AdminClient getAdminClient() {
		return this.adminClient;
	}

	public IndexNAliasManager adminClient(Client esClient) {
		this.adminClient = esClient.admin();
		return get();
	}

}
