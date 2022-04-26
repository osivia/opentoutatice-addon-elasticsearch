/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.manager;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.UnmodifiableIterator;
import org.nuxeo.elasticsearch.api.ElasticSearchIndexing;
import org.nuxeo.runtime.api.Framework;
import org.opentoutatice.elasticsearch.api.OttcElasticSearchIndexing;
import org.opentoutatice.elasticsearch.config.OttcElasticSearchIndexOrAliasConfig;
import org.opentoutatice.elasticsearch.core.reindexing.docs.exception.IndexException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.index.IndexName;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception.ReIndexingException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.transitory.TransitoryIndexUse;

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
    
    // For tests only
    public static void reset() {
        instance = null;
    }

    // Must be called after OttcElasticSearchComponent#applicationStarted
    public static synchronized IndexNAliasManager get() {
        return instance;
    }

    public Boolean indexExists(String indexName) {
        return this.getAdminClient().indices().prepareExists(indexName).execute().actionGet().isExists();
    }

    public List<String> getIndices() {
        List<String> indices = null;

        ImmutableOpenMap<String, IndexMetaData> indicesMap = this.getAdminClient().cluster().prepareState().get().getState().getMetaData().getIndices();
        if (indicesMap != null) {
            indices = new ArrayList<String>();
            UnmodifiableIterator<String> indicesIt = indicesMap.keysIt();
            while (indicesIt.hasNext()) {
                String index = indicesIt.next();
                indices.add(index);
            }
        }

        return indices;
    }

    public Boolean aliasExists(String aliasName) {
        return this.getAdminClient().indices().prepareAliasesExist(aliasName).execute().actionGet().isExists();
    }

    public String getIndexOfAlias(String alias) {
        return this.getAdminClient().indices().prepareGetAliases(alias).get().getAliases().keysIt().next();
    }

    public List<String> getIndicesOfAlias(String alias) throws NoSuchElementException {
        List<String> indices = null;

        ImmutableOpenMap<String, List<AliasMetaData>> aliases = this.getAdminClient().indices().prepareGetAliases(alias).get().getAliases();
        if ((aliases != null) && (aliases.size() > 0)) {
            indices = new ArrayList<String>();
            UnmodifiableIterator<String> keysIt = aliases.keysIt();

            while (keysIt.hasNext()) {
                String index = keysIt.next();
                if (index != null) {
                    indices.add(index);
                }
            }
        } else {
            throw new NoSuchElementException(String.format("Alias [%s] does not exist.", alias));
        }

        return indices;
    }

    public synchronized String getTransientAlias(String repositoryName, TransitoryIndexUse use) {
        String alias = null;

        if (log.isTraceEnabled()) {
            log.trace(String.format("Getting %s alias for repository [%s] ", use.getAlias(), repositoryName));
        }

        if (TransitoryIndexUse.Read.equals(use)) {
            alias = TransitoryIndexUse.Read.getAlias();
        } else {
            alias = TransitoryIndexUse.Write.getAlias();
        }

        if (log.isTraceEnabled()) {
            log.trace(String.format("%s alias for repository [%s]: [%s] ", use.getAlias(), repositoryName, alias));
        }

        return alias;
    }

    public Boolean mayTransientAliasesExist() {
        return this.aliasExists(TransitoryIndexUse.Read.getAlias()) || this.aliasExists(TransitoryIndexUse.Write.getAlias());
    }

    public Boolean transientAliasesExist() {
        return this.aliasExists(TransitoryIndexUse.Read.getAlias()) && this.aliasExists(TransitoryIndexUse.Write.getAlias());
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

        if (!this.indexExists(newIndex)) {
            this.getAdminClient().indices().prepareCreate(newIndex).setSettings(nxAliasCfg.getSettings()).get();

            this.getAdminClient().indices().preparePutMapping(newIndex).setType(nxAliasCfg.getType()).setSource(nxAliasCfg.getMapping()).get();

            transientCfg = nxAliasCfg.clone();
            transientCfg.setName(newIndex);

        } else {
            throw new IndexException(String.format("Index [%s] yet exists.", transientCfg));
        }

        if (log.isInfoEnabled()) {
            log.info(String.format("New index [%s] created.", transientCfg));
        }

        return transientCfg;
    }

    public void createAliasFor(String indexName, String aliasName) {
        this.getAdminClient().indices().prepareAliases().addAlias(indexName, aliasName).get();
    }

    /**
     * @param initialIdxName
     * @param newIdxName
     * @throws ReIndexingException
     */
    public void createTransientAliases(IndexName initialIndex, IndexName newIndex) throws ReIndexingException {
        if (log.isDebugEnabled()) {
            log.debug(String.format("About to create transient aliases: [%s on (%s, %s) | %s on %s]...", TransitoryIndexUse.Read.getAlias(),
                    initialIndex.toString(), newIndex.toString(), TransitoryIndexUse.Write.getAlias(), newIndex.toString()));
        }

        try {
            // FIXME: check atomicity!!!!!!
            this.getAdminClient().indices().prepareAliases().addAlias(initialIndex.toString(), TransitoryIndexUse.Read.getAlias())
                    .addAlias(newIndex.toString(), TransitoryIndexUse.Read.getAlias()).addAlias(newIndex.toString(), TransitoryIndexUse.Write.getAlias()).get();
        } catch (ElasticsearchException e) {
            throw new ReIndexingException(e);
        }

        if (log.isInfoEnabled()) {
            log.info(String.format("Transient aliases: [%s on (%s, %s) | %s on %s] created.", TransitoryIndexUse.Read.getAlias(), initialIndex.toString(),
                    newIndex.toString(), TransitoryIndexUse.Write.getAlias(), newIndex.toString()));
        }
    }

    public void updateEsAlias(String aliasName, IndexName initialIndex, IndexName newIndex) throws ReIndexingException {
        if (log.isDebugEnabled()) {
            log.debug(String.format("About to update [%s] alias: switch from [%s] index to [%s] index ...", aliasName, initialIndex.toString(),
                    newIndex.toString()));
        }

        try {
            // FIXME: check atomicity!!!!!!
            this.getAdminClient().indices().prepareAliases().addAlias(newIndex.toString(), aliasName).removeAlias(initialIndex.toString(), aliasName).get();
        } catch (ElasticsearchException e) {
            throw new ReIndexingException(e);
        }

        if (log.isInfoEnabled()) {
            log.info(String.format("Alias [%s] updated: switched from [%s] index to [%s] index.", aliasName, initialIndex.toString(), newIndex.toString()));
        }
    }

    public void updateEsFormerAlias(String aliasName, IndexName initialIndex) throws ReIndexingException {
        String formerAlias = this.getFormerAliasName(aliasName);

        if (log.isDebugEnabled()) {
            log.debug(String.format("About to update [%s] alias to [%s] index ...", formerAlias, initialIndex.toString()));
        }

        try {
            if (this.aliasExists(formerAlias)) {
                String formerIndex = this.getIndexOfAlias(formerAlias);
                this.getAdminClient().indices().prepareAliases().removeAlias(formerIndex, formerAlias).get();
            }
            // FIXME: check atomicity!!!!!!
            this.getAdminClient().indices().prepareAliases().addAlias(initialIndex.toString(), formerAlias).get();
        } catch (ElasticsearchException e) {
            throw new ReIndexingException(e);
        }

        if (log.isInfoEnabled()) {
            log.info(String.format("Alias [%s] updated to [%s] index.", formerAlias, initialIndex.toString()));
        }
    }

    public void fixAlias(String aliasName, String currentIndex, IndexName initialIndex) throws ReIndexingException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("About to fix [%s] alias: setting to [%s] index ...", aliasName, initialIndex.toString()));
        }

        try {
            this.getAdminClient().indices().prepareAliases().removeAlias(currentIndex, aliasName).addAlias(initialIndex.toString(), aliasName).get();
        } catch (ElasticsearchException e) {
            throw new ReIndexingException(e);
        }

        if (log.isDebugEnabled()) {
            log.debug(String.format("ALias [%s] fixed: set to [%s] index", aliasName, initialIndex.toString()));
        }
    }

    /**
     * @param aliasName
     * @return
     */
    public String getFormerAliasName(String aliasName) {
        return FORMER_ALIAS_PREFIX.concat(aliasName);
    }

    /**
     * @param initialCfg
     * @param newCfg
     * @throws ReIndexingException
     */
    public void deleteTransientAliases(IndexName initialIndex, IndexName newIndex) throws ReIndexingException {
        if (log.isDebugEnabled()) {
            log.debug(String.format("About to delete transient aliases: [%s on (%s, %s) | %s on %s] ...", TransitoryIndexUse.Read.getAlias(),
                    initialIndex.toString(), newIndex.toString(), TransitoryIndexUse.Write.getAlias(), newIndex.toString()));
        }

        try {
            // FIXME: check atomicity!!!!!!
            this.getAdminClient().indices().prepareAliases().removeAlias(initialIndex.toString(), TransitoryIndexUse.Read.getAlias())
                    .removeAlias(newIndex.toString(), TransitoryIndexUse.Read.getAlias()).removeAlias(newIndex.toString(), TransitoryIndexUse.Write.getAlias())
                    .get();
        } catch (ElasticsearchException e) {
            throw new ReIndexingException(e);
        }

        if (log.isInfoEnabled()) {
            log.info(String.format("Transient aliases: [%s on (%s, %s) | %s on %s] deleted.", TransitoryIndexUse.Read.getAlias(), initialIndex.toString(),
                    newIndex.toString(), TransitoryIndexUse.Write.getAlias(), newIndex.toString()));
        }
    }

    /**
     * @param name
     */
    public void deleteIndex(String name) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("About to delete index [%s] ...", name));
        }

        this.getAdminClient().indices().prepareDelete(name).get();

        if (log.isDebugEnabled()) {
            log.debug(String.format("Index [%s] deleted", name));
        }
    }

    // Set ElasticSearchIndexing on fly cause difficult to instantiate at startup
    public OttcElasticSearchIndexing getElasticSearchIndexing() {
        if (this.elasticSearchIndexing == null) {
            this.setElasticSearchIndexing((OttcElasticSearchIndexing) Framework.getService(ElasticSearchIndexing.class));
        }
        return this.elasticSearchIndexing;
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
