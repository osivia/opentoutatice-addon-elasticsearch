/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.test;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.logging.Log;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.nuxeo.elasticsearch.config.ElasticSearchIndexConfig;
import org.nuxeo.runtime.api.Framework;
import org.opentoutatice.elasticsearch.core.reindexing.docs.test.constant.ReIndexingTestConstants;
import org.opentoutatice.elasticsearch.core.reindexing.docs.transitory.TransitoryIndexUse;

/**
 * @author david
 *
 */
public class EsNodeTestInitializer {

    public static void initializeEsNodeInTestMode(Client client, Log log) {
        if (BooleanUtils.isTrue(Boolean.valueOf(Framework.getProperty(ReIndexingTestConstants.CREATE_ALIAS_N_INDEX_ON_STARTUP_TEST)))) {
            // FIXME: parametrize aliases names
            if (log.isDebugEnabled()) {
                log.debug(String.format("Creating index [%s] and its alias [%s].", "idx-tst", "nxutest-alias"));
            }
            client.admin().indices().prepareCreate("idx-tst").setSettings(ElasticSearchIndexConfig.DEFAULT_SETTING).get();
            client.admin().indices().preparePutMapping("idx-tst").setType("doc").setSource(ElasticSearchIndexConfig.DEFAULT_MAPPING).get();
            client.admin().indices().prepareAliases().addAlias("idx-tst", "nxutest-alias").get();
            if (log.isDebugEnabled()) {
                log.debug(String.format("Index [%s] and its alias [%s] created.", "idx-tst", "nxutest-alias"));
            }

            if (BooleanUtils.isTrue(Boolean.valueOf(Framework.getProperty(ReIndexingTestConstants.CREATE_BAD_ALIAS_N_INDEX_ON_STARTUP_TEST)))) {

                client.admin().indices().prepareCreate("idx-tst-bad").get();
                client.admin().indices().prepareAliases().addAlias("idx-tst-bad", "nxutest-alias").get();

                if (log.isDebugEnabled()) {
                    log.debug("BAD alias nxutest-alias created: points on 2 indices [idx-tst, idx-tst-bad]");
                }
            }

            if (BooleanUtils.isTrue(Boolean.valueOf(Framework.getProperty(ReIndexingTestConstants.CREATE_READ_ALIAS_ON_STARTUP_TEST)))) {
                client.admin().indices().prepareAliases().addAlias("idx-tst", TransitoryIndexUse.Read.getAlias()).get();

                if (log.isDebugEnabled()) {
                    log.debug("BAD yet existing alias [r-alias] created on [idx-tst]");
                }
            }

            if (BooleanUtils.isTrue(Boolean.valueOf(Framework.getProperty(ReIndexingTestConstants.CREATE_BAD_FORMER_ALIAS_ON_STARTUP_TEST)))) {

                client.admin().indices().prepareAliases().addAlias("idx-tst", "former-nxutest-alias").get();

                if (log.isDebugEnabled()) {
                    log.debug("BAD former alias former-nxutest-alias created: points on index [idx-tst]");
                }
            }
        }
    }
    
    public static void cleanIndices(Client client, Log log) {
        ImmutableOpenMap<String, IndexMetaData> indices = client.admin().cluster().prepareState().get().getState().getMetaData().getIndices();
        for (ObjectCursor<String> index : indices.keys()) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("== Cleaning index [%s]", index.value));
            }
            client.admin().indices().prepareDelete(index.value).execute().actionGet();
        }

        ImmutableOpenMap<String, ImmutableOpenMap<String, AliasMetaData>> aliases = client.admin().cluster().prepareState().get().getState().getMetaData()
                .getAliases();
        for (ObjectCursor<String> alias : aliases.keys()) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("== Cleaning alias [%s]", alias.value));
            }
            client.admin().indices().prepareDelete(alias.value).execute().actionGet();
        }
    }

}
