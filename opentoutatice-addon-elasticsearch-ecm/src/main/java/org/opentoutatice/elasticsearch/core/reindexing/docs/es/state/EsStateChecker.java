/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.es.state;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.client.AdminClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.UnmodifiableIterator;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.opentoutatice.elasticsearch.core.reindexing.docs.TransientIndexUse;
import org.opentoutatice.elasticsearch.core.reindexing.docs.es.state.exception.EsStateCheckException;
import org.opentoutatice.elasticsearch.core.reindexing.docs.manager.IndexNAliasManager;

/**
 * @author david
 *
 */
public class EsStateChecker {
	
	private static EsStateChecker instance;
	
	private EsStateChecker() {
		
	}
	
	public static synchronized EsStateChecker get() {
		if(instance == null) {
			instance = new EsStateChecker();
		}
		return instance;
	}
	
	public EsState getEsState() throws InterruptedException, ExecutionException {
		EsState esState = new EsState();
		
		AdminClient adminClient = IndexNAliasManager.get().getAdminClient();
		
		// Indices
		ImmutableOpenMap<String,IndexMetaData> indices = adminClient.cluster().prepareState().get().getState().getMetaData().getIndices();
		if(indices != null) {
			UnmodifiableIterator<String> indicesIt = indices.keysIt();	
			while(indicesIt.hasNext()) {
				String index = indicesIt.next();
				esState.addIndex(index);
			}
		}
		
		// Aliases
		
		ImmutableOpenMap<String,ImmutableOpenMap<String,AliasMetaData>> aliases = adminClient.cluster().prepareState().get().getState().getMetaData().getAliases();
		if(aliases != null) {
			Iterator<ObjectObjectCursor<String, ImmutableOpenMap<String, AliasMetaData>>> aliasesIt = aliases.iterator();
			while(aliasesIt.hasNext()) {
				ObjectObjectCursor<String, ImmutableOpenMap<String, AliasMetaData>> alias = aliasesIt.next();
				String aliasName = alias.key;
			
				ImmutableOpenMap<String,AliasMetaData> indicesOfMap = alias.value;
				List<String> indicesOf = new ArrayList<String>();
				if(indicesOfMap != null) {
					UnmodifiableIterator<String> idxIt = indicesOfMap.keysIt();
					while(idxIt.hasNext()) {
						String idx = idxIt.next();
						indicesOf.add(idx);
					}
				}
				
				esState.addAlias(aliasName, indicesOf);
			}
		}
		
		
		return esState;
	}
	
	public boolean aliasExistsWithOnlyOneIndex(String aliasName) throws EsStateCheckException {
		boolean verified = false;

		try {
			if (IndexNAliasManager.get().aliasExists(aliasName)) {
				List<String> indicesOfAlias = IndexNAliasManager.get().getIndicesOfAlias(aliasName);
				if (indicesOfAlias != null) {
					int nbIndices = indicesOfAlias.size();
					if (nbIndices == 1) {
						verified = true;
					} else {
						if (nbIndices == 0) {
							throw new EsStateCheckException(String.format("No index defined for alias [%s]. Fix Elastisearch state.", aliasName));
						} else if (indicesOfAlias.size() > 1) {
							throw new EsStateCheckException(String.format("[%s] indices defined for alias [%s]. Fix Elastisearch state.", String.valueOf(nbIndices), aliasName));
						}
					}
				} else {
					throw new EsStateCheckException(
							String.format("Alias [%s] does not exist. You must create it.", aliasName));
				}
			} else {
				throw new EsStateCheckException(
						String.format("No index defined for alias [%s]. Fix Elastiseach state.", aliasName));
			}
		} catch (Exception e) {
			if (!(e instanceof EsStateCheckException)) {
				throw new EsStateCheckException(e);
			}
		}

		return verified;
	}
	
	public boolean transientAliasesNotExist() throws EsStateCheckException {
		boolean verified = false;
		
		try {
			if(IndexNAliasManager.get().transientAliasesExists()) {
				throw new EsStateCheckException(String.format("Transient aliases [%s, %s] still exist. Fix Elastisearch state", TransientIndexUse.Read.getAlias(), TransientIndexUse.Write.getAlias()));
			} else {
				verified = true;
			}
		} catch (Exception e) {
			if (!(e instanceof EsStateCheckException)) {
				throw new EsStateCheckException(e);
			}
		}
		
		return verified;
	}

}
