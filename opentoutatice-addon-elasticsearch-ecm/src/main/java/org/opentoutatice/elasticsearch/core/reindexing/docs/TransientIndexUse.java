/**
 * 
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs;

/**
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
public enum TransientIndexUse {
	
	Read("r-alias"), Write("w-alias");
	
	private String alias;
	
	private TransientIndexUse(String alias) {
		this.alias = alias;
	}
	
	public String getAlias() {
		return this.alias;
	}
}
