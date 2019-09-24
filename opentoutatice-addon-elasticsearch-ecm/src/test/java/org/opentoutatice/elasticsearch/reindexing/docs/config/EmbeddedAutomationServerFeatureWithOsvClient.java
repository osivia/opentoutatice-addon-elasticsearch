/**
 * 
 */
package org.opentoutatice.elasticsearch.reindexing.docs.config;

import org.nuxeo.ecm.automation.client.jaxrs.impl.HttpAutomationClient;
import org.nuxeo.ecm.automation.test.EmbeddedAutomationServerFeature;

/**
 * @author david
 *
 */
public class EmbeddedAutomationServerFeatureWithOsvClient extends EmbeddedAutomationServerFeature {

	@Override
	protected HttpAutomationClient getHttpAutomationClient() {
//		HttpAutomationClient client = new HttpAutomationClient("http://localhost:18080/automation",
//				HTTP_CONNECTION_TIMEOUT);
//		// Deactivate global operation registry cache to allow tests using this
//		// feature in a test suite to deploy different set of operations
//		client.setSharedRegistryExpirationDelay(0);
		
		HttpAutomationClient client = new HttpAutomationClient("http://localhost:18080/automation", null);
		
		return client;
	}

}
