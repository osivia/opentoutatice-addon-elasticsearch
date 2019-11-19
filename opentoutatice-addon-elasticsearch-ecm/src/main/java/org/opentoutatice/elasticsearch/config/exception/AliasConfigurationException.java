/**
 *
 */
package org.opentoutatice.elasticsearch.config.exception;

/**
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
public class AliasConfigurationException extends Exception {

    private static final long serialVersionUID = 6248156697996620456L;

    public AliasConfigurationException(String msg) {
        super(msg);
    }

    public AliasConfigurationException(Exception exc) {
        super(exc.getCause());
    }

}
