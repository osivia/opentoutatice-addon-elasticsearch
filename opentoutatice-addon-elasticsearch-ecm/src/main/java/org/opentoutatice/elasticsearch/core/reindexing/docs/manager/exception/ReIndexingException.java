/**
 *
 */
package org.opentoutatice.elasticsearch.core.reindexing.docs.manager.exception;

/**
 * @author david
 *
 */
public class ReIndexingException extends Exception {

    private static final long serialVersionUID = 9167570665480723294L;

    public ReIndexingException(String msg) {
        super(msg);
    }

    public ReIndexingException(Exception e) {
        super(e);
    }

    public ReIndexingException(String msg, Exception e) {
        super(msg, e);
    }

    // public static String getMessageWithEsState(String message) {
    // String esState = StringUtils.EMPTY;
    // try {
    // EsState finalEsState = EsStateChecker.get().getEsState();
    // esState = finalEsState.toString();
    // } catch (InterruptedException | ExecutionException exc) {
    // // Nothing: do not block for logs
    // }
    //
    // StringBuffer sb = new StringBuffer(message);
    // if (StringUtils.isNotEmpty(esState)) {
    // sb.append(System.lineSeparator()).append(esState);
    // }
    //
    // return sb.toString();
    // }

}
