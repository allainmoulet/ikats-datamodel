package fr.cs.ikats.datamanager.client.opentsdb;

/**
 * a client Exception 
 */
@SuppressWarnings("javadoc")
public class IkatsWebClientException extends Exception {

    /**
     * serialUID
     */
    private static final long serialVersionUID = 5678303167002824230L;

    /**
     * {@inheritDoc}
     */   
    public IkatsWebClientException() {
    }

    /**
     * {@inheritDoc}
     */
    public IkatsWebClientException(String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public IkatsWebClientException(Throwable cause) {
        super(cause);
    }

    /**
     * {@inheritDoc}
     */
    public IkatsWebClientException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * {@inheritDoc}
     */
    public IkatsWebClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
