/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : Fait technique : 142998 : 1 avr. 2016 : Creation 
 *
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.common.dao.exception;

/**
 * Supertype of errors raised from DAO layer components.
 */
public class IkatsDaoException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = -3946760439416930687L;
    
    /**
     * 
     */
    public IkatsDaoException() {
    }

    /**
     * @param message
     */
    public IkatsDaoException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public IkatsDaoException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public IkatsDaoException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public IkatsDaoException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
