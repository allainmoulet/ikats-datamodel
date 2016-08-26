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
 * Subclass of IkatsDaoException, 
 * raised in case of roll-back failure on a transaction
 */
public class IkatsDaoRollbackException extends IkatsDaoException {
 
    /**
     * 
     */
    private static final long serialVersionUID = -2807926355574145478L;

    /**
     * 
     */
    public IkatsDaoRollbackException() {
    }

    /**
     * @param message
     */
    public IkatsDaoRollbackException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public IkatsDaoRollbackException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public IkatsDaoRollbackException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public IkatsDaoRollbackException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
