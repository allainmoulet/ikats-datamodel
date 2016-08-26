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
 * raised in case of database conflict on a resource.
 */
public class IkatsDaoConflictException extends IkatsDaoException {

    /**
     * 
     */
    private static final long serialVersionUID = -6979304746326635840L;

    /**
     * 
     */
    public IkatsDaoConflictException() {
        // TODO Auto-generated constructor stub
    }

    /**
     * @param message
     */
    public IkatsDaoConflictException(String message) {
        super(message);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param cause
     */
    public IkatsDaoConflictException(Throwable cause) {
        super(cause);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param message
     * @param cause
     */
    public IkatsDaoConflictException(String message, Throwable cause) {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public IkatsDaoConflictException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        // TODO Auto-generated constructor stub
    }

}
