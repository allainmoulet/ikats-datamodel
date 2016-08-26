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
 * Subclass of IkatsDaoException: exceptions raised when a ressource is missing. 
 */
public class IkatsDaoMissingRessource extends IkatsDaoException {
 
    /**
     * 
     */
    private static final long serialVersionUID = -6925941034613818210L;

    /**
     * Subclass of IkatsDaoException: error raised when a ressource was not found
     */
    public IkatsDaoMissingRessource() { 
    }

    /**
     * @param message
     */
    public IkatsDaoMissingRessource(String message) {
        super(message); 
    }

    /**
     * @param cause
     */
    public IkatsDaoMissingRessource(Throwable cause) {
        super(cause); 
    }

    /**
     * @param message
     * @param cause
     */
    public IkatsDaoMissingRessource(String message, Throwable cause) {
        super(message, cause); 
    }

    /**
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public IkatsDaoMissingRessource(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
