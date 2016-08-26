/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 13 nov. 2015 : Creation 
 *
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.temporaldata.exception;

/**
 * exception indicating no ressoure is found
 */
public class ResourceNotFoundException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = -7372675465107111550L;

    /**
     * 
     */
    public ResourceNotFoundException() {
    }

    /**
     * @param message error message
     */
    public ResourceNotFoundException(String message) {
        super(message);
    }

    /**
     * @param cause the root cause
     */
    public ResourceNotFoundException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message error message
     * @param cause the root cause
     */
    public ResourceNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }


}
