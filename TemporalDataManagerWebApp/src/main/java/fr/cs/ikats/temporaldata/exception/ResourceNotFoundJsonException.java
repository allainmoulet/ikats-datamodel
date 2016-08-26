/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 13 janv. 2016 : Creation 
 *
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.temporaldata.exception;

import javax.ws.rs.core.Response.Status;

/**
 * Exception indicating no resource is found.
 * Adapted to http requests producing json.
 * 
 */
public class ResourceNotFoundJsonException extends IkatsJsonException {
 
    /**
     * 
     */
    private static final long serialVersionUID = -6942435746728380602L;
    
    /**
     * @param message the message
     * @param cause the cause
     */
    public ResourceNotFoundJsonException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message the message
     */
    public ResourceNotFoundJsonException(String message) {
        super(message);
    }

    /**
     * 
     * @return Status.NOT_FOUND
     */
    public Status getHtppStatus()
    {
        return Status.NOT_FOUND;
    }
   

}
