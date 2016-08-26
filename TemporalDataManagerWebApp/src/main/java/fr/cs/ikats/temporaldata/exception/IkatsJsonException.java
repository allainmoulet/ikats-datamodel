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
 * Ikats exception managed for http response producing json
 * @author ikats
 *
 */
public class IkatsJsonException extends IkatsException {

    /**
     * 
     */
    private static final long serialVersionUID = -3512718462627659049L;

    /**
     * @param message error message
     */
    public IkatsJsonException(String message) {
        super(message);
    }
     
    /**
     * @param message error message
     * @param cause the root cause
     */
    public IkatsJsonException(String message, Throwable cause) {
        super(message, cause);
    }
 
    /**
     * Status is BAD_REQUEST for IkatsJsonException.
     * Note: subclasses may override this method for specific codes
     * @return the http request status associted to this exception
     */
    public Status getHtppStatus()
    {
        return Status.BAD_REQUEST;
    }
}
