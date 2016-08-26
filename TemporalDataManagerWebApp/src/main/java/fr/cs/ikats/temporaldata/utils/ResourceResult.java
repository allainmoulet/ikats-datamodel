/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 7 janv. 2016 : Creation 
 *
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.temporaldata.utils;


/**
 * Generic wrapper of single resource result T.
 * Ought to be translated to JSON response of web service.
 * @param <T> resource type
 */
public class ResourceResult<T> {

    private final T result;
    private final ServiceStatus status;
    
    /**
     * 
     * @param argValue the wrapped resource
     * @param aStatusInfo the service status
     */
    public ResourceResult (T argValue, ServiceStatus aStatusInfo)
    {
        result = argValue;
        status = aStatusInfo;   
    }
    
    /**
     * 
     * @return the wrapped resource
     */
    public T getResult()
    {
        return result;
    }
    
    /**
     * 
     * @return the service status
     */
    public ServiceStatus getStatus()
    {
        return status;
    }
}
