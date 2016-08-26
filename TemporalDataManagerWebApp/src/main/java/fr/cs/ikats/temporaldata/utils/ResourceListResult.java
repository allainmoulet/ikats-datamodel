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

import java.util.List;

/**
 * Generic wrapper of a web service result with (0..N) resources T.
 * Ought to be translated to JSON response of web service.
 * @param <T> resource type
 */
public class ResourceListResult<T> {

    private final List<T> result;
    private final ServiceStatus status;
    /**
     * 
     * @param argValue list of resources
     * @param aStatusInfo status information
     */
    public ResourceListResult (List<T> argValue, ServiceStatus aStatusInfo)
    {
        result = argValue;
        status = aStatusInfo;   
    }
    /**
     * 
     * @return result list
     */
    public List<T> getResult()
    {
        return result;
    }
    /**
     * 
     * @return status
     */
    public ServiceStatus getStatus()
    {
        return status;
    }
}
