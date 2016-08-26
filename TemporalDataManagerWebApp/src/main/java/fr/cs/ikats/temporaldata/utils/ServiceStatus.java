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

import fr.cs.ikats.temporaldata.exception.IkatsJsonException;

/**
 * Generic wrapper for json management: status
 * Mostly used for error message produced.
 * But may also be included in more complexe json returned, with NOMINAL or WARN information.
 */

public class ServiceStatus {
     
    /**
     * Value for json
     */
    public static final int NOMINAL = 0;
    /**
     * Value for json 
     */
    public static final int WARN = 1;
    /**
     * Value for json
     */
    public static final int ERROR = -1;
    
    private int code;
    private Object msg;
    
    /**
     * Constructor of any status
     * @param aExecCode the code
     * @param aMsg the msg is an Object with understandable toString() method
     */
    public ServiceStatus( int aExecCode, Object aMsg )
    {
        code= aExecCode;
        msg = aMsg;
    }
    
    /**
     * Constructor of error status from IkatsJsonException
     * @param exception the exception providing the message with getMessage()
     */
    public ServiceStatus( IkatsJsonException exception )
    {
        code = exception.getHtppStatus().getStatusCode();
        msg = exception.getMessage();
    }
    
    /**
     * Constructor of error status from Exception
     * @param exception the exception providing the message with getMessage()
     */
    public ServiceStatus( Exception exception )
    {
        code = ERROR;
        msg = exception.getMessage();
    }
    
    /**
     * Constructor of error status from Error
     * @param error the error providing the message with getMessage()
     */
    public ServiceStatus( Error error )
    {
        code = ERROR;
        msg = error.getMessage();
    }
    
    /** 
     * @return the message with  this.msg.toString()
     */
    public String getMsg()
    {
        
        if ( msg != null )
        {
            return msg.toString();
        }
        else
        {
            return "";
        }
    }
    
    /**
     * 
     * @return the code associated to the message
     */
    public int getCode()
    {
        return code;
    }
}
