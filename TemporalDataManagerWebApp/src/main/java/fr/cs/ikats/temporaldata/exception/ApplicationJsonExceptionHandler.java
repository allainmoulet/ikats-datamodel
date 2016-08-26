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

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.log4j.Logger; 

import fr.cs.ikats.temporaldata.utils.ServiceStatus;

/**
 * This handler is adapted to http requests producing json.
 * It is using  IkatsJsonException and its getHtppStatus() method, which may be overridden 
 * (see {@link ResourceNotFoundJsonException}).
 */
@Provider
public class ApplicationJsonExceptionHandler implements ExceptionMapper<IkatsJsonException> {

    private static Logger logger = Logger.getLogger(ApplicationJsonExceptionHandler.class);
     
    /**
     * Encode an error in HTTP Response:
     * <ul>
     * <li>Set status defined in IkatsJsonException (sub)class, using  exception.getHtppStatus()</li>
     * <li>Write json content from thrown instances of IkatsJsonException (sub)class.</li>
     * </ul>
     * {@inheritDoc}
     */
    @Override
    public Response toResponse(IkatsJsonException exception) {

            Status status =  exception.getHtppStatus();
            if(status.equals(Status.NOT_FOUND)) {
                logger.error("Error handled while processing request :" + status.getStatusCode() + " " + status.getReasonPhrase() + " " + exception.getMessage() );
            } else {
                logger.error("Error handled while processing request :" + status.getStatusCode() + " " + status.getReasonPhrase(), 
                        exception );
            }
            
            ServiceStatus lWrappedError = new ServiceStatus( exception );
            return Response.status( status ).type( MediaType.APPLICATION_JSON_TYPE ).entity( lWrappedError ).build();   
    }
}
