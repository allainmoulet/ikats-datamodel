/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 4 avr. 2016 : Creation 
 *
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.temporaldata.exception;


import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.log4j.Logger;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;

/**
 * Handler of IkatsDaoExceptions
 */
@Provider
public class IkatsDaoExceptionHandler implements ExceptionMapper<IkatsDaoException> {

    private static Logger logger = Logger.getLogger(IkatsDaoExceptionHandler.class);
     
    /**
     * 
     * {@inheritDoc}
     * @since [#142998] handle IkatsDaoException, with different status Status.CONFLICT, Status.NOT_FOUND ...
     */
    @Override
    public Response toResponse(IkatsDaoException exception) {
        
        if(IkatsDaoMissingRessource.class.isAssignableFrom(exception.getClass())) {
            logger.error("Error processing the request: resource not found on server: ");
            logger.error( exception );
            return Response.status(Status.NOT_FOUND).entity(exception.getMessage()).build(); 
            
        } else if(IkatsDaoConflictException.class.isAssignableFrom(exception.getClass())) {
            logger.error("Error processing the request: resource conflict on server: ");
            logger.error( exception );
            return Response.status(Status.CONFLICT).entity(exception.getMessage()).build();
            
        } else {
            logger.error("Error handled while processing request ",exception);
            return Response.status(Status.BAD_REQUEST).entity(exception.getMessage()).build();
        }
    }
}
