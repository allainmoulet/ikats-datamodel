/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 11 janv. 2016 : Creation 
 *
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.temporaldata.exception;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.log4j.Logger;

/**
 * Handler of exception InvalidValue.
 * Returns a NOT ACCEPTABLE (406) response status.
 */
@Provider
public class InvalidValueExceptionHandler implements ExceptionMapper<InvalidValueException> {

    private static Logger logger = Logger.getLogger(InvalidValueExceptionHandler.class);

    @Override
    public Response toResponse(InvalidValueException exception) {
        logger.error(exception);
        return Response.status(Status.NOT_ACCEPTABLE).entity(exception.getMessage()).build();
    }

}
