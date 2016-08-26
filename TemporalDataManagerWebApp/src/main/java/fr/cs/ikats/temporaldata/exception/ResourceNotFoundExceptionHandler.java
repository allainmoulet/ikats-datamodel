package fr.cs.ikats.temporaldata.exception;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.log4j.Logger;

/**
 * Handle ResourceNotFoundException and set NOT_FOUND to the responses
 */
@Provider
public class ResourceNotFoundExceptionHandler implements ExceptionMapper<ResourceNotFoundException> {

    private static Logger logger = Logger.getLogger(ResourceNotFoundExceptionHandler.class);

    @Override
    public Response toResponse(ResourceNotFoundException exception) {
        logger.error("Resource not found on server : " + exception.getMessage());
        return Response.status(Status.NOT_FOUND).entity(exception.getMessage()).build();
    }

}
