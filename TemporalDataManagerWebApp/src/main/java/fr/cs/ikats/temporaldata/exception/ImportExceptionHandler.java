/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 1 déc. 2015 : Creation 
 *
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.temporaldata.exception;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.log4j.Logger;
import fr.cs.ikats.datamanager.client.opentsdb.ApiResponse;
import fr.cs.ikats.datamanager.client.opentsdb.ImportResult;

/**
 * Handler converting ImportException object into HTTP Response object
 */
@Provider
public class ImportExceptionHandler implements ExceptionMapper<ImportException> {

	private static Logger logger = Logger.getLogger(ImportExceptionHandler.class);

	@Override
	public Response toResponse(ImportException exception) {
		logger.error("Error handled while importing data", exception);
		Status handledStatus = Status.BAD_REQUEST;
		if (ImportFileNotFoundException.class.isAssignableFrom(exception.getClass())) {
			handledStatus = Status.NOT_FOUND;
		}
		ApiResponse resultatTotal = exception.getImportResult(); 
		if (resultatTotal == null) resultatTotal = new ImportResult();
		resultatTotal.setSummary(exception.getMessage());
		
		return Response.status(handledStatus).entity(resultatTotal).build();
	}

	/**
	 * constructor
	 */
	public ImportExceptionHandler() {
		logger.info("INIT IMPORT EXCEPTION HANDLER");
	}
}
