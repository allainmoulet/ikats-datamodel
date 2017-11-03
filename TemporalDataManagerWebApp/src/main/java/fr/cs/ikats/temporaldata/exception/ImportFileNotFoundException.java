package fr.cs.ikats.temporaldata.exception;

import fr.cs.ikats.datamanager.client.opentsdb.ApiResponse;

/**
 * Specific ImportException in order to handle specific service status during import services.
 */
public class ImportFileNotFoundException extends ImportException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4745603418413343985L;

	/**
	 * @param message
	 * @param importResult
	 */
	public ImportFileNotFoundException(String message, ApiResponse importResult) {
		super(message, importResult); 
	}

	/**
	 * @param message
	 * @param cause
	 * @param importResult
	 */
	public ImportFileNotFoundException(String message, Throwable cause, ApiResponse importResult) {
		super(message, cause, importResult); 
	}

	/**
	 * @param message
	 * @param cause
	 */
	public ImportFileNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 */
	public ImportFileNotFoundException(String message) {
		super(message);
	}
}
