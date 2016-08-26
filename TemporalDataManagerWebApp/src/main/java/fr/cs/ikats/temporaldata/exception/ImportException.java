package fr.cs.ikats.temporaldata.exception;

import fr.cs.ikats.datamanager.client.opentsdb.ImportResult;

/**
 * import Exception
 */
public class ImportException extends IkatsException {
	
	private ImportResult importResult = null;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1989793706365984507L;

	/**
	 * 
	 * @param message error message
	 */
	public ImportException(String message) {
		super(message);
	}

	/**
	 * 
	 * @param message error message
	 * @param importResult {@link ImportResult} provided
	 */
	public ImportException(String message, ImportResult importResult) {
		super(message);
		this.importResult = importResult;
	}

	/**
	 * 
	 * @param message error message
	 * @param cause the root cause
	 */
	public ImportException(String message, Throwable cause) {
		super(message+" : "+cause.getLocalizedMessage(), cause);
	}
	
	/**
	 * 
	 * @param message error message
	 * @param cause the root cause
	 * @param importResult {@link ImportResult} provided
	 */
	public ImportException(String message, Throwable cause, ImportResult importResult) {
		super(message+" : "+cause.getLocalizedMessage(), cause);
		this.importResult = importResult;
	}
	
	/**
	 * @return the importResult
	 */
	public ImportResult getImportResult() {
		return importResult;
	}
	
}
