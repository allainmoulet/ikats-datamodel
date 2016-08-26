/**
 * 
 */
package fr.cs.ikats.temporaldata.exception;

/**
 * LookupException
 * @author ikats
 *
 */
public class IkatsLookupException extends IkatsException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8024152433074466146L;

	/**
	 * @param message error message
	 */
	public IkatsLookupException(String message) {
		super(message);
	}

	/**
	 * @param message error message
     * @param cause the root cause
	 */
	public IkatsLookupException(String message, Throwable cause) {
		super(message, cause);
	}


}
