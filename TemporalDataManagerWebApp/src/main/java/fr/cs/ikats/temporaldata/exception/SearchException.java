/**
 * 
 */
package fr.cs.ikats.temporaldata.exception;


/**
 * 
 * Search Exception
 * @author ikats
 *
 */
public class SearchException extends IkatsException {

	/**
	 * serialUID
	 */
	private static final long serialVersionUID = 7242079610903851378L;

	/**
	 * @param message error message
     * @param cause the root cause
	 */
	public SearchException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message error message
	 */
	public SearchException(String message) {
		super(message);
	}

}
