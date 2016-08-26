package fr.cs.ikats.datamanager;

/**
 * Convenient class created in order to replace all the basic Exception throwing.
 * 
 * FIXME FTO à revoir avec une gestion d'exceptions centralisée dans un package fr.cs.ikats.common
 * 
 * @author ftoral
 *
 */
public class DataManagerException extends Exception {

	static final long serialVersionUID = -6147882893702441580L;
	
	public DataManagerException() {
		super();
	}

	public DataManagerException(String message, Throwable cause) {
		super(message, cause);
	}

	public DataManagerException(String message) {
		super(message);
	}

	public DataManagerException(Throwable cause) {
		super(cause);
	}

}
