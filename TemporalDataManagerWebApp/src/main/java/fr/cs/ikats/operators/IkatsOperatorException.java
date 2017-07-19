package fr.cs.ikats.operators;

/**
 * Generic exception for operators 
 */
public class IkatsOperatorException extends Exception {

    /**
     * serial UID
     */
    private static final long serialVersionUID = -3377103300178010217L;

    /**
     * @param message
     */
    public IkatsOperatorException(String message) {
        super(message);
    }

    /**
     * 
     * @param message
     * @param cause
     */
    public IkatsOperatorException(String message, Throwable cause) {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }
}
