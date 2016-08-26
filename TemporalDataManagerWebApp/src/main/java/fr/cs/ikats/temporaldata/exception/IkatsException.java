package fr.cs.ikats.temporaldata.exception;

/**
 * Generic ikats Exception 
 */
public class IkatsException extends Exception {

    /**
     * serial UID
     */
    private static final long serialVersionUID = -4975793907370072209L;

    /**
     * constructor
     * @param message error message
     */
    public IkatsException(String message) {
        super(message);
    }


    /**
     * constructor
     * @param message error message
     * @param cause the root cause
     */
    public IkatsException(String message, Throwable cause) {
        super(message, cause);
    }

}
