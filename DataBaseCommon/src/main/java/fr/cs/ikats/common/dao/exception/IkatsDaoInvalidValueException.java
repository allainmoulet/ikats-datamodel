/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 7 avr. 2016 : Creation 
 *
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.common.dao.exception;

/**
 * Exception raised when invalid value is provided to the DAO layer.
 */
public class IkatsDaoInvalidValueException extends IkatsDaoException {

    /**
     * 
     */
    private static final long serialVersionUID = 6699220248971152655L;

    /**
     * Optional field: when user wants to specifically stock the column in this exception
     */
    private String column = null;
    
    /**
     * Optional field: when user wants tospecifically stock the value causing this exception
     */
    private Object value = null;
    
    /**
     * Getter
     * @return the column
     */
    public String getColumn() {
        return column;
    }

    /**
     * Setter
     * @param column the column to set
     */
    public void setColumn(String column) {
        this.column = column;
    }

    /**
     * Getter
     * @return the value
     */
    public Object getValue() {
        return value;
    }

    /**
     * Setter
     * @param value the value to set
     */
    public void setValue(Object value) {
        this.value = value;
    }

    /**
     *  
     */
    public IkatsDaoInvalidValueException() { 
    }

    /**
     * @param message error message
     */
    public IkatsDaoInvalidValueException(String message) {
        super(message);
    }

    /**
     * @param column column on error occures
     * @param value value causing the error
     * @param message error message
     */
    public IkatsDaoInvalidValueException(String column, Object value, String message) {
        this(message);
        this.column = column;
        this.value = value;
    }
    
    /**
     * @param cause cause of error
     */
    public IkatsDaoInvalidValueException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message error message
     * @param cause cause of error
     */
    public IkatsDaoInvalidValueException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * @param column column on error occures
     * @param value value causing the error
     * @param message error message
     * @param cause cause of error
     */
    public IkatsDaoInvalidValueException(String column, Object value, String message, Throwable cause) {
        this(message, cause);
        this.column = column;
        this.value = value;
        
    }

    /**
     * @param message error message
     * @param cause cause of error
     * @param enableSuppression flag enabling suppression
     * @param writableStackTrace flag enabling stack trace writing
     */
    public IkatsDaoInvalidValueException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    /**
     * @param column column on error occures
     * @param value value causing the error
     * @param message error message
     * @param cause cause of error
     * @param enableSuppression flag enabling suppression
     * @param writableStackTrace flag enabling stack trace writing
     */
    public IkatsDaoInvalidValueException(String column, Object value, String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
    
}
