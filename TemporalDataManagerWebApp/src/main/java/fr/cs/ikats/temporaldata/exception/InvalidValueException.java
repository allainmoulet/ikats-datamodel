package fr.cs.ikats.temporaldata.exception;

import fr.cs.ikats.temporaldata.application.ApplicationLabels;

/**
 * Exception for invalid value
 */
public class InvalidValueException extends Exception {

    /**
     * value found
     */
    String found;
    /**
     * property for the value
     */
    String property;
    /**
     * model class concerned
     */
    String modelClass;
    /**
     * instance concerned
     */
    String instanceId = null;
    /**
     * expected format
     */
    String excepted;

    /**
     * {@inheritDoc}
     */
    @Override
    public String getMessage() {
        if(instanceId!=null) {
            return ApplicationLabels.getInstance().getLabel("error.invalid.value.with.id", found, property, modelClass, instanceId, excepted);
        } else {
            return ApplicationLabels.getInstance().getLabel("error.invalid.value", found, property, modelClass, excepted);
        }
        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getLocalizedMessage() {
        return getMessage();
    }

    /**
     * 
     */
    private static final long serialVersionUID = 2274897881691729460L;

    /**
     * @param modelClass
     *            model class concerned
     * @param property
     *            property for the value
     * @param excepted
     *            expected format
     * @param found
     *            value found
     * @param instanceId
     *            instance concerned
     * @param cause
     *            the root cause
     */
    public InvalidValueException(String modelClass, String property, String excepted, String found, String instanceId, Throwable cause) {
        super(cause);
        this.property = property;
        this.excepted = excepted;
        this.modelClass = modelClass;
        this.found = found;
        this.instanceId = instanceId;
    }

    /**
     * @param modelClass
     *            model class concerned
     * @param property
     *            property for the value
     * @param excepted
     *            expected format
     * @param found
     *            value found
     * @param instanceId
     *            instance concerned
     */
    public InvalidValueException(String modelClass, String property, String excepted, String found, String instanceId) {
        super();
        this.property = property;
        this.excepted = excepted;
        this.modelClass = modelClass;
        this.found = found;
        this.instanceId = instanceId;
    }

    /**
     * @param message
     *            the exception message
     */
    public InvalidValueException(String message) {
        super(message);
    }
    
    /**
     * Constructor from one message and one cause exception 
     * @param message the invalid value message
     * @param cause the exception causing this exception
     */
    public InvalidValueException(String message, Throwable cause) {
        super(message, cause);
    }

}
