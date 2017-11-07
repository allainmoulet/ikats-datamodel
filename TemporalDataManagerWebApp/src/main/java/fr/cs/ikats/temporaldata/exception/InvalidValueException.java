/**
 * LICENSE:
 * --------
 * Copyright 2017 CS SYSTEMES D'INFORMATION
 * 
 * Licensed to CS SYSTEMES D'INFORMATION under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. CS licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 * @author Fabien TORAL <fabien.toral@c-s.fr>
 * @author Fabien TORTORA <fabien.tortora@c-s.fr>
 * @author Mathieu BERAUD <mathieu.beraud@c-s.fr>
 * 
 */

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

