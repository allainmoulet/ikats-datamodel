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
 * 
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
     * Optional field: when user wants to specifically stock the value causing this exception
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

