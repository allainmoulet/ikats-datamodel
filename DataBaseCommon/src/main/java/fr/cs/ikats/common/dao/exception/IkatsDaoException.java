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
 * http://www.apache.org/licenses/LICENSE-2.0
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
 */

package fr.cs.ikats.common.dao.exception;

/**
 * Supertype of errors raised from DAO layer components.
 */
public class IkatsDaoException extends Exception {

    /**
     *
     */
    private static final long serialVersionUID = -3946760439416930687L;

    /**
     *
     */
    public IkatsDaoException() {
    }

    /**
     * @param message
     */
    public IkatsDaoException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public IkatsDaoException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public IkatsDaoException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public IkatsDaoException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}

