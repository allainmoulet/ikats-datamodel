/**
 * Copyright 2018-2019 CS Systèmes d'Information
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.cs.ikats.common.dao.exception;

/**
 * Subclass of IkatsDaoException: exceptions raised when a ressource is missing. 
 */
public class IkatsDaoMissingResource extends IkatsDaoException {

    /**
     *
     */
    private static final long serialVersionUID = -6925941034613818210L;

    /**
     * Subclass of IkatsDaoException: error raised when a ressource was not found
     */
    public IkatsDaoMissingResource() {
    }

    /**
     * @param message
     */
    public IkatsDaoMissingResource(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public IkatsDaoMissingResource(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public IkatsDaoMissingResource(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public IkatsDaoMissingResource(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
