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

package fr.cs.ikats.temporaldata.exception;

/**
 * LookupException
 *
 */
public class IkatsLookupException extends IkatsException {

    /**
     *
     */
    private static final long serialVersionUID = -8024152433074466146L;

    /**
     * @param message error message
     */
    public IkatsLookupException(String message) {
        super(message);
    }

    /**
     * @param message error message
     * @param cause the root cause
     */
    public IkatsLookupException(String message, Throwable cause) {
        super(message, cause);
    }


}
