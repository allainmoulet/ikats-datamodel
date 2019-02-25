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

package fr.cs.ikats.datamanager.client.opentsdb;

/**
 * a client Exception
 */
@SuppressWarnings("javadoc")
public class IkatsWebClientException extends Exception {

    /**
     * serialUID
     */
    private static final long serialVersionUID = 5678303167002824230L;

    /**
     * {@inheritDoc}
     */
    public IkatsWebClientException() {
    }

    /**
     * {@inheritDoc}
     */
    public IkatsWebClientException(String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public IkatsWebClientException(Throwable cause) {
        super(cause);
    }

    /**
     * {@inheritDoc}
     */
    public IkatsWebClientException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * {@inheritDoc}
     */
    public IkatsWebClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}

