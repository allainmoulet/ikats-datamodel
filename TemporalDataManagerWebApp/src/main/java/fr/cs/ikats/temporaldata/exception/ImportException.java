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

import fr.cs.ikats.datamanager.client.opentsdb.ApiResponse;
import fr.cs.ikats.datamanager.client.opentsdb.ImportResult;

/**
 * import Exception
 */
public class ImportException extends IkatsException {

    private ApiResponse importResult = null;

    /**
     *
     */
    private static final long serialVersionUID = 1989793706365984507L;

    /**
     *
     * @param message error message
     */
    public ImportException(String message) {
        super(message);
    }

    /**
     *
     * @param message error message
     * @param importResult {@link ImportResult} provided
     */
    public ImportException(String message, ApiResponse importResult) {
        super(message);
        this.importResult = importResult;
    }

    /**
     *
     * @param message error message
     * @param cause the root cause
     */
    public ImportException(String message, Throwable cause) {
        super(message + " : " + cause.getLocalizedMessage(), cause);
    }

    /**
     *
     * @param message error message
     * @param cause the root cause
     * @param importResult {@link ImportResult} provided
     */
    public ImportException(String message, Throwable cause, ApiResponse importResult) {
        super(message + " : " + cause.getLocalizedMessage(), cause);
        this.importResult = importResult;
    }

    /**
     * @return the importResult
     */
    public ApiResponse getImportResult() {
        return importResult;
    }

}
