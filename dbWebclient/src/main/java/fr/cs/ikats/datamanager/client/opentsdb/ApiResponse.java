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

import java.io.Serializable;

public class ApiResponse implements Serializable {

    private String summary;

    private int statusCode;

    private Error error;

    /**
     * Getter
     *
     * @return the summary
     */
    public String getSummary() {
        return summary;
    }

    /**
     * Setter
     *
     * @param summary the summary to set
     */
    public void setSummary(String summary) {
        this.summary = summary;
    }

    /**
     * @return the statusCode
     */
    public int getStatusCode() {
        return statusCode;
    }

    /**
     * @param reponseCode the statusCode to set
     */
    protected void setStatusCode(int reponseCode) {
        this.statusCode = reponseCode;
    }

    /**
     * Return an {@link Error} details for that API response
     *
     * @return the current error for that response
     */
    public Error getError() {
        return error;
    }

    /**
     * Provides an {@link Error} details for that API response
     *
     * @param error current error for that response
     */
    protected void setError(Error error) {
        this.error = error;
    }

    /**
     * Error elements returned in case of API code error response.
     */
    class Error implements Serializable {
        String message;
    }

}
