/**
 * Copyright 2018 CS Systèmes d'Information
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

import java.util.HashMap;
import java.util.Map;

/**
 * content result of an opentsdb import operation
 */
public class ImportResult extends ApiResponse {

    private String tsuid;
    private String funcId;
    private long numberOfSuccess;
    private long numberOfFailed;
    private long startDate;
    private long endDate;
    private Map<String, String> errors;

    /**
     * default constructor.
     */
    public ImportResult() {
        errors = new HashMap<String, String>();
    }

    /**
     * Getter
     *
     * @return the tsuid
     */
    public String getTsuid() {
        return tsuid;
    }

    /**
     * Setter
     *
     * @param tsuid the tsuid to set
     */
    public void setTsuid(String tsuid) {
        this.tsuid = tsuid;
    }

    /**
     * couple of (key,error) to add
     *
     * @param key   the error key
     * @param error error
     */
    public void addError(String key, String error) {
        errors.put(key, error);
    }

    /**
     * add an error to the map
     *
     * @param errors map of errors to set
     */
    public void addErrors(Map<String, String> errors) {
        errors.putAll(errors);
    }

    /**
     * Getter
     *
     * @return the numberOfSuccess
     */
    public long getNumberOfSuccess() {
        return numberOfSuccess;
    }

    /**
     * Setter
     *
     * @param numberOfSuccess numberOfSuccess to set
     */
    public void setNumberOfSuccess(long numberOfSuccess) {
        this.numberOfSuccess = numberOfSuccess;
    }

    /**
     * Getter
     *
     * @return the errors
     */
    public Map<String, String> getErrors() {
        return errors;
    }

    /**
     * Getter
     *
     * @return the timeseries start date
     */
    public long getStartDate() {
        return startDate;
    }

    /**
     * Setter
     *
     * @param startDate the startDate to set
     */
    public void setStartDate(long startDate) {
        this.startDate = startDate;
    }

    /**
     * Getter
     *
     * @return the timeseries end date
     */
    public long getEndDate() {
        return endDate;
    }

    /**
     * Setter
     *
     * @param endDate the end_date to set
     */
    public void setEndDate(long endDate) {
        this.endDate = endDate;
    }

    /**
     * Getter
     *
     * @return the funcId
     */
    public String getFuncId() {
        return funcId;
    }

    /**
     * Setter
     *
     * @param funcId the funcId to set
     */
    public void setFuncId(String funcId) {
        this.funcId = funcId;
    }

    /**
     * Return a string representation based on Apache commons ToStringStyle.DEFAULT_STYLE
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append("[tsuid=").append(this.tsuid).append(",");
        sb.append("funcId=").append(this.funcId).append(",");
        sb.append("startDate=").append(this.startDate).append(",");
        sb.append("endDate=").append(this.endDate).append(",");
        sb.append("numberOfSuccess=").append(this.numberOfSuccess).append(",");
        sb.append("summary=").append(this.getSummary()).append(",");
        sb.append("errors=").append(this.errors).append("]");

        return sb.toString();
    }

    /**
     * @return the numberOfFailed
     */
    public long getNumberOfFailed() {
        return numberOfFailed;
    }

    /**
     * @param numberOfFailed the numberOfFailed to set
     */
    public void setNumberOfFailed(long numberOfFailed) {
        this.numberOfFailed = numberOfFailed;
    }
}

