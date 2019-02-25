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

package fr.cs.ikats.temporaldata.business;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.process.data.ProcessDataFacade;
import fr.cs.ikats.process.data.model.ProcessData;
import fr.cs.ikats.temporaldata.application.TemporalDataApplication;

/**
 * Manager for ProcessData data
 */
public class ProcessDataManager {

    /**
     * private method to get the MetaDataFacade from Spring context.
     *
     * @return
     */
    private ProcessDataFacade getProcessDataFacade() {
        return TemporalDataApplication.getApplicationConfiguration().getSpringContext().getBean(ProcessDataFacade.class);
    }


    /**
     * import an inputStream into database
     *
     * @param fileis     the inputStream
     * @param fileLength size of data
     * @param name       name of data
     * @param processId  the data producer identifier
     * @param dataType   the dataType
     *
     * @return the internal identifier of the result.
     *
     * @throws IOException In case of error when reading fileis
     */
    public String importProcessData(InputStream fileis, Long fileLength, String processId, String dataType, String name)
            throws IkatsDaoException, IOException {
        ProcessData data = new ProcessData(processId, dataType, name);
        return getProcessDataFacade().importProcessData(data, fileis, fileLength.intValue());
    }

    /**
     * Import a string/opaque byte array to database, with default type "ANY"
     *
     * @param processId the data producer identifier
     * @param name      name of data
     * @param data      the data
     *
     * @return the internal identifier of the result.
     */
    public String importProcessData(String processId, String name, byte[] data) throws IkatsDaoException {
        ProcessData processData = new ProcessData(processId, "ANY", name);
        return getProcessDataFacade().importProcessData(processData, data);
    }

    /**
     * Import a string/opaque byte array to database, with specified datatype: this is required for the JSON or CSV
     * results !
     *
     * @param processId the data producer identifier
     * @param name      name of data
     * @param data      the data
     * @param type      among possible enum values.
     *
     * @return the internal identifier of the result.
     */
    public String importProcessData(String processId, String name, byte[] data, ProcessResultTypeEnum type) throws IkatsDaoException {
        ProcessData processData = new ProcessData(processId, type.toString(), name);
        return getProcessDataFacade().importProcessData(processData, data);
    }

    /**
     * ResultType ENUM
     */
    public enum ProcessResultTypeEnum {
        /**
         * Value for JSON type
         */
        JSON,
        /**
         * Value for CSV file type
         */
        CSV,
        /**
         * Value for Any other type
         */
        ANY
    }


    /**
     * get a single processResult for internal identifier id.
     *
     * @param id the internal process identifier
     *
     * @return null if nothing is found.
     */
    public ProcessData getProcessPieceOfData(int id) throws IkatsDaoException {
        return getProcessDataFacade().getProcessPieceOfData(id);
    }

    /**
     * get All processResults for a processId.
     *
     * @param processId the process execution identifier.
     *
     * @return null if not processResult is found.
     */
    public List<ProcessData> getProcessData(String processId) throws IkatsDaoException {
        return getProcessDataFacade().getProcessData(processId);
    }

    /**
     * get All Tables
     *
     * @return the list of all tables. null returned only in case of error.
     */
    public List<ProcessData> listTables() throws IkatsDaoException {
        return getProcessDataFacade().listTables();
    }

    /**
     * remove all processResults for a processId.
     *
     * @param processId the process exec identifier.
     *
     * @throws IkatsDaoException if error occurs in database
     */
    public void removeProcessData(String processId) throws IkatsDaoException {
        getProcessDataFacade().removeProcessData(processId);
    }

}
