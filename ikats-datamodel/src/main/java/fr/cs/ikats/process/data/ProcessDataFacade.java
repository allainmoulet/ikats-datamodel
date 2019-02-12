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

package fr.cs.ikats.process.data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.annotation.PreDestroy;

import org.apache.log4j.Logger;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.process.data.dao.ProcessDataDAO;
import fr.cs.ikats.process.data.model.ProcessData;

/**
 * Facade to the storage facility for datasets
 */
@Component("ProcessDataFacade")
@Scope("singleton")
public class ProcessDataFacade {

    /**
     * the logger instance for this class
     */
    private static final Logger LOGGER = Logger.getLogger(ProcessDataFacade.class);

    /**
     * DataSetFacade the DAO for access to MetaData storage
     */
    private ProcessDataDAO dao = new ProcessDataDAO();

    /**
     * @param data   processData
     * @param is     input stream
     * @param length size of data, could be -1 to read until the end of the stream.
     * @return the internal identifier
     * @throws IOException
     */
    public String importProcessData(ProcessData data, InputStream is, int length) throws IkatsDaoException, IOException {

        // Prepare a buffer to get the table of bytes from the InputStream
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        // Put a boolean flag for the case where length = -1
        boolean canRead = true;

        for (int i = 0; i < length || canRead; i++) {
            int read = is.read();
            if (read != -1) {
                buffer.write(read);
            } else {
                canRead = false;
            }
        }
        buffer.flush();

        return dao.persist(data, buffer.toByteArray());
    }

    /**
     * Import a data to database
     *
     * @param processData processData to use
     * @param data        data to save
     * @return the internal identifier
     */
    public String importProcessData(ProcessData processData, byte[] data) throws IkatsDaoException {
        return dao.persist(processData, data);
    }

    /**
     * get all processData for processId
     *
     * @param processId the producer
     * @return empty collection if nothing is found. null if hibernate error occured
     */
    public List<ProcessData> getProcessData(String processId) throws IkatsDaoException {
        return dao.getProcessData(processId);
    }

    public boolean alreadyExistsInProcessData(String processId) throws IkatsDaoException {
        return countProcessDataWithProcessId(processId) > 0;
    }

    /**
     * @param processId
     * @return
     */
    public int countProcessDataWithProcessId(String processId) throws IkatsDaoException {
        List<ProcessData> collectionProcessData = dao.getProcessData(processId);
        if (collectionProcessData == null)
            throw new IkatsDaoException("Unexpected Hibernate error: processDataManager.getProcessData(" + processId + ")");

        return collectionProcessData.size();
    }

    /**
     * get the processData for internal id id
     *
     * @param id the internal identifier.
     * @return null if not found.
     */
    public ProcessData getProcessPieceOfData(int id) throws IkatsDaoException {
        return dao.getProcessData(id);
    }

    /**
     * List all Tables
     *
     * @return the list of all tables
     */
    public List<ProcessData> listTables() throws IkatsDaoException {
        return dao.listTables();
    }

    /**
     * remove all processResults for processId
     *
     * @param processId the producer
     * @throws IkatsDaoException if error occurs in database
     */
    public void removeProcessData(String processId) throws IkatsDaoException {
        dao.removeAllProcessData(processId);
    }

    /**
     * destroy the facade
     */
    @PreDestroy
    public void destroy() {
        dao.stop();
    }
}
