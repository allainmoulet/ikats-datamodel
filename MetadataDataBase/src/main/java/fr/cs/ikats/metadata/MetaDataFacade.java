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
 * @author Mathieu BERAUD <mathieu.beraud@c-s.fr>
 */

package fr.cs.ikats.metadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import javax.annotation.PreDestroy;

import org.apache.log4j.Logger;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoInvalidValueException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingResource;
import fr.cs.ikats.common.expr.Group;
import fr.cs.ikats.metadata.dao.FunctionalIdentifierDAO;
import fr.cs.ikats.metadata.dao.MetaDataDAO;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.metadata.model.MetaData.MetaType;
import fr.cs.ikats.metadata.model.MetadataCriterion;

/**
 * Facade to manage MetaData
 */
@Component("MetaDataFacade")
@Scope("singleton")
public class MetaDataFacade {

    /**
     * the logger instance for this class
     */
    private static final Logger LOGGER = Logger.getLogger(MetaDataFacade.class);

    /**
     * the DAO for access to MetaData storage
     */
    private MetaDataDAO dao;

    private FunctionalIdentifierDAO idDao;

    /**
     * Constructor
     */
    public MetaDataFacade() {
        init();
    }

    /**
     * init the dao and its mapping : use the hibernate.cfg.xml file + add package and classes where annotations are
     * set.
     */
    public void init() {
        dao = new MetaDataDAO();
        dao.init("/metaDataHibernate.cfg.xml");

        dao.addAnotatedPackage("fr.cs.ikats.metadata.model");
        dao.addAnnotatedClass(MetaData.class);
        dao.completeConfiguration();

        idDao = new FunctionalIdentifierDAO();
        idDao.init("/metaDataHibernate.cfg.xml");
        idDao.addAnotatedPackage("fr.cs.ikats.metadata.model");
        idDao.addAnnotatedClass(FunctionalIdentifier.class);
        idDao.completeConfiguration();

    }

    /**
     * Create MetaData in database for a given tsuid, name and value, with default dtype == MetaType.string
     *
     * @param tsuid the tsuid
     * @param name  name of the metadata
     * @param value value for this metadata
     * @return the internal id of the inserted metadata.
     * @throws IkatsDaoConflictException create error raised on conflict with another resource
     * @throws IkatsDaoException         another error from DAO
     */
    public Integer persistMetaData(String tsuid, String name, String value) throws IkatsDaoConflictException, IkatsDaoException {
        return persistMetaData(tsuid, name, value, MetaType.string);
    }

    /**
     * update meta data for a given tsuid, name with value
     *
     * @param tsuid the tsuid
     * @param name  name of the metadata
     * @param value new value for this metadata
     * @return the id of the metadata or null if no update has been performed
     * @throws IkatsDaoConflictException in case of conflict with existing ( tsuid, name ) pair
     * @throws IkatsDaoMissingResource   in case of missing MetaData
     * @throws IkatsDaoException         if the meta doesn't exists or if database can't be accessed
     * @since [#142998] Manage IkatsDaoConflictException, IkatsDaoMissingRessource, IkatsDaoException
     */
    public Integer updateMetaData(String tsuid, String name, String value)
            throws IkatsDaoConflictException, IkatsDaoMissingResource, IkatsDaoException {

        // [#142998] do not need to test the results from facade/dao:
        // exception is raised if result is not good
        MetaData metadata = getMetaData(tsuid, name);
        if (metadata == null) {
            throw new IkatsDaoMissingResource("Metadata " + name + "does not exists for tsuid <" + tsuid + ">");
        }
        metadata.setValue(value);
        dao.update(metadata);
        return metadata.getId();
    }

    /**
     * Create MetaData in database for a given tsuid, name and value, and associated type specified with a String
     *
     * @param tsuid the tsuid
     * @param name  the name
     * @param value the value
     * @param dtype the type: string matching the enumerate MetaType
     * @return key of persisted metadata.
     * @throws IkatsDaoInvalidValueException error raised for invalid dtype value: unmatched enumerate MetaType value
     * @throws IkatsDaoConflictException     create error raised on conflict with another resource
     * @throws IkatsDaoException             another error from DAO
     */
    public Integer persistMetaData(String tsuid, String name, String value, String dtype)
            throws IkatsDaoConflictException, IkatsDaoInvalidValueException, IkatsDaoException {

        MetaType enumType = null;
        try {
            enumType = MetaType.valueOf(dtype);
        } catch (Exception e) {

            throwInvalidDTypeException(tsuid, name, dtype, e);
        }
        return persistMetaData(tsuid, name, value, enumType);
    }

    /**
     * @param tsuid
     * @param name
     * @param dtype
     * @param e
     * @throws IkatsDaoInvalidValueException
     */
    private void throwInvalidDTypeException(String tsuid, String name, String dtype, Throwable e) throws IkatsDaoInvalidValueException {
        throw new IkatsDaoInvalidValueException("Unexpected dtype=" + dtype + " for MetaData on tsuid=" + tsuid + " for name=" + name,
                e);
    }

    /**
     * Create MetaData in database for a given tsuid, name and value, and associated type specified with enum value from
     * MetaType
     *
     * @param tsuid the tsuid
     * @param name  name of the metadata
     * @param value value for this metadata
     * @param dtype data type for this metadata
     * @return the internal id of the inserted metadata.
     * @throws IkatsDaoConflictException create error raised on conflict with another resource
     * @throws IkatsDaoException         another error from DAO
     */
    public Integer persistMetaData(String tsuid, String name, String value, MetaType dtype) throws IkatsDaoConflictException, IkatsDaoException {
        MetaData metadata = new MetaData();
        metadata.setName(name);
        metadata.setTsuid(tsuid);
        metadata.setValue(value);
        metadata.setDType(dtype);

        // List of available data types.
        // Since this is a free string field, the constraint is managed in code
        // here.
        // To extend the possibilities, just add the new dtype in this list
        // The constraint is not performed at dao or model level because
        // this constraint is clearly a business contraint.

        return dao.persist(metadata);
    }

    /**
     * Create MetaData in database from a csv line, using getMetaDataFromCSV parsing method.
     *
     * @param csvLine the line
     * @return the internal identifier
     * @throws IkatsDaoConflictException conflict error with a Metadata already created.
     * @throws IkatsDaoMissingResource   error on missing functional identifier resource: required here
     * @throws IkatsDaoException         other errors
     */
    public Integer persistMetaData(String csvLine) throws IkatsDaoConflictException, IkatsDaoMissingResource, IkatsDaoException {

        // raise error on CSV parsing failure
        MetaData md = getMetaDataFromCSV(csvLine);
        Integer id = dao.persist(md);
        return id;
    }

    /**
     * get Meta Data for TS
     *
     * @param tsuid the tsuid
     * @return List of MetaData
     * @throws IkatsDaoMissingResource error raised when no matching resource is found, for a tsuid different from '*'
     * @throws IkatsDaoException       any error raised by DAO layer.
     */
    public List<MetaData> getMetaDataForTS(String tsuid) throws IkatsDaoMissingResource, IkatsDaoException {
        return dao.listForTS(tsuid);
    }

    /**
     * get all Meta Data types
     *
     * @return List of MetaData
     * @throws IkatsDaoException any error raised by DAO layer.
     */
    public Map<String, String> getMetaDataTypes() throws IkatsDaoException {
        return dao.listTypes();
    }

    /**
     * Retrieve the MetaData matching the criteria tsuid+name
     *
     * @param tsuid the tsuid criterion value
     * @param name  the name criterion value
     * @return the MetaData or null if not exists
     * @throws IkatsDaoConflictException error raised when multiple metadata are found.
     */
    public MetaData getMetaData(String tsuid, String name) throws IkatsDaoException, IkatsDaoConflictException {
        return dao.getMD(tsuid, name);
    }

    /**
     * remove MetaData for a tsuid from database.
     *
     * @param tsuid identifier of ts
     * @return number of removals
     * @throws IkatsDaoException error deleting the MetaData ressources
     */
    public Integer removeMetaDataForTS(String tsuid) throws IkatsDaoException {
        return dao.remove(tsuid);
    }

    /**
     * remove one MetaData for a tsuid from database.
     *
     * @param tsuid identifier of tsuids
     * @param name  name of the metadata
     * @return number of removals (expecting 1 or 0)
     * @throws IkatsDaoException error deleting the resource
     */
    public int removeMetaDataForTS(String tsuid, String name) throws IkatsDaoException {
        return dao.remove(tsuid, name);
    }

    /**
     * get csv representation for one metadata. if functional identifier is found, the first value is the functional
     * identifier, else, the tsuid is printed.
     *
     * @param md the metadata to represent.
     * @return a CSV formated String with : funcId;name;value
     */
    private String getCSVForMetaData(MetaData md) {
        String tsuid = md.getTsuid();
        // default value of idFunc is tsuid,
        // before trying to retrieve actual funcId below
        String idFunc = tsuid;
        try {
            FunctionalIdentifier funcId = getFunctionalIdentifierByTsuid(tsuid);
            if (funcId != null) {
                idFunc = funcId.getFuncId();
            }
        } catch (IkatsDaoException e) {
            LOGGER.warn("Failed to retrieve functional ID associated to tsuid=" + tsuid, e);
            LOGGER.warn(" => CSV MetaData:first column: TSUID instead of FUNCID");
            idFunc = tsuid;
        }
        return idFunc + ";" + md.getName() + ";" + md.getValue();
    }

    /**
     * create a metadata instance from data line formated as : <ul> <li><b>funcId;name;value</b></li> <li>or
     * <b>funcId;name#type;value</b></li> </ul> where type is among the values defined by MetaData.MetaType.
     *
     * @param line
     * @return
     * @throws IkatsDaoMissingResource       error when there is a missing functional identifier
     * @throws IkatsDaoInvalidValueException error when there is an invalid metadata type defined in the CSV
     */
    public MetaData getMetaDataFromCSV(String line)
            throws IkatsDaoMissingResource, IkatsDaoInvalidValueException, IkatsDaoConflictException, IkatsDaoException {

        StringTokenizer tokenizer = new StringTokenizer(line, ";");
        MetaData md = null;

        String idFunc = extractNextValue(tokenizer);

        FunctionalIdentifier funcId = getFunctionalIdentifierByFuncId(idFunc);
        if (funcId != null) {
            md = new MetaData();

            String tsuid = funcId.getTsuid();
            md.setTsuid(tsuid);

            String nameAndType = extractNextValue(tokenizer);
            String name;

            String value = extractNextValue(tokenizer);
            md.setValue(value);
            MetaType dtype = MetaType.string;

            // Avoid using: nameAndType.replaceAll("\"", "").split("#")
            // because one value part may contain a '#', which causes a failure
            int indexSplit = nameAndType.lastIndexOf('#');
            if (indexSplit != -1) {
                name = nameAndType.substring(0, indexSplit);
                String parsedType = nameAndType.substring(indexSplit + 1);
                dtype = MetaType.valueOf(parsedType);

            } else {
                name = nameAndType;
            }
            md.setName(name);
            md.setDType(dtype);

        } else {
            String msg = "Importing metadata row from CSV: unknown functional identifier [" + idFunc
                    + "] is not registered in database. Unable to retrieve the corresponding tsuid.";
            LOGGER.error(msg);
            throw new IkatsDaoMissingResource(msg);
        }
        return md;
    }

    /**
     * Try to parse the whole CSV file. Blank lines are ignored. One error on valid line is discarding the parsing, once
     * every problems are logged.
     *
     * @param fileis
     * @return
     * @throws IkatsDaoMissingResource
     * @throws IkatsDaoInvalidValueException
     */
    public List<MetaData> getMetaDataFromCSV(InputStream fileis) throws IOException, IkatsDaoException {
        List<MetaData> results = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileis));
        String line = "<not yet read>";
        ArrayList<String> metadataNames = new ArrayList<>();
        int lineNumber = -1;
        String separator = ";";
        Exception firstError = null;
        try {
            // retrieve the metadata names from the header
            String[] items = reader.readLine().split(separator);
            for (String item : items) {
                if (!item.isEmpty()) {
                    metadataNames.add(item.replaceAll("\"", ""));
                }
            }
            line = reader.readLine();
            lineNumber = 2;
            String msgCurrentLine = null;
            Exception errorWithCurrentLine = null;
            if (line == null) {
                firstError = new IOException("Empty content of CSV of MetaData");
            }
            while (line != null) {
                msgCurrentLine = null;
                errorWithCurrentLine = null;

                // first check if the line is correctly formatted
                if (!line.isEmpty() && line.contains(separator)) {
                    ArrayList<String> metadataValues = new ArrayList<>();
                    // retrieve functional id or metric + values of metadata
                    String[] values = line.split(separator, -1);
                    String metricOrFuncId = values[0].replaceAll("\"", "");
                    for (int i = 1; i < values.length; i++) {
                        metadataValues.add(values[i].replaceAll("\"", ""));
                    }
                    // check of csv data line size
                    if (metadataNames.size() != metadataValues.size()) {
                        errorWithCurrentLine = new IOException("Bad CSV syntax");
                        msgCurrentLine = "Import: MetaData CSV line [" + lineNumber + "] is not correctly formated. Skipped: (not enough metadata values)";
                    } else {
                        try {
                            List<MetaData> metas = getMetaImportListFromCSV(metricOrFuncId, metadataNames, metadataValues);
                            results.addAll(metas);
                        } catch (IkatsDaoException e) {
                            errorWithCurrentLine = e;
                            msgCurrentLine = "Import: MetaData CSV line [" + lineNumber + "] is skipped: one error occured: ";
                        }
                    }
                } else {
                    errorWithCurrentLine = new IOException("Bad CSV syntax");
                    msgCurrentLine = "Import: MetaData CSV line [" + lineNumber + "] is not correctly formated. Skipped: ";
                }
                if (errorWithCurrentLine != null) {
                    LOGGER.error(msgCurrentLine);
                    LOGGER.error("content=" + line, errorWithCurrentLine);

                    if (firstError == null) {
                        // service
                        firstError = errorWithCurrentLine;
                    }
                    // keep going on with next lines
                }
                line = reader.readLine();
                lineNumber++;
            }
        } catch (IOException finalError) {
            String lineInfo = (line != null) ? line : "<empty line>";
            String contextError = "Unexpected interruption: importing MetaData CSV at [" + lineNumber + "]: " + lineInfo;
            LOGGER.error(contextError, finalError);
            throw new IOException(contextError);
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                LOGGER.error("", e);
            }
        }
        if (firstError != null) {
            if (firstError instanceof IkatsDaoException) {
                throw new IkatsDaoException(firstError);
            } else {
                throw new IOException(firstError);
            }
        }
        return results;
    }

    /**
     * This service creates a list of <ready-to-import> Metadata objects from a csv file : First entry point is a metric
     * or a functional identifier. In the case of a metric is provided, a list of functional identifiers corresponding
     * to this metric is retrieved from db. Second entry point is metadata names and values.
     * <p>
     * The service generates the combinatory between functional ids and metadata names and values to generate a list of
     * metadata objects to be imported.
     *
     * @param metricOrFuncId a metric or a functional identifier
     * @param metadataNames  a list of metadata names
     * @param metadataValues a list of metadata values, corresponding to metadata names provided (same order)
     * @return a list of metadata objects
     * @throws Exception
     */
    private List<MetaData> getMetaImportListFromCSV(String metricOrFuncId, ArrayList<String> metadataNames, ArrayList<String> metadataValues)
            throws IkatsDaoException {
        String separator = ";";
        String dataLineToProcess;
        List<MetaData> results = new ArrayList<MetaData>();
        MetaData currentMetaData = null;

        List<String> funcIds = new ArrayList<String>();
        String currentMDName;
        String currentMDVal;

        if (!isValidFuncId(metricOrFuncId)) {
            // case metric, retrieve list of functional identifiers
            funcIds = dao.getListFuncIdFromMetric(metricOrFuncId);
        } else {
            // case funcId, just add the functional identifier
            funcIds.add(metricOrFuncId);
        }

        // for each func id
        for (String funcId : funcIds) {
            Iterator<String> mdName = metadataNames.iterator();
            Iterator<String> mdVal = metadataValues.iterator();

            // for each meta data
            while (mdName.hasNext() && mdVal.hasNext()) {
                currentMDName = mdName.next();
                currentMDVal = mdVal.next();
                if (!currentMDVal.isEmpty() && !currentMDName.isEmpty()) {
                    dataLineToProcess = funcId + separator + currentMDName + separator + currentMDVal;
                    // create and add to results a new metadata to import
                    currentMetaData = getMetaDataFromCSV(dataLineToProcess);
                    results.add(currentMetaData);
                }
            }
        }
        return results;
    }

    /**
     * This service tests if the item provided is a db-referenced functional identifier
     *
     * @param itemToTest
     * @return boolean: if itemToTest is a valid functional id in db, return true, return false otherwise
     */
    private boolean isValidFuncId(String itemToTest) {
        try {
            getFunctionalIdentifierByFuncId(itemToTest);
            return true;
        } catch (IkatsDaoException e) {
            LOGGER.error("", e);
            return false;
        }
    }

    /**
     * call replaceAll only if required => improve perfo ...
     *
     * @param aTokenizer
     * @return
     */
    private String extractNextValue(StringTokenizer aTokenizer) {

        String lValue = aTokenizer.nextToken();
        if (lValue.indexOf("\"") >= 0) {
            // rare case when value is surrounded by double-quote
            return lValue.replaceAll("\"", "");
        } else {
            return lValue;
        }
    }

    /**
     * get a CSV representation for the mdList in param
     *
     * @param mdList list of metadata
     * @return a csv formated string.
     */
    public String getCSVForMetaData(List<MetaData> mdList) {
        StringBuilder sb = new StringBuilder(getCSVHeaderForMetaData());
        sb.append("\n");
        for (MetaData md : mdList) {
            String csvForMetaData = getCSVForMetaData(md);
            sb.append(csvForMetaData).append("\n");
        }
        return sb.toString();
    }

    /**
     * get csv headers
     *
     * @return the headers in a string
     */
    public String getCSVHeaderForMetaData() {
        return "Tsuid;Name;Value";
    }

    /**
     * create FunctionalIdentifiers from a map of &lt;tsuid,funcId&gt;
     *
     * @param ids the map.
     * @return the number of funcId actually stored in database
     * @throws IkatsDaoException
     */
    public int persistFunctionalIdentifier(Map<String, String> ids) throws IkatsDaoException {
        int count = 0;
        for (String tsuid : ids.keySet()) {
            FunctionalIdentifier id = new FunctionalIdentifier();
            id.setTsuid(tsuid);
            id.setFuncId(ids.get(tsuid));
            try {
                int added = idDao.persist(id);
                count = count + added;
            } catch (Exception e) {
                LOGGER.error("Unable to insert functional identifier " + id.getFuncId(), e);
                throw new IkatsDaoException(e);
            }
        }
        return count;
    }

    /**
     * create one FunctionalIdentifier
     *
     * @param tsuid
     * @param funcid
     * @return
     * @throws IkatsDaoConflictException
     * @throws IkatsDaoException
     */
    public int persistFunctionalIdentifier(String tsuid, String funcid) throws IkatsDaoConflictException, IkatsDaoException {

        int count = 0;
        FunctionalIdentifier id = new FunctionalIdentifier(tsuid, funcid);
        count = idDao.persist(id);
        // TODO error management correction in DAO layer => simplify this
        // code ...
        if (count == 0) {
            throw new IkatsDaoException("Unable to create FunctionalIdentifier tsduid=" + tsuid + " funcId=" + funcid);
        }

        return count;
    }

    /**
     * @param mdList list of metadata to persist in database
     * @param update if true, already existing metadata is updated otherwise no metadata is persisted if one of them
     *               already exists
     * @return a list of imported metadata identifiers in db
     * @throws IkatsDaoConflictException
     * @throws IkatsDaoException
     */
    public List<Integer> persist(List<MetaData> mdList, Boolean update) throws IkatsDaoConflictException, IkatsDaoException {
        return dao.persist(mdList, update);
    }

    /**
     * remove the functional identifiers for the list of tsuids.
     *
     * @param tsuids the tsuids
     * @return number of removals
     * @throws IkatsDaoException
     * @throws IkatsDaoConflictException
     */
    public int removeFunctionalIdentifier(List<String> tsuids) throws IkatsDaoConflictException, IkatsDaoException {
        return idDao.remove(tsuids);
    }

    /**
     * get the corresponding functional identifier for the tsuid single value.
     *
     * @param tsuid the tsuid
     * @return a list of FunctionalIdentifier, or null if nothing is found.
     */
    public FunctionalIdentifier getFunctionalIdentifierByTsuid(String tsuid) throws IkatsDaoException {
        List<String> tsuids = new ArrayList<String>();
        tsuids.add(tsuid);
        FunctionalIdentifier result = null;
        List<FunctionalIdentifier> results = idDao.list(tsuids);
        if ((results != null) && (!results.isEmpty())) {
            result = results.get(0);
        } else {
            result = null;
        }
        return result;
    }

    /**
     * @param funcId the funcId
     * @return a list of FunctionalIdentifier, or null if nothing is found.
     * @see FunctionalIdentifierDAO#getFromFuncId(String)
     */
    public FunctionalIdentifier getFunctionalIdentifierByFuncId(String funcId)
            throws IkatsDaoConflictException, IkatsDaoMissingResource, IkatsDaoException {
        return idDao.getFromFuncId(funcId);
    }

    /**
     * Get the functional identifier list matching the dataset name
     *
     * @param datasetName Name of the dataset to use
     * @return a list of FunctionalIdentifier, or null if nothing is found.
     */
    public List<FunctionalIdentifier> getFunctionalIdentifierFromDataset(String datasetName)
            throws IkatsDaoException {

        List<FunctionalIdentifier> results = idDao.listFromDataset(datasetName);
        return results;
    }

    /**
     * Get the functional identifier list matching the list of tsuid values
     *
     * @param tsuids list of search criteria: tsuid values
     * @return a list of FunctionalIdentifier, or null if nothing is found.
     */
    public List<FunctionalIdentifier> getFunctionalIdentifierByTsuidList(List<String> tsuids) throws IkatsDaoException {

        List<FunctionalIdentifier> results = idDao.list(tsuids);
        return results;
    }

    /**
     * Get the functional identifier list matching the list of functional id values
     *
     * @param funcIds list of search criteria: functional ID values
     * @return a list of FunctionalIdentifier, or null if nothing is found.
     */
    public List<FunctionalIdentifier> getFunctionalIdentifierByFuncIdList(List<String> funcIds) throws IkatsDaoException {

        List<FunctionalIdentifier> results = idDao.listByFuncIds(funcIds);
        return results;
    }

    /**
     * Get all the functional identifier list
     *
     * @return a list of FunctionalIdentifier, or null if nothing is found.
     */
    public List<FunctionalIdentifier> getFunctionalIdentifiersList() throws IkatsDaoException {

        List<FunctionalIdentifier> results = idDao.listAll();
        return results;
    }

    /**
     * destroy the facade
     */
    @PreDestroy
    public void destroy() {
        LOGGER.debug("Destroying MetaDataFacade");
        dao.stop();
        idDao.stop();
    }

    /**
     * Searches the ts identifiers matched by the metadata criteria formula. Facade delegates the searching request to
     * the metadata dao.
     *
     * @param scope:  set of ts where is operated the search
     * @param formula logical expression of metadata criterion
     * @return the result
     * @throws IkatsDaoException
     */
    public List<FunctionalIdentifier> searchFuncId(List<FunctionalIdentifier> scope, Group<MetadataCriterion> formula) throws IkatsDaoException {
        return dao.searchFuncId(scope, formula);
    }

    /**
     * Searches the ts in the dataset matched by the metadata criteria list, with AND group. Facade delegates the
     * searching request to the metadata dao.
     *
     * @param datasetName
     * @param criteria
     * @return the result
     * @throws IkatsDaoException
     */
    public List<FunctionalIdentifier> searchFuncId(String datasetName, List<MetadataCriterion> criteria) throws IkatsDaoException {
        return dao.searchFuncId(datasetName, criteria);
    }

}

