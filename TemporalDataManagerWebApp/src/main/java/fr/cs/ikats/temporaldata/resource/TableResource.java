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
 * @author Maxime PERELMUTER <maxime.perelmuter@c-s.fr>
 * @author Pierre BONHOURE <pierre.bonhoure@c-s.fr>
 */

package fr.cs.ikats.temporaldata.resource;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.metadata.MetaDataFacade;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.operators.IkatsOperatorException;
import fr.cs.ikats.operators.JoinTableWithTs;
import fr.cs.ikats.operators.TablesMerge;
import fr.cs.ikats.operators.TrainTestSplitTable;
import fr.cs.ikats.table.TableEntitySummary;
import fr.cs.ikats.temporaldata.business.MetaDataManager;
import fr.cs.ikats.temporaldata.business.table.Table;
import fr.cs.ikats.temporaldata.business.table.TableInfo;
import fr.cs.ikats.temporaldata.business.table.TableManager;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.IkatsJsonException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.utils.Chronometer;
import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.FormDataParam;

/**
 * resource for Table
 */
@Path("table")
public class TableResource extends AbstractResource {

    /**
     *
     */

    private static Logger logger = Logger.getLogger(TableResource.class);

    /**
     * TableManager
     */
    private TableManager tableManager;

    /**
     * init the processDataManager
     */
    public TableResource() {

        tableManager = new TableManager();
        metadataManager = new MetaDataManager();

    }


    /**
     * get the JSON result as an attachement file in the response.
     *
     * @param tableName the name of the table to retrieve
     * @return a Response with content-type json
     * @throws ResourceNotFoundException if table not found
     * @throws IkatsDaoException         if hibernate exception raised while storing table in db
     * @throws IkatsException            others unexpected exceptions
     */
    @GET
    @Path("/{tableName}")
    public Response downloadTable(@PathParam("tableName") String tableName) throws ResourceNotFoundException, IkatsException, IkatsDaoException, IkatsJsonException, IOException, ClassNotFoundException {

        // get table in db by name
        TableInfo table = tableManager.readFromDatabase(tableName);
        try {

            String jsonString = tableManager.serializeToJson(table);

            return Response.ok(jsonString, MediaType.APPLICATION_JSON_TYPE).build();
        } catch (Exception e) {
            throw new IkatsException("Failed: service downloadTable() " + tableName + " : caught unexpected Throwable:", e);
        }

    }

    /**
     * Database (processData table) import of a csv table
     *
     * @param tableIn json mapping TableInfo
     * @return the internal id
     * @throws IkatsDaoException     error while accessing database to check if table already exists
     * @throws IkatsException        error when serializing table
     * @throws InvalidValueException if table name does not match expected pattern
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createTable(TableInfo tableIn) throws IkatsDaoException, InvalidValueException, IkatsException {

        String tableName = tableIn.table_desc.name;

        // check that tableName does not already exist
        if (tableManager.existsInDatabase(tableName)) {
            String context = "Table name already exists : " + tableName;
            logger.error(context);
            return Response.status(Response.Status.CONFLICT).entity(context).build();
        }

        tableManager.createInDatabase(tableIn);
        return Response.status(Status.OK).entity(tableName).build();

    }

    /**
     * Database (processData table) import of a csv table
     *
     * @param tableName       name of the table
     * @param fileis          the file input stream
     * @param fileDisposition information about the Multipart with file
     * @param rowName         table row name for unique id
     * @param formData        the form data
     * @param uriInfo         all info on URI
     * @return the internal id
     * @throws IOException           error when parsing input csv file
     * @throws IkatsDaoException     error while accessing database to check if table already exists
     * @throws IkatsJsonException    error when mapping imported table to business object Table
     * @throws InvalidValueException if table name does not match expected pattern
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response importTableFromCSV(@FormDataParam("tableName") String tableName,
                                       @FormDataParam("file") InputStream fileis,
                                       @FormDataParam("file") FormDataContentDisposition fileDisposition,
                                       @FormDataParam("rowName") String rowName, FormDataMultiPart formData,
                                       @Context UriInfo uriInfo) throws IkatsException, IOException, IkatsDaoException, InvalidValueException {

        // check output table name validity
        tableManager.validateTableName(tableName, "importTableFromCSV");

        // check that tableName does not already exist
        if (tableManager.existsInDatabase(tableName)) {
            String context = "Table name already exists : " + tableName;
            logger.error(context);
            return Response.status(Response.Status.CONFLICT).entity(context).build();
        }

        Chronometer chrono = new Chronometer(uriInfo.getPath(), true);
        String fileName = fileDisposition.getFileName();
        try {
            logger.info("Import csv file : " + fileName);
            logger.info("Table Name : " + tableName);

            BufferedReader reader = new BufferedReader(new InputStreamReader(fileis));
            String separator = ",";
            Integer rowIndexId = -1;
            List<String> columnHeaders;
            List<String> rowHeaders = new ArrayList<>();

            // consume header to retrieve column index of unique identifier in the table
            columnHeaders = Arrays.asList(reader.readLine().split(separator));
            for (int i = 0; i < columnHeaders.size(); i++) {
                if (!columnHeaders.get(i).isEmpty() && columnHeaders.get(i).equals(rowName)) {
                    rowIndexId = i;
                    break;
                }
            }

            if (rowIndexId == -1) {
                String context = "Row name not found in csv file header : " + rowName;
                logger.error(context);
                return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
            }

            HashMap<String, String> keyTableMap = new HashMap<>();
            String line;
            // first element of row header is column header : set to null
            rowHeaders.add(null);

            // init output table with both columns/rows header
            Table outputTable = TableManager.initEmptyTable(true, true);

            // init line counter
            Integer lineNb = 1;

            // parse csv content to :
            // 1. retrieve data to build json table structure to store
            // 2. fix duplicates : if occurs, we stop at first duplicate and
            // send back 409 http code
            while ((line = reader.readLine()) != null) {

                // increment line number
                lineNb++;

                // skip empty lines
                if (line.isEmpty()) {
                    continue;
                }

                String[] items = line.split(separator, -1);
                // check table content consistency
                if (items.length != columnHeaders.size()) {
                    String context = "CSV line "+ lineNb + " : length does not fit headers size in file " + fileName;
                    logger.error(context);
                    return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
                }

                // retrieving table data
                rowHeaders.add(items[0]);
                List<String> newRow = new ArrayList<>();
                for (int i = 1; i < items.length; i++) {
                    newRow.add(items[i]);
                }
                // add row to output table
                outputTable.appendRow(newRow);

                // seeking for duplicates
                String idRef = items[rowIndexId];
                if (!keyTableMap.containsKey(idRef)) {
                    keyTableMap.put(idRef, "NA");
                } else {
                    String context = "Duplicate found in csv file: " + rowName + " = " + idRef;
                    logger.error(context);
                    return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
                }
            }

            // fill table description
            outputTable.setName(tableName);
            outputTable.setTitle(tableName);
            outputTable.setDescription(fileName);

            // fill headers
            outputTable.getColumnsHeader().addItems(columnHeaders.toArray());
            outputTable.getRowsHeader().addItems(rowHeaders.toArray());

            // store Table
            tableManager.createInDatabase(outputTable.getTableInfo());
            chrono.stop(logger);

            // table name is returned in the body
            return Response.status(Response.Status.OK).entity(tableName).build();

        } catch (IOException e) {
            String contextError = "Unexpected interruption while parsing CSV file : " + fileName;
            throw new IOException(contextError, e);
        }

    }

    /**
     * Extends the table defined by the tableName parameter, by adding one
     * column per selected metric:
     * <ul>
     * <li>Insert column header having the metric name,</li>
     * <li>Insert cells of timeseries references, from selected dataset,
     * selected by the join column, present in original table.</li>
     * </ul>
     * <p>
     * Join principle: for the inserted column J for metric XXX, each joined
     * timeseries at table[i,J] has the specified metric metadata value=XXX and
     * has the metadata
     * <ul>
     * <li>with name defined by the parameter joinMetaName</li>,
     * <li>whose value is equal to the value table[i,K] from join column K,
     * defined by the parameter joinColName.</li>
     * </ul>
     * <p>
     * Created columns are inserted according to the parameter targetColName.
     *
     * @param tableName       table name
     * @param metrics         selected metrics separated by ";". Spaces are ignored.
     * @param dataset         the dataset name.
     * @param joinColName     the name of the table column used by the join. Optional: if
     *                        undefined (""), the first column will be used by the join.
     * @param joinMetaName    defines the name of metadata used by the join, useful when the
     *                        column and metadata names are different. Optional default is
     *                        undefined (""): if joinMetaName is undefined, then the
     *                        metadata has the name of the table column used by the join
     *                        (see joinColName), and if both criteria (joinColName +
     *                        joinMetaName) are undefined: it is assumed that the first
     *                        column header provides the expected metadata name.
     * @param targetColName   name of the target column. Optional: default is undefined
     *                        (""). When target name is defined, the joined columns are
     *                        inserted before the target column; when undefined, the joined
     *                        columns are appended at the end.
     * @param outputTableName name of the table joined by metric, and created in the
     *                        database. The name ought to be conformed to the pattern:
     *                        {@link TableManager#TABLE_NAME_PATTERN}
     * @return
     * @throws IkatsDaoException         a database access error occured during the service.
     * @throws InvalidValueException     error raised if one of the inputs is invalid.
     * @throws ResourceNotFoundException error raised if one of the resource required by computing is
     *                                   not found
     * @throws IkatsException            unexpected error occured on the server.
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/join/metrics")
    public Response joinByMetrics(@FormDataParam("tableName") String tableName,
                                  @FormDataParam("metrics") String metrics,
                                  @FormDataParam("dataset") String dataset,
                                  @FormDataParam("joinColName") @DefaultValue("") String joinColName,
                                  @FormDataParam("joinMetaName") @DefaultValue("") String joinMetaName,
                                  @FormDataParam("targetColName") @DefaultValue("") String targetColName,
                                  @FormDataParam("outputTableName") String outputTableName)
            throws ResourceNotFoundException, IkatsException,
            IkatsDaoException, InvalidValueException {

        // check output table name validity
        tableManager.validateTableName(outputTableName, "joinByMetrics");

        // check that tableName does not already exist
        if (tableManager.existsInDatabase(outputTableName)) {
            String context = "Table name already exists : " + outputTableName;
            logger.error(context);
            return Response.status(Response.Status.CONFLICT).entity(context).build();
        }

        // delegates the work to the operator JoinTableWithTs
        JoinTableWithTs opeJoinTableWithTs = new JoinTableWithTs();
        TableInfo tableInfo = tableManager.readFromDatabase(tableName);
        opeJoinTableWithTs.apply(tableInfo, metrics, dataset, joinColName, joinMetaName, targetColName, outputTableName);

        // table name is returned in the body
        return Response.status(Response.Status.OK).entity(outputTableName).build();

    }

    /**
     * Table process to :
     * - change table key from metadata (populationId)
     * - add metadata (metaName) value to original column headers  : metaName_colHeader
     * <p>
     * Input table first column must be time series functional identifiers
     *
     * @param tableName       the table to convert (json)
     * @param metaName        name of metadata to concat with agregates ref
     * @param populationId    id of population (which is in fact a metadata name) = key of output table
     * @param outputTableName name of the table generated
     * @param formData        the form data
     * @param uriInfo         all info on URI
     * @return the internal id
     * @throws IOException               error when parsing input csv file
     * @throws IkatsDaoException         error while accessing database to check if table already exists
     * @throws ResourceNotFoundException if table not found
     * @throws InvalidValueException     if table name does not match expected pattern
     * @throws IkatsException            others unexpected exceptions
     */
    @POST
    @Path("/ts2feature")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response ts2Feature(@FormDataParam("tableName") String tableName,
                               @FormDataParam("metaName") String metaName,
                               @FormDataParam("populationId") String populationId,
                               @FormDataParam("outputTableName") String outputTableName,
                               FormDataMultiPart formData,
                               @Context UriInfo uriInfo) throws IOException, IkatsDaoException, IkatsException, ResourceNotFoundException, InvalidValueException {


        if (outputTableName == null) {
            // check outputTableName is not null
            String context = "outputTableName shall not be null";
            return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
        }

        if (tableManager.existsInDatabase(outputTableName)) {
            // check output table name does not already exists
            String context = "Output table name already exists : choose a different one (" + outputTableName + ")";
            return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
        }

        if (populationId == null) {
            // check population id is not null
            String context = "populationId shall not be null";
            return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
        }

        Chronometer chrono = new Chronometer(uriInfo.getPath(), true);

        // retrieve table tableName from db
        TableInfo tableInfo = tableManager.readFromDatabase(tableName);
        Table table = tableManager.initTable(tableInfo, false);

        logger.info("Working on table (" + table.getDescription() + ") " +
                "with metaName (" + metaName + ") " +
                "and with populationId (" + populationId + ")");
        logger.info("Output table name is (" + outputTableName + ")");

        // retrieve headers
        List<Object> colHeaders = table.getColumnsHeader().getItems(Object.class);
        List<Object> rowHeaders = table.getRowsHeader().getItems(Object.class);

        MetaDataFacade metaFacade = new MetaDataFacade();
        List<String> colPopId = new ArrayList<>();
        List<String> colMetaTs = new ArrayList<>();
        for (int i = 1; i < rowHeaders.size(); i++) {

            // retrieving tsuids of input
            String tsuid = metaFacade.getFunctionalIdentifierByFuncId(rowHeaders.get(i).toString()).getTsuid();
            MetaData metaTs = metaFacade.getMetaData(tsuid, metaName);
            MetaData metaPopId = metaFacade.getMetaData(tsuid, populationId);

            // filling new colPopId column
            colPopId.add(metaPopId.getValue());

            // filling new colMetaTs column
            colMetaTs.add(metaTs.getValue());
        }

        // processing an ordered list of populationId values without duplicates
        Set<String> setPopId = new HashSet<>(colPopId);
        List<String> listPopId = new ArrayList<>(setPopId);
        Collections.sort(listPopId);

        // processing an ordered list of metaName values by ts without duplicates
        Set<String> setMetaTs = new HashSet<>(colMetaTs);
        List<String> listMetaTs = new ArrayList<>(setMetaTs);
        Collections.sort(listMetaTs);

        // init empty table managing rows/columns headers
        Table outputTable = TableManager.initEmptyTable(true, true);
        outputTable.getColumnsHeader().addItem(populationId);
        for (String metaTs : listMetaTs) {
            for (int i = 1; i < colHeaders.size(); i++) {
                outputTable.getColumnsHeader().addItem(metaTs + "_" + colHeaders.get(i));
            }
        }
        int tableContentWidth = outputTable.getColumnsHeader().getItems().size() - 1;

        // filling rows headers and content by popId
        outputTable.getRowsHeader().addItem(null);
        for (String popId : listPopId) {
            outputTable.getRowsHeader().addItem(popId);
            List<Integer> listIndexPopId = retrieveIndexesListOfEltInList(popId, colPopId);

            List<Object> cellsLine = new ArrayList<>();
            for (String metaTS : listMetaTs) {
                for (Integer index : listIndexPopId) {
                    if (metaTS.equals(colMetaTs.get(index))) {
                        cellsLine.addAll(table.getRow(index + 1, Object.class));
                    }
                }
            }
            // check line size is consistent
            if (cellsLine.size() != tableContentWidth) {
                String context = "Output table : line length inconsistency";
                return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
            }
            outputTable.appendRow(cellsLine);
        }
        // store table in db
        outputTable.setName(outputTableName);
        tableManager.createInDatabase(outputTable.getTableInfo());

        chrono.stop(logger);

        // table name is returned in the body
        return Response.status(Response.Status.OK).entity(outputTableName).build();
    }

    /**
     * Return list of indexes where element is present in the list
     */
    private List<Integer> retrieveIndexesListOfEltInList(String element, List<String> list) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).equals(element)) {
                result.add(i);
            }
        }
        return result;
    }

    /**
     * Read the Table from database, using media-type
     * (with DAO Table: merge equivalent services readTable <=> downlodTable into one compliant with final solution)
     *
     * @param name unique identifier of the table
     * @return the table read from database
     * @throws IkatsJsonException        error parsing the json content from the database
     * @throws IkatsDaoException         database access error
     * @throws ResourceNotFoundException resource not found in the database, for specified name
     */
    @GET
    @Path("/json/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public TableInfo readTable(@PathParam("name") String name) throws IkatsException, IkatsDaoException, ResourceNotFoundException, IOException, ClassNotFoundException {
        TableManager tableMgt = new TableManager();
        return tableMgt.readFromDatabase(name);
    }

    /**
     * Table process to :
     * - change table key from metadata (populationId)
     * - add metadata (metaName) value to original column headers  : metaName_colHeader
     * <p>
     * Input table first column must be time series functional identifiers
     *
     * @param tableName the table to convert
     * @param formData  the form data
     * @param uriInfo   all info on URI
     * @return the internal id
     * @throws IOException               error when parsing input csv file
     * @throws IkatsDaoException         error while accessing database to check if table already exists
     * @throws InvalidValueException     if table name does not match expected pattern
     * @throws IkatsException            row from input table is undefined
     * @throws ResourceNotFoundException row from input table is not found
     */
    @POST
    @Path("/traintestsplit")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response trainTestSplit(@FormDataParam("tableName") String tableName,
                                   @FormDataParam("targetColumnName") @DefaultValue("") String targetColumnName,
                                   @FormDataParam("repartitionRate") @DefaultValue("0.5") double repartitionRate,
                                   @FormDataParam("outputTableName") String outputTableName,
                                   FormDataMultiPart formData,
                                   @Context UriInfo uriInfo) throws IOException, IkatsDaoException, IkatsException, InvalidValueException, ResourceNotFoundException {


        // check output table name validity
        tableManager.validateTableName(outputTableName, "trainTestSplit");

        // check that outputTableName does not already exist
        if (tableManager.existsInDatabase(outputTableName)) {
            String context = "Table name already exists : " + outputTableName;
            logger.error(context);
            return Response.status(Response.Status.CONFLICT).entity(context).build();
        }

        // Creates the request to the operator. Should be replaced in a future version by the JAXRS JSON transformation
        // from the HTTP request. See mergeTables(Request)
        TrainTestSplitTable.Request request = new TrainTestSplitTable.Request();
        request.tableName = tableName;
        request.targetColumnName = targetColumnName;
        request.repartitionRate = repartitionRate;
        request.outputTableName = outputTableName;

        // Try to initialize the operator with the request
        TrainTestSplitTable trainTestSplitTable = new TrainTestSplitTable(request);

        Chronometer chrono = new Chronometer(uriInfo.getPath(), true);
        trainTestSplitTable.apply();
        chrono.stop(logger);

        // tables names are returned in the body
        return Response.status(Response.Status.OK).entity(
                outputTableName + "_Train" + "," + outputTableName + "_Test").build();

    }


    /**
     * Operator call for the merge of two tables with a join column.<br>
     * See {@link TablesMerge.Request} for JSON input specification
     *
     * @param request
     * @return the HTTP response with the merged table as content
     */
    @POST
    @Path("/merge")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response mergeTables(TablesMerge.Request request) throws InvalidValueException, IkatsDaoException {

        // check output table name validity
        tableManager.validateTableName(request.outputTableName, "mergeTables");

        // check that outputTableName does not already exist
        if (tableManager.existsInDatabase(request.outputTableName)) {
            String context = "Table name already exists : " + request.outputTableName;
            logger.error(context);
            return Response.status(Response.Status.CONFLICT).entity(context).build();
        }
        TablesMerge tablesMergeOperator;
        try {
            // Try to initialize the operator with the request
            tablesMergeOperator = new TablesMerge(request);
        } catch (IkatsOperatorException e) {
            // The request check has failed
            return Response.status(Status.BAD_REQUEST).entity(e).build();
        }

        try {
            // check tables existence
            for (String tableName : request.tableNames) {
                if (!tableManager.existsInDatabase(tableName)) {
                    String msg = "Table " + tableName + " not found";
                    return Response.status(Status.BAD_REQUEST).entity(msg).build();
                }
            }
        } catch (IkatsDaoException e) {
            // Hibernate Exception raised
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e).build();
        }

        try {
            // Do the job and return the name of the table
            tablesMergeOperator.apply();
            return Response.status(Status.OK).entity(request.outputTableName).build();
        } catch (IkatsOperatorException | IkatsException e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e).build();
        } catch (IkatsDaoConflictException e) {
            return Response.status(Status.CONFLICT).entity(e).build();
        }

    }


    /**
     * List all tables found in database <br>
     *
     * @return the HTTP response with the list of all table as content
     */
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response listTables() {
        // List the tables
        List<TableEntitySummary> tables;
        try {
            tables = tableManager.listTables();
        } catch (IkatsDaoException e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e).build();
        }

        if (tables == null) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Error occurred while reading Table").build();
        }

        return Response.status(Status.OK).entity(tables).build();

    }


    /**
     * Delete a Table
     *
     * @param tableName the name of the table to delete
     * @return the HTTP response: useful in case of error
     */
    @DELETE
    @Path("/{tableName}")
    public Response removeTable(@PathParam("tableName") String tableName) throws ResourceNotFoundException {
        try {
            tableManager.deleteFromDatabase(tableName);
        } catch (IkatsDaoException e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e).build();
        }
        return Response.status(Status.NO_CONTENT).build();
    }
}

