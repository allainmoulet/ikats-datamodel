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

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import fr.cs.ikats.operators.JoinTableWithTs;
import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.FormDataParam;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.metadata.MetaDataFacade;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.temporaldata.business.*;
import fr.cs.ikats.temporaldata.business.DataSetManager;
import fr.cs.ikats.temporaldata.business.MetaDataManager;
import fr.cs.ikats.temporaldata.business.Table;
import fr.cs.ikats.temporaldata.business.TableInfo;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.IkatsJsonException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.utils.Chronometer;

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
     * MetadataManager
     */
    private MetaDataManager metaManager;

    /**
     * DatasetManager
     */
    private DataSetManager datasetManager;

    /**
     * init the processDataManager
     */
    public TableResource() {

        tableManager = new TableManager();
        datasetManager = new DataSetManager();
        metadataManager = new MetaDataManager();

    }


    /**
     * get the JSON result as an attachement file in the response.
     *
     * @param tableName
     *            the name of the table to retrieve
     * @return a Response with content-type json
     * @throws ResourceNotFoundException
     *             if table not found
     * @throws IkatsDaoException
     *             if hibernate exception raised while storing table in db
     * @throws IkatsException
     *             others unexpected exceptions
     */
    @GET
    @Path("/{tableName}")
    public Response downloadTable(@PathParam("tableName") String tableName) throws ResourceNotFoundException, IkatsException, IkatsDaoException, IkatsJsonException {
        // get id of table in processData db
        // assuming there is only one table by tableName
        TableInfo table = tableManager.readFromDatabase(tableName);
        try {

            String jsonString = tableManager.serializeToJson(table);

            return Response.ok(jsonString, MediaType.APPLICATION_JSON_TYPE).build();
        }
        catch (Exception e) {
            throw new IkatsException("Failed: service downloadTable() " + tableName + " : caught unexpected Throwable:", e);
        }

    }

    /**
     * Database (processData table) import of a csv table
     *
     * @param tableName
     *            name of the table
     * @param fileis
     *            the file input stream
     * @param fileDisposition
     *            information about the Multipart with file
     * @param rowName
     *            table row name for unique id
     * @param formData
     *            the form data
     * @param uriInfo
     *            all info on URI
     * @return the internal id
     * @throws IOException           error when parsing input csv file
     * @throws IkatsDaoException     error while accessing database to check if table already
     *                               exists
     * @throws IkatsJsonException    error when mapping imported table to business object Table
     * @throws InvalidValueException if table name does not match expected pattern
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @SuppressWarnings("unchecked")
    public Response importTableFromCSV(@FormDataParam("tableName") String tableName,
                                       @FormDataParam("file") InputStream fileis,
                                       @FormDataParam("file") FormDataContentDisposition fileDisposition,
                                       @FormDataParam("rowName") String rowName, FormDataMultiPart formData,
                                       @Context UriInfo uriInfo) throws IkatsException, IOException, IkatsDaoException, InvalidValueException {

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
            List<List<String>> cells = new ArrayList<>();

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
            Table outputTable = tableManager.initEmptyTable(true, true);

            // parse csv content to :
            // 1. retrieve data to build json table structure to store
            // 2. fix duplicates : if occurs, we stop at first duplicate and
            // send back 409 http code
            while ((line = reader.readLine()) != null) {

                // skip empty lines
                if (line.isEmpty()) {
                    continue;
                }

                String[] items = line.split(separator, -1);
                // check table content consistency
                if (items.length != columnHeaders.size()) {
                    String context = "Line length does not fit headers size in csv file : " + fileName;
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
            String rid = tableManager.createInDatabase(tableName, outputTable);
            chrono.stop(logger);

            // result id is returned in the body
            // Note: with DAO Table: should be changed to return the name of Table
            return Response.status(Response.Status.OK).entity(rid).build();

        } catch (IOException e) {
            String contextError = "Unexpected interruption while parsing CSV file : " + fileName;
            throw new IOException(contextError, e);
        }

    }

    /**
     * Extends the table defined by the tableJson parameter, by adding one
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
     * @param tableJson
     *            the raw String representing the JSON plain content
     * @param metrics
     *            selected metrics separated by ";". Spaces are ignored.
     * @param dataset
     *            the dataset name.
     * @param joinColName
     *            the name of the table column used by the join. Optional: if
     *            undefined (""), the first column will be used by the join.
     * @param joinMetaName
     *            defines the name of metadata used by the join, useful when the
     *            column and metadata names are different. Optional default is
     *            undefined (""): if joinMetaName is undefined, then the
     *            metadata has the name of the table column used by the join
     *            (see joinColName), and if both criteria (joinColName +
     *            joinMetaName) are undefined: it is assumed that the first
     *            column header provides the expected metadata name.
     * @param targetColName
     *            name of the target column. Optional: default is undefined
     *            (""). When target name is defined, the joined columns are
     *            inserted before the target column; when undefined, the joined
     *            columns are appended at the end.
     * @param outputTableName
     *            name of the table joined by metric, and created in the
     *            database. The name ought to be conformed to the pattern:
     *            {@link TableManager#TABLE_NAME_PATTERN}
     * @return
     * @throws IkatsDaoException
     *             a database access error occured during the service.
     * @throws InvalidValueException
     *             error raised if one of the inputs is invalid.
     * @throws ResourceNotFoundException
     *             error raised if one of the resource required by computing is
     *             not found
     * @throws IkatsException
     *             unexpected error occured on the server.
     */
    @POST
    @SuppressWarnings("unchecked")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/join/metrics")
    public Response joinByMetrics(@FormDataParam("tableJson") String tableJson, @FormDataParam("metrics") String metrics,
            @FormDataParam("dataset") String dataset, @FormDataParam("joinColName") @DefaultValue("") String joinColName,
            @FormDataParam("joinMetaName") @DefaultValue("") String joinMetaName,
            @FormDataParam("targetColName") @DefaultValue("") String targetColName, @FormDataParam("outputTableName") String outputTableName)
            throws IkatsDaoException, InvalidValueException, ResourceNotFoundException, IkatsException {

        // delegates the work to the operator JoinTableWithTs
        JoinTableWithTs opeJoinTableWithTs = new JoinTableWithTs();
        String rid = opeJoinTableWithTs.apply(tableJson, metrics, dataset, joinColName, joinMetaName, targetColName, outputTableName);

        // Nominal case: result id is returned in the body
        // Note: with DAO Table: should be changed to return the name of Table
        return Response.status(Response.Status.OK).entity(rid).build();

    }

    /**
     * Table process to :
     * - change table key from metadata (populationId)
     * - add metadata (metaName) value to original column headers  : metaName_colHeader
     * <p>
     * Input table first column must be time series functional identifiers
     *
     * @param tableJson       the table to convert (json)
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
    @SuppressWarnings("unchecked")
    public Response ts2Feature(@FormDataParam("tableJson") String tableJson,
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

        // convert tableJson to table
        TableInfo tableInfo = tableManager.loadFromJson(tableJson);
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
        Table outputTable = tableManager.initEmptyTable(true, true);
        outputTable.getColumnsHeader().addItem(null);
        for (String metaTs : listMetaTs) {
            for (int i = 1; i < colHeaders.size(); i++) {
                outputTable.getColumnsHeader().addItem(metaTs + "_" + colHeaders.get(i));
            }
        }
        int tableContentWidth = outputTable.getColumnsHeader().getItems().size() - 1;

        // filling rows headers and content by popId
        outputTable.getRowsHeader().addItem(populationId);
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
        String rid = tableManager.createInDatabase(outputTableName, outputTable);

        chrono.stop(logger);

        // result id is returned in the body
        // Note: with DAO Table: should be changed to return the name of Table
        return Response.status(Response.Status.OK).entity(rid).build();
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
     * @throws IkatsJsonException error parsing the json content from the database
     * @throws IkatsDaoException database access error
     * @throws ResourceNotFoundException resource not found in the database, for specified name
     */
    @GET
    @Path("/json/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    @SuppressWarnings("unchecked")
    public TableInfo readTable(@PathParam("name") String name) throws IkatsJsonException, IkatsDaoException, ResourceNotFoundException
    {
    	TableManager tableMgt = new TableManager();
    	return tableMgt.readFromDatabase( name);
    }
    /**
     * Table process to :
     * - change table key from metadata (populationId)
     * - add metadata (metaName) value to original column headers  : metaName_colHeader
     * <p>
     * Input table first column must be time series functional identifiers
     *
     * @param tableJson the table to convert (json)
     * @param formData  the form data
     * @param uriInfo   all info on URI
     *
     * @return the internal id
     *
     * @throws IOException               error when parsing input csv file
     * @throws IkatsDaoException         error while accessing database to check if table already exists
     * @throws InvalidValueException     if table name does not match expected pattern
     * @throws IkatsException            row from input table is undefined
     * @throws ResourceNotFoundException row from input table is not found
     */
    @POST
    @Path("/traintestsplit")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response trainTestSplit(@FormDataParam("tableJson") String tableJson,
                                   @FormDataParam("targetColumnName") @DefaultValue("") String targetColumnName,
                                   @FormDataParam("repartitionRate") @DefaultValue("0.5") double repartitionRate,
                                   @FormDataParam("outputTableName") String outputTableName,
                                   FormDataMultiPart formData,
                                   @Context UriInfo uriInfo) throws IOException, IkatsDaoException, IkatsException, InvalidValueException, ResourceNotFoundException {


        Chronometer chrono = new Chronometer(uriInfo.getPath(), true);

        // Convert tableJson to table
        TableInfo tableInfo = tableManager.loadFromJson(tableJson);
        Table table = tableManager.initTable(tableInfo, false);

        List<Table> tabListResult;
        if (targetColumnName.equals("")) {
            tabListResult = tableManager.randomSplitTable(table, repartitionRate);
        }
        else {
            tabListResult = tableManager.trainTestSplitTable(table, targetColumnName, repartitionRate);
        }

        // Store tables in database
        String rid1 = tableManager.createInDatabase(outputTableName + "_Train", tabListResult.get(0));
        String rid2 = tableManager.createInDatabase(outputTableName + "_Test", tabListResult.get(1));

        chrono.stop(logger);

        // Result id is returned in the body
        return Response.status(Response.Status.OK).entity(rid1 + "," + rid2).build();
    }

}
