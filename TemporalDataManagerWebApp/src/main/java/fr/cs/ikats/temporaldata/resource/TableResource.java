package fr.cs.ikats.temporaldata.resource;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import fr.cs.ikats.metadata.MetaDataFacade;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.temporaldata.business.TableInfo;
import fr.cs.ikats.temporaldata.business.TableManager;
import fr.cs.ikats.temporaldata.business.TableManager.Table;
import fr.cs.ikats.temporaldata.exception.IkatsJsonException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.FormDataParam;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.utils.Chronometer;


/**
 * resource for Table
 */
@Path("table")
public class TableResource extends AbstractResource {

    private static Logger logger = Logger.getLogger(TableResource.class);

    /**
     * TableManager
     */
    private TableManager tableManager;

    /**
     * init the tableManager
     */
    public TableResource() {
        tableManager = new TableManager();
    }


    /**
     * get the JSON result as an attachement file in the response.
     *
     * @param tableName the name of the table to retrieve
     * @return a Response with content-type json
     * @throws ResourceNotFoundException if table not found
     * @throws IkatsDaoException         if hibernate exception raised while storing table in db
     * @throws IkatsException            others unexpected exceptions
     * @throws IkatsJsonException        error when mapping downloaded table to business object Table
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

        } catch (Exception e) {
            throw new IkatsException("Failed: service downloadTable() " +
                    tableName + " : caught unexpected Exception:", e);
        }

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

            // init output table
            Table outputTable = tableManager.initEmptyTable();

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
            outputTable.getColumnsHeader().getData().addAll(columnHeaders);
            outputTable.getRowsHeader().getData().addAll(rowHeaders);

            // store Table
            String rid = tableManager.createInDatabase(tableName, outputTable.getTableInfo());
            chrono.stop(logger);

            // result id is returned in the body
            return Response.status(Response.Status.OK).entity(rid).build();

        } catch (IOException e) {
            String contextError = "Unexpected interruption while parsing CSV file : " + fileName;
            throw new IOException(contextError, e);
        }

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
        List<Object> colHeaders = table.getColumnsHeader().getData();
        List<Object> rowHeaders = table.getRowsHeader().getData();

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

        // filling outputTable
        // filling col headers
        // first element is null
        Table outputTable = tableManager.initEmptyTable();
        outputTable.getColumnsHeader().addItem(null);
        for (String metaTs : listMetaTs) {
            for (int i = 1; i < colHeaders.size(); i++) {
                outputTable.getColumnsHeader().addItem(metaTs + "_" + colHeaders.get(i));
            }
        }
        int tableContentWidth = outputTable.getColumnsHeader().getData().size() - 1;

        // filling rows headers and content by popId
        outputTable.getRowsHeader().addItem(populationId);
        for (String popId : listPopId) {
            outputTable.getRowsHeader().addItem(popId);
            List<Integer> listIndexPopId = retrieveIndexesListOfEltInList(popId, colPopId);

            List<Object> cellsLine = new ArrayList<>();
            for (String metaTS : listMetaTs) {
                for (Integer index : listIndexPopId) {
                    if (metaTS.equals(colMetaTs.get(index))) {
                        cellsLine.addAll(table.getRow(index + 1));
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
        String rid = tableManager.createInDatabase(outputTableName, outputTable.getTableInfo());

        chrono.stop(logger);

        // result id is returned in the body
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

}
