package fr.cs.ikats.temporaldata.resource;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.cs.ikats.metadata.MetaDataFacade;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.temporaldata.business.Table;
import fr.cs.ikats.temporaldata.business.TableManager;
import fr.cs.ikats.temporaldata.exception.IkatsJsonException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.FormDataParam;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.process.data.model.ProcessData;
import fr.cs.ikats.temporaldata.business.ProcessDataManager;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.utils.Chronometer;
import org.json.simple.JSONObject;


/**
 * resource for Table
 */
@Path("table")
public class TableResource extends AbstractResource {

    private static Logger logger = Logger.getLogger(TableResource.class);
    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9_-]+");

    /**
     * ProcessManager
     */
    private ProcessDataManager processDataManager;

    /**
     * ProcessManager
     */
    private TableManager tableManager;

    /**
     * init the processDataManager and the tableManager
     */
    public TableResource() {
        processDataManager = new ProcessDataManager();
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
     */
    @GET
    @Path("/{tableName}")
    public Response downloadTable(@PathParam("tableName") String tableName) throws IkatsException, ResourceNotFoundException, SQLException, IkatsDaoException {
        // get id of table in processData db
        // assuming there is only one table by tableName

        Table table = getTableFromProcessData(tableName);

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
     * @throws IOException       error when parsing input csv file
     * @throws IkatsDaoException error while accessing database to check if table already
     *                           exists
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @SuppressWarnings("unchecked")
    public Response importTable(@FormDataParam("tableName") String tableName,
                                @FormDataParam("file") InputStream fileis,
                                @FormDataParam("file") FormDataContentDisposition fileDisposition,
                                @FormDataParam("rowName") String rowName, FormDataMultiPart formData,
                                @Context UriInfo uriInfo) throws IOException, IkatsDaoException, InvalidValueException, IkatsJsonException {

        Chronometer chrono = new Chronometer(uriInfo.getPath(), true);
        String fileName = fileDisposition.getFileName();
        try {
            if (!validaTetableName(tableName)) {
                String context = "empty parameter 'tableName' provided";
                logger.error(context);
                throw new InvalidValueException("String", "tableName", TableResource.TABLE_NAME_PATTERN.pattern(), tableName, null);
            }
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
                if (items.length != columnHeaders.size()) {
                    String context = "Line length does not fit headers size in csv file : " + fileName;
                    logger.error(context);
                    return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
                }

                // retrieving table data
                rowHeaders.add(items[0]);
                List<String> cells_content = new ArrayList<>();
                for (int i = 1; i < items.length; i++) {
                    cells_content.add(items[i]);
                }
                cells.add(cells_content);

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

            // check that tableName does not already exist
            List<ProcessData> dataTables = processDataManager.getProcessData(tableName);
            if (dataTables == null) {
                throw new IkatsDaoException("DAO exception while searching table : " + tableName);
            }
            if (dataTables.isEmpty()) {
                Table outputTable = tableManager.initTableStructure();

                // fill table description
                outputTable.table_desc.title = tableName;
                outputTable.table_desc.desc = fileName;

                // fill headers
                outputTable.headers.col.data = convertToListOfObject(columnHeaders);
                outputTable.headers.row.data = convertToListOfObject(rowHeaders);

                // fill content
                outputTable.content.cells = convertToListOfListOfObject(cells);

                // store Table
                String rid = storeTableinProcessData(tableName, outputTable);
                chrono.stop(logger);

                // result id is returned in the body
                return Response.status(Response.Status.OK).entity(rid).build();
            } else {
                String context = "Table name already exists : " + tableName;
                logger.error(context);
                chrono.stop(logger);
                return Response.status(Response.Status.CONFLICT).entity(context).build();
            }
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
     * @param tableName       name of the table to convert
     * @param metaName        name of metadata to concat with agregates ref
     * @param populationId    id of population (which is in fact a metadata name) = key of output table
     * @param outputTableName name of the table generated
     * @param formData        the form data
     * @param uriInfo         all info on URI
     * @return the internal id
     * @throws IOException               error when parsing input csv file
     * @throws IkatsDaoException         error while accessing database to check if table already exists
     * @throws ResourceNotFoundException if table not found
     */
    @POST
    @Path("/changekey")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @SuppressWarnings("unchecked")
    public Response changeKey(@FormDataParam("tableName") String tableName,
                              @FormDataParam("metaName") String metaName,
                              @FormDataParam("populationId") String populationId,
                              @FormDataParam("outputTableName") String outputTableName,
                              FormDataMultiPart formData,
                              @Context UriInfo uriInfo) throws IOException, IkatsDaoException, InvalidValueException, SQLException, IkatsJsonException, ResourceNotFoundException {


        Chronometer chrono = new Chronometer(uriInfo.getPath(), true);

        if (outputTableName == null) {
            String context = "Output table name shall not be null";
            return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
        }

        if (populationId == null) {
            String context = "populationId shall not be null";
            return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
        }

        // check that outputTableName does not already exist
        List<ProcessData> dataTables = processDataManager.getProcessData(outputTableName);
        if (dataTables == null) {
            throw new IkatsDaoException("DAO exception while searching table : " + outputTableName);
        }
        if (!dataTables.isEmpty()) {
            String context = "Output table name already exists : choose a different one";
            return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
        }

        logger.info("Working on table (" + tableName + ") " +
                "with metaName (" + metaName + ") " +
                "and with populationId (" + populationId + ")");
        logger.info("Output table name is (" + outputTableName + ")");

        Table outputTable = tableManager.initTableStructure();

        // retrieve input table from process data
        Table table = getTableFromProcessData(tableName);

        // retrieve headers
        List<Object> colHeaders = table.headers.col.data;
        List<Object> rowHeaders = table.headers.row.data;

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

        // processing an ordered list of population ids without doubloons
        Set<String> setPopId = new HashSet<>(colPopId);
        List<String> listPopId = new ArrayList<>(setPopId);
        Collections.sort(listPopId);

        // processing an ordered list of metaName by ts without doubloons
        Set<String> setMetaTs = new HashSet<>(colMetaTs);
        List<String> listMetaTs = new ArrayList<>(setMetaTs);
        Collections.sort(listMetaTs);

        // filling col headers
        // first element is null
        outputTable.headers.col.data.add(null);
        for (String metaTs : listMetaTs) {
            for (int i = 1; i < colHeaders.size(); i++) {
                outputTable.headers.col.data.add(metaTs + "_" + colHeaders.get(i));
            }
        }
        Integer tableContentWidth = outputTable.headers.col.data.size() - 1;

        // filling rows headers and content by popId
        outputTable.headers.row.data.add(populationId);
        for (String popId : listPopId) {
            outputTable.headers.row.data.add(popId);
            List<Integer> listIndexPopId = retrieveIndexesListOfEltInList(popId, colPopId);

            List<Object> cellsLine = new ArrayList<>();
            for (String metaTS : colMetaTs) {
                for (Integer index : listIndexPopId) {
                    if (metaTS == colMetaTs.get(index)) {
                        cellsLine.addAll(table.content.cells.get(index));
                    }
                }
            }
            // check line size
            if (cellsLine.size() != tableContentWidth) {
                String context = "Output table ";
                return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
            }
            outputTable.content.cells.add(cellsLine);
        }
        // store table in db
        String rid = storeTableinProcessData(outputTableName, outputTable);

        chrono.stop(logger);

        // result id is returned in the body
        return Response.status(Response.Status.OK).entity(rid).build();
    }

    /**
     * Store a Table in process data database
     */
    public String storeTableinProcessData(String tableName, Table tableToStore) throws IkatsJsonException {
        byte[] data = tableManager.serializeToJson(tableToStore).getBytes();
        String rid = processDataManager.importProcessData(tableName, tableToStore.table_desc.desc, data);
        logger.info("Table stored Ok in db : " + tableName);

        return rid;
    }

    /**
     * Get a Table from process data database
     */
    public Table getTableFromProcessData(String tableName) throws IkatsJsonException, IkatsDaoException, ResourceNotFoundException, SQLException {

        List<ProcessData> dataTables = processDataManager.getProcessData(tableName);

        if (dataTables == null) {
            throw new IkatsDaoException("DAO exception while attempting to get table : " + tableName);
        } else if (dataTables.isEmpty()) {
            throw new ResourceNotFoundException("No result found for tableName " + tableName);
        }

        ProcessData dataTable = dataTables.get(0);

        // extract data to json string
        String jsonString = new String(dataTable.getData().getBytes(1, (int) dataTable.getData().length()));

        // convert to Table type
        Table table = tableManager.loadFromJson(jsonString);

        logger.info("Table retrieved from db OK : " + tableName);

        return table;
    }

    /**
     * Get a table column from a table
     */
    public <T> List<T> getColumnfromTable(String tableName, String columnName) throws IkatsException, IkatsDaoException, ResourceNotFoundException, SQLException {

        Table table = getTableFromProcessData(tableName);

        List<T> column = tableManager.getColumnfromTable(table, columnName);

        logger.info("Column " + columnName + " retrieved from table : " + tableName);

        return column;
    }

    /**
     * Validate table name according to TABLE_NAME_PATTERN
     */
    private boolean validaTetableName(String tableName) {
        Matcher matcher = TableResource.TABLE_NAME_PATTERN.matcher(tableName);
        return matcher.matches();
    }

    /**
     * Convert list of Object to list of String
     */
    public List<String> convertToListOfString(List<Object> listObject) {
        return listObject.stream()
                .map(object -> Objects.toString(object, null))
                .collect(Collectors.toList());
    }

    /**
     * Convert list of String to list of Object
     */
    private List<Object> convertToListOfObject(List<String> listString) {
        return listString.stream()
                .map(object -> Objects.toString(object, null))
                .collect(Collectors.toList());
    }

    /**
     * Convert list of list of String to list of list of Object
     */
    private List<List<Object>> convertToListOfListOfObject(List<List<String>> listString) {
        List<List<Object>> output = new ArrayList<>();
        for (List<String> list : listString) {
            output.add(new ArrayList<>(list));
        }
        return output;
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
