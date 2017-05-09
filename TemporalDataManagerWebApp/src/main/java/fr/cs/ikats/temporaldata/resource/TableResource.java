package fr.cs.ikats.temporaldata.resource;

import java.io.*;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
     * init the processDataManager
     */
    public TableResource() {
        processDataManager = new ProcessDataManager();
    }

    private boolean validatetableName(String tableName) {
        Matcher matcher = TableResource.TABLE_NAME_PATTERN.matcher(tableName);
        return matcher.matches();
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
    public Response downloadTable(@PathParam("tableName") String tableName) throws ResourceNotFoundException, IkatsDaoException, IkatsException {
        // get id of table in processData db
        // assuming there is only one table by tableName

        List<ProcessData> dataTables = processDataManager.getProcessData(tableName);

        if (dataTables == null) {
            throw new IkatsDaoException("DAO exception while attempting to store table : " + tableName);
        } else if (dataTables.isEmpty()) {
            throw new ResourceNotFoundException("No result found for tableName " + tableName);
        }

        try {
            ProcessData dataTable = dataTables.get(0);

            logger.info(dataTable.toString());
            ResponseBuilder responseBuilder;

            String jsonString = new String(dataTable.getData().getBytes(1, (int) dataTable.getData().length()));
            logger.debug("JSON String read : " + jsonString + " END");

            responseBuilder = Response.ok(jsonString, MediaType.APPLICATION_JSON_TYPE);

            return responseBuilder.build();

        } catch (Exception e) {
            throw new IkatsException("Failed: service downloadTable() " +
                    tableName + " : caught unexpected Throwable:", e);
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
    public Response importTable(@FormDataParam("tableName") String tableName, @FormDataParam("file") InputStream fileis,
                                @FormDataParam("file") FormDataContentDisposition fileDisposition, @FormDataParam("rowName") String rowName, FormDataMultiPart formData,
                                @Context UriInfo uriInfo) throws IOException, IkatsDaoException, InvalidValueException {

        String fileName = "";
        try {
            if (!validatetableName(tableName)) {
                String context = "empty parameter 'tableName' provided";
                logger.error(context);
                throw new InvalidValueException("String", "tableName", TableResource.TABLE_NAME_PATTERN.pattern(), tableName, null);
            }
            Chronometer chrono = new Chronometer(uriInfo.getPath(), true);
            fileName = fileDisposition.getFileName();
            logger.info("Import csv file : " + fileName);
            logger.info("Table Name : " + tableName);
            Long fileSize = fileDisposition.getSize();

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
            // 1. retrieving data to build json table structure to store
            // 2. fix duplicates : if occurs, we stop at first duplicate and
            // send back 409 http code
            while ((line = reader.readLine()) != null) {

                // skip empty lines
                if (line.isEmpty()) {
                    continue;
                }
                // Review:MBD:156259 ce serait bien de verifier que la taille de
                // columnHeaders vale
                // Review:MBD:156259 celle de items du while ... sinon il manque
                // un separateur ...

                String[] items = line.split(separator, -1);
                if (items.length != columnHeaders.size()){
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
                // filling json structure
                JSONObject json = new JSONObject();

                // fill table description
                JSONObject table_desc = new JSONObject();
                table_desc.put("title", tableName);
                table_desc.put("desc", fileName);
                json.put("table_desc", table_desc);

                // fill headers
                JSONObject headers = new JSONObject();
                JSONObject col = new JSONObject();
                col.put("data", columnHeaders);
                JSONObject row = new JSONObject();
                row.put("data", rowHeaders);
                headers.put("col", col);
                headers.put("row", row);
                json.put("headers", headers);

                // fill content
                JSONObject content = new JSONObject();
                content.put("cells", cells);
                json.put("content", content);

                InputStream is = new ByteArrayInputStream(json.toString().getBytes());
                String rid = processDataManager.importProcessData(is, fileSize, tableName, "JSON", fileName);
                logger.info("Table stored Ok in db : " + tableName);
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

}
