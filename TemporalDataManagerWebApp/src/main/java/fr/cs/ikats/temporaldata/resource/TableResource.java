package fr.cs.ikats.temporaldata.resource;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
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

    // Review:MBD:156259 corrections sur autour du throw(s) Throwable a eviter
    // 1/ warning sonar déconseillant de catcher un Throwable, un peu trop
    // général ( en fin de body)
    // => il vaut mieux catcher/lever des Exception ...
    // 2/ ... on peut amemiorer le code en gerant le throw de IkatsException qui
    // etend Exception (c'est mieux)
    // (note si tu as sonar: 3/ il reste un warning sonar bizarre mais cela
    // reste acceptable ... vu dans des blogs)
    // (cf
    // http://howtodoinjava.com/best-practices/java-exception-handling-best-practices/)
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
    public Response downloadTable(@PathParam("tableName") String tableName) throws ResourceNotFoundException, IkatsDaoException, IkatsException {
        // get id of table in processData db
        // assuming there is only one table by tableName

        List<ProcessData> dataTables = processDataManager.getProcessData(tableName);

        if (dataTables == null) {
            throw new IkatsDaoException("DAO exception while attempting to store table : " + tableName);
        }
        else if (dataTables.isEmpty()) {
            throw new ResourceNotFoundException("No result found for tableName " + tableName);
        }

        try {
            ProcessData dataTable = dataTables.get(0);

            logger.info(dataTable.toString());
            ResponseBuilder responseBuilder;

            String jsonString = new String(dataTable.getData().getBytes(1, (int) dataTable.getData().length()));
            // Review:MBD:156259 info trop verbeux sur le contenu BLOB (qui peut
            // etre gros...)
            // => mettre de preference au niveau debug activable en maintenance
            //
            // Review:MBD:156259 changé 'written' -> 'read'
            logger.debug("JSON String read : " + jsonString + " END");

            responseBuilder = Response.ok(jsonString, MediaType.APPLICATION_JSON_TYPE);

            return responseBuilder.build();

        }
        // Review:MBD:156259 correction proposee:
        // 1/ déconseillé de catcher un Throwable, un peu trop général
        // => il vaut mieux catcher/lever des Exception
        // 2/ on peut amémiorer le code en gerant le throw de IkatsException:
        // mieux que Exception
        catch (Exception e) {
            throw new IkatsException("Failed: service downloadTable() " +
                                     tableName + " : caught unexpected Throwable:", e);
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
     * @throws IOException
     *             error when parsing input csv file
     * @throws IkatsDaoException
     *             error while accessing database to check if table already
     *             exists
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @SuppressWarnings("unchecked")
    public Response importTable(@FormDataParam("tableName") String tableName, @FormDataParam("file") InputStream fileis,
            @FormDataParam("file") FormDataContentDisposition fileDisposition, @FormDataParam("rowName") String rowName, FormDataMultiPart formData,
            @Context UriInfo uriInfo) throws IOException, IkatsDaoException {

        // Review:MBD:156259 pour simplifier le code: un seul bloc try ... catch
        // ...
        // encadrant tout: c'est plus facile pour assurer une robustesse
        // - rajouter des catch pour les erreurs anticipées (voir remarques plus
        // bas)
        // - quitte a rajouter IkatsException pour les erreurs inattendues mais
        // pour lesquelles on log le contexte
        //

        Chronometer chrono = new Chronometer(uriInfo.getPath(), true);
        String fileName = fileDisposition.getFileName();
        logger.info("Import csv file : " + fileName);
        logger.info("Table Name : " + tableName);
        Map<String, List<FormDataBodyPart>> params = formData.getFields();
        Long fileSize = fileDisposition.getSize();

        // default value to file
        // Review:MBD:156259: fileType n'est pas utile => du coup ce code peut
        // etre bien plus simple
        // Review:MBD:156259: sans for ... et sans switch ... just tester que
        // params.get("fileSize")
        // Review:MBD:156259: est non null, et non vide ...
        // Review:MBD:156259: avant de remodifier fileSize avec
        // params.get("fileSize").get(0).getValue()
        String fileType = "file";
        for (String key : params.keySet()) {

            switch (key) {
            case "fileType":
                fileType = params.get(key).get(0).getValue();
                break;
            case "fileSize":
                // Review:MBD:156259: catcher NumberFormatException pour avoir
                // au moins un log avec le context
                // (conseil: a faire avec remarque initiale en debut de methode)
                fileSize = Long.parseLong((params.get(key).get(0).getValue()));
            }
        }
        // Review:MBD:156259:exemple simplification:
        // if ( (params.get("fileSize") != null) &&
        // (params.get("fileSize").size() > 0)){
        // fileSize =
        // Long.parseLong((params.get("fileSize").get(0).getValue()));
        // }

        BufferedReader reader = new BufferedReader(new InputStreamReader(fileis));
        String separator = ",";
        Integer rowIndexId = -1;
        List<String> columnHeaders = new ArrayList<String>();
        List<String> rowHeaders = new ArrayList<String>();
        // Review:MBD:156259
        // dans ton cas: le JSON aura headers.row.data[0] === null
        // pour que le coin ne soit defini QUE par headers.col.data[0]
        // c'est pour permettre plus tard de gerer d'autres tableaux ...
        // il manque juste ici:
        // rowHeaders.add(null)
        List<List<String>> cells = new ArrayList<List<String>>();
        try {

            // consume header to retrieve column index of unique identifier in
            // the table
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

            // parse csv content to :
            // 1. retrieving data to build json table structure to store
            // 2. fix duplicates : if occurs, we stop at first duplicate and
            // send back 409 http code
            String line = reader.readLine();
            while (line != null) {

                // Review:MBD:156259 ce serait bien de verifier que la taille de
                // columnHeaders vale
                // Review:MBD:156259 celle de items du while ... sinon il manque
                // un separateur ...
                // Review:MBD:156259 a discuter aussi : et si une ligne est
                // Review:MBD:156259 "1,,,,33,a" => comment sont gerees les
                // valeurs vides par le split ? l'ideal serait "" ou null

                String[] items = line.split(separator);

                // retrieving table data
                rowHeaders.add(items[0]);
                List<String> cells_content = new ArrayList<String>();
                for (int i = 1; i < items.length; i++) {
                    cells_content.add(items[i]);
                }
                cells.add(cells_content);

                // seeking for duplicates
                String idRef = items[rowIndexId];
                if (!keyTableMap.containsKey(idRef)) {
                    keyTableMap.put(idRef, "NA");
                }
                else {
                    String context = "Duplicate found in csv file: " + rowName + " = " + idRef;
                    logger.error(context);
                    return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
                }
                line = reader.readLine();
            }
        }
        catch (IOException e) {
            // Review:MBD:156259 rajouter le fileName
            String contextError = "Unexpected interruption while parsing CSV file";
            throw new IOException(contextError, e);
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
            json.put("content", cells);

            InputStream is = new ByteArrayInputStream(json.toString().getBytes());
            String rid = processDataManager.importProcessData(is, fileSize, tableName, "JSON", fileName);
            logger.info("Table stored Ok in db : " + tableName);
            chrono.stop(logger);

            // result id is returned in the body
            return Response.status(Response.Status.OK).entity(rid).build();
        }
        else {
            String context = "Table name already exists : " + tableName;
            logger.error(context);
            chrono.stop(logger);
            return Response.status(Response.Status.CONFLICT).entity(context).build();
        }

    }

    /**
     * return a new Instance of OutputStreaming, used for streaming out the csv
     * file
     *
     * @param bs
     *            the bytes to write
     * @return
     */

    private StreamingOutput getOut(final byte[] bs) {
        return new StreamingOutput() {
            @Override
            public void write(OutputStream out) throws IOException, WebApplicationException {
                out.write(bs);
            }
        };
    }

}
