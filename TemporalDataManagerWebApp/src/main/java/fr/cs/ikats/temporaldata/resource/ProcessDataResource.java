package fr.cs.ikats.temporaldata.resource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.process.data.model.ProcessData;
import fr.cs.ikats.temporaldata.business.ProcessDataManager;
import fr.cs.ikats.temporaldata.business.ProcessDataManager.ProcessResultTypeEnum;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.utils.Chronometer;

/**
 * resource for ProcessData
 * 
 * TODO [#143428] Correction sur la gestion des exceptions sur le gest. de
 * données.
 * 
 *    ICI: refactoring exceptions: simplifier avec IkatsDaoException => revoir les 
 *    declaratons throws et les handlers sous-jacents afin 
 *    - d'unifier sous IkatsDaoException
 *    - et reduire le nombre de classes (redondances de code)
 *
 * 
 */
@Path("processdata")
public class ProcessDataResource extends AbstractResource {

    private static Logger logger = Logger.getLogger(ProcessDataResource.class);

    /**
     * ProcessManager
     */
    protected ProcessDataManager processDataManager;

    /**
     * init the processDataManager
     */
    public ProcessDataResource() {
        processDataManager = new ProcessDataManager();
    }

    /**
     * get the processData for processid exec identifier.
     * 
     * @param processId
     *            the process id
     * @return a list of processData
     * @throws ResourceNotFoundException
     *             if nothing is found.
     */
    @GET
    @Path("/{processId}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<ProcessData> getProcessData(@PathParam("processId") String processId) throws ResourceNotFoundException {
        List<ProcessData> result = processDataManager.getProcessData(processId);
        if (result == null) {
            throw new ResourceNotFoundException("No ProcessResult found for processId " + processId);
        }
        return result;

    }

    /**
     * get the result as an attachement file in the response. media type will
     * depends on the dataType of the result.
     * 
     * @param id
     *            the internal id
     * @return a Response with content-type depending on the processData type
     * @throws ResourceNotFoundException
     *             if result is null
     * @throws SQLException
     *             if Blob cannot be read
     */
    @GET
    @Path("/id/download/{id}")
    public Response downloadProcessData(@PathParam("id") Integer id) throws ResourceNotFoundException, SQLException, IkatsDaoException {

        // Response res = null;
        ProcessData result = processDataManager.getProcessPieceOfData(id);

        if (result == null) {
            throw new ResourceNotFoundException("No ProcessResult found for processId " + id);
        }

        try {

            logger.info(result.toString());

            String fileName = result.getName();
            ResponseBuilder responseBuilder;

            // TODO robustness: result.getFormat() may be null ?
            if (result.getDataType().equals(ProcessResultTypeEnum.JSON.toString())) {
                String jsonString = new String(result.getData().getBytes(1, (int) result.getData().length()));
                logger.info("JSON String written : " + jsonString + " END");
                responseBuilder = Response.ok(jsonString, MediaType.APPLICATION_JSON_TYPE);
            }
            else {

                responseBuilder = Response.ok(getOut(result.getData().getBytes(1, (int) result.getData().length()))).header("Content-Disposition",
                        "attachment;filename=" + fileName);
                // TODO robustness: result.getFormat() may be null ?
                if (result.getDataType().equals(ProcessResultTypeEnum.CSV.toString())) {
                    responseBuilder.header("Content-Type", "application/ms-excel");
                }
            }

            return responseBuilder.build();
        }
        catch (SQLException sqle) {
            logger.error("Failed: service downloadProcessData(): caught SQLException:", sqle);
            throw sqle;
        }
        catch (Throwable e) {
            IkatsDaoException ierror = new IkatsDaoException("Failed: service downloadProcessData(): caught unexpected Throwable:", e);
            logger.error(ierror);
            throw ierror;
        }

    }

    /**
     * get the processData for processid exec identifier. the response is a
     * multipart response containing two parts : 1 containing a JSON
     * representation of the data. 2 containing a OutputStream to download the
     * data.
     * 
     * @param id
     *            the internal id
     * @return a Response with mutlipart
     * @throws ResourceNotFoundException
     *             if nothing is found.
     * @throws IkatsException
     *             when error occured
     * @throws IOException
     *             when error occured
     * @throws SQLException
     *             when error occured
     */
    @GET
    @Path("/id/{id}")
    @Produces(MediaType.MULTIPART_FORM_DATA)
    public Response getProcessData(@PathParam("id") Integer id) throws ResourceNotFoundException, IkatsException, IOException, SQLException {

        // TODO : service a supprimer ??? PAS UTILISE

        ProcessData result = processDataManager.getProcessPieceOfData(id);

        if (result == null) {
            throw new ResourceNotFoundException("No ProcessResult found for processId " + id);
        }

        final FormDataMultiPart multipart = new FormDataMultiPart();

        try {

            multipart.bodyPart(new BodyPart(result, MediaType.APPLICATION_JSON_TYPE));

            if (ProcessResultTypeEnum.valueOf(result.getDataType()).equals(ProcessResultTypeEnum.JSON)) {
                String jsonString = new String(result.getData().getBytes(1, (int) result.getData().length()));
                multipart.bodyPart(new BodyPart(jsonString, MediaType.APPLICATION_JSON_TYPE));
            }
            else if (ProcessResultTypeEnum.valueOf(result.getDataType()).equals(ProcessResultTypeEnum.CSV)) {
                multipart.bodyPart(new StreamDataBodyPart("myFile.csv", result.getData().getBinaryStream()));
            }
        }
        catch (SQLException sqle) {
            logger.error("Failed: service getProcessData(Integer): caught SQLException:", sqle);
            try {
                multipart.close();
            }
            catch (Throwable e) { // no more closing attempt
            }

            throw sqle;
        }
        catch (Throwable e) {
            IkatsException ierror = new IkatsException("Failed: service getProcessData(Integer): caught unexpected Throwable:", e);
            logger.error(ierror);

            try {
                multipart.close();
            }
            catch (Throwable xe) { // no more closing attempt
            }

            throw ierror;
        }

        return Response.ok(multipart).build();
    }

    class FileStreamingOutput implements StreamingOutput {

        InputStream is;

        public FileStreamingOutput(InputStream inS) {
            is = inS;
        }

        @Override
        public void write(OutputStream output) throws IOException, WebApplicationException {
            output.write(is.read());
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

    /**
     * @param processId
     *            the process id to delete
     */
    @DELETE
    @Path("/{processId}")
    public void deleteProcessData(@PathParam("processId") String processId) {
        processDataManager.removeProcessData(processId);
    }

    /**
     * lance l'import d'un resultat de type fichier.
     * 
     * @param processId
     *            the processId
     * @param fileis
     *            the file input stream
     * @param fileDisposition
     *            information about the Multipart with file
     * @param formData
     *            the form data
     * @param uriInfo
     *            all info on URI
     * @return the internal id
     */
    @POST
    @Path("/{processId}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public String importProcessResult(@PathParam("processId") String processId, @FormDataParam("file") InputStream fileis,
            @FormDataParam("file") FormDataContentDisposition fileDisposition, FormDataMultiPart formData, @Context UriInfo uriInfo) {
        Chronometer chrono = new Chronometer(uriInfo.getPath(), true);
        String fileName = fileDisposition.getFileName();
        logger.info("Import result file : " + fileName);
        logger.info("processId : " + processId);
        Map<String, List<FormDataBodyPart>> params = formData.getFields();
        Long fileSize = fileDisposition.getSize();

        // default value to file
        String fileType = "file";
        for (String key : params.keySet()) {
            if (key.equals("fileType")) {
                fileType = params.get(key).get(0).getValue();
            }
            else if (key.equals("fileSize")) {
                fileSize = new Long(Integer.parseInt((params.get(key).get(0).getValue())));
            }
            else if (key.equals("file")) {
                // do nothing it is the file inputStream, not a tag.
            }
        }
        chrono.stop(logger);
        String id = processDataManager.importProcessData(fileis, fileSize, processId, fileType, fileName);
        return id;
    }

    /**
     * lance l'import d'un resultat de type fichier.
     * 
     * @param processId the processId
     * @param name the name of the processResult
     * @param json the contained json
     * @param sizeParam the size of the json string
     * @param uriInfo all info on URI
     * @return the internal id
     */
    @POST
    @Path("/{processId}/JSON")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_JSON)
    public String importProcessResultAsJson(@PathParam("processId") String processId, @FormParam("name") String name, @FormParam("json") String json, @FormParam("size") String sizeParam,@Context UriInfo uriInfo) {
      
        // TODO EVOL REST: simplifier: se passer de sizeParam ? US/FT a creer
        //    => consumes JSON + param name: passe par URL @PathParam ...
        Chronometer chrono = new Chronometer(uriInfo.getPath(), true);
        logger.info("processId : " + processId);        
        Long size =Long.parseLong(sizeParam);
        String type = "JSON";
        InputStream is = new ByteArrayInputStream(json.getBytes());
        String id = processDataManager.importProcessData(is, size, processId, type,name);
        chrono.stop(logger);
        return id;
    }

}
