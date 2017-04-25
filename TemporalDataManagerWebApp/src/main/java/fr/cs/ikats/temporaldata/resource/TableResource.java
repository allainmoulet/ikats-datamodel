package fr.cs.ikats.temporaldata.resource;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.FormDataParam;

import fr.cs.ikats.process.data.model.ProcessData;
import fr.cs.ikats.temporaldata.business.ProcessDataManager;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.utils.Chronometer;

/**
 * resource for Table
 *
 */
@Path("table")
public class TableResource extends AbstractResource {

    private static Logger logger = Logger.getLogger(TableResource.class);

    /**
     * ProcessManager
     */
    protected ProcessDataManager processDataManager;

    /**
     * init the processDataManager
     */
    public TableResource() {
        processDataManager = new ProcessDataManager();
    }

    /**
     * get the processData for tableName identifier.
     * 
     * @param tableName
     *            the name of the table to retrieve
     * @return a processData conatining a table
     * @throws ResourceNotFoundException
     *             if nothing is found.
     */
    @GET
    @Path("/{tableName}")
    @Produces(MediaType.APPLICATION_JSON)
    public ProcessData getTable(@PathParam("tableName") String tableName) throws ResourceNotFoundException {
        List<ProcessData> result = processDataManager.getProcessData(tableName);
        if (result == null) {
            throw new ResourceNotFoundException("No Table found for tableName " + tableName);
        }
        return result.get(0);

    }

    /**
     * lance l'import d'une table de type fichier csv.
     * 
     * @param tableName
     *            name of the table
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
    @Path("/{tableName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public String importProcessResult(@PathParam("tableName") String tableName, @FormDataParam("file") InputStream fileis,
            @FormDataParam("file") FormDataContentDisposition fileDisposition, FormDataMultiPart formData, @Context UriInfo uriInfo) {
        Chronometer chrono = new Chronometer(uriInfo.getPath(), true);
        String fileName = fileDisposition.getFileName();
        logger.info("Import result file : " + fileName);
        logger.info("Table Name : " + tableName);
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
        String id = processDataManager.importProcessData(fileis, fileSize, tableName, fileType, fileName);
        return id;
    }


}
