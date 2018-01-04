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

package fr.cs.ikats.temporaldata.resource;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.process.data.model.ProcessData;
import fr.cs.ikats.temporaldata.business.ProcessDataManager;
import fr.cs.ikats.temporaldata.business.ProcessDataManager.ProcessResultTypeEnum;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.utils.Chronometer;
import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.*;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.ResponseBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * resource for ProcessData
 * <p>
 * TODO [#143428] Correction sur la gestion des exceptions sur le gest. de
 * données.
 * <p>
 * ICI: refactoring exceptions: simplifier avec IkatsDaoException => revoir les
 * declaratons throws et les handlers sous-jacents afin
 * - d'unifier sous IkatsDaoException
 * - et reduire le nombre de classes (redondances de code)
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
     * @param processId the process id
     * @return a list of processData
     * @throws ResourceNotFoundException if nothing is found.
     */
    @GET
    @Path("/{processId}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<ProcessData> getProcessData(@PathParam("processId") String processId) throws IkatsDaoException, ResourceNotFoundException {
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
     * @param id the internal id
     * @return a Response with content-type depending on the processData type
     * @throws ResourceNotFoundException if result is null
     * @throws SQLException              if Blob cannot be read
     */
    @GET
    @Path("/id/download/{id}")
    public Response downloadProcessData(@PathParam("id") Integer id) throws ResourceNotFoundException, SQLException, IkatsDaoException {

        // Response res = null;
        ProcessData result = processDataManager.getProcessPieceOfData(id);

        if (result == null) {
            throw new ResourceNotFoundException("No ProcessResult found for processId " + id);
        }

        logger.info(result.toString());

        String fileName = result.getName();
        ResponseBuilder responseBuilder;

        if (result.getDataType().equals(ProcessResultTypeEnum.ANY.toString())) {
            byte[] bytes = result.getData();
            logger.trace("Body written : " + Arrays.toString(bytes) + " END");
            responseBuilder = Response.ok(bytes, MediaType.APPLICATION_OCTET_STREAM);
        } else if (result.getDataType().equals(ProcessResultTypeEnum.JSON.toString())) {
            String jsonString = new String(result.getData(), Charset.defaultCharset());
            logger.trace("JSON String written : " + jsonString + " END");
            responseBuilder = Response.ok(jsonString, MediaType.APPLICATION_JSON_TYPE);
        } else {
            responseBuilder = Response.ok(getOut(result.getData())).header("Content-Disposition", "attachment;filename=" + fileName);
            if (result.getDataType().equals(ProcessResultTypeEnum.CSV.toString())) {
                responseBuilder.header("Content-Type", "application/ms-excel");
            }
        }

        return responseBuilder.build();

    }

    /**
     * get the processData for processid exec identifier. the response is a
     * multipart response containing two parts : 1 containing a JSON
     * representation of the data. 2 containing a OutputStream to download the
     * data.
     *
     * @param id the internal id
     * @return a Response with mutlipart
     * @throws ResourceNotFoundException if nothing is found.
     * @throws IkatsException            when error occurred
     * @throws IOException               when error occurred
     * @throws SQLException              when error occurred
     */
    @GET
    @Path("/id/{id}")
    @Produces(MediaType.MULTIPART_FORM_DATA)
    public Response getProcessData(@PathParam("id") Integer id) throws IkatsDaoException, ResourceNotFoundException, IkatsException {

        // TODO : service a supprimer ??? PAS UTILISE

        ProcessData result = processDataManager.getProcessPieceOfData(id);

        if (result == null) {
            throw new ResourceNotFoundException("No ProcessResult found for processId " + id);
        }

        final FormDataMultiPart multipart = new FormDataMultiPart();

        multipart.bodyPart(new BodyPart(result, MediaType.APPLICATION_JSON_TYPE));

        if (ProcessResultTypeEnum.valueOf(result.getDataType()).equals(ProcessResultTypeEnum.JSON)) {
            String jsonString = new String(result.getData(), Charset.defaultCharset());
            multipart.bodyPart(new BodyPart(jsonString, MediaType.APPLICATION_JSON_TYPE));
        } else if (ProcessResultTypeEnum.valueOf(result.getDataType()).equals(ProcessResultTypeEnum.CSV)) {
            multipart.bodyPart(new StreamDataBodyPart("myFile.csv", new ByteArrayInputStream(result.getData())));
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
     * @param bs the bytes to write
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
     * @param processId the process id to delete
     * @return the Response with HTTP status end message: useful in case of error.
     */
    @DELETE
    @Path("/{processId}")
    public Response deleteProcessData(@PathParam("processId") String processId) {
        try {
            processDataManager.removeProcessData(processId);
        } catch (IkatsDaoException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
        }
        return Response.status(Response.Status.OK).build();
    }

    /**
     * Import any process data type
     *
     * @param processId the processId
     * @param name      the name of the processResult
     * @param data      the data to save
     * @param uriInfo   all info on URI
     * @return the internal id
     */
    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public String importProcessResult(
            @QueryParam("processId") String processId,
            @QueryParam("name") String name,
            byte[] data,
            @Context UriInfo uriInfo) throws IkatsDaoException {

        Chronometer chrono = new Chronometer(uriInfo.getPath(), true);
        logger.info("ProcessId : " + processId);
        String id = processDataManager.importProcessData(processId, name, data);
        chrono.stop(logger);
        return id;
    }


    /**
     * lance l'import d'un resultat de type fichier.
     *
     * @param processId       the processId
     * @param fileis          the file input stream
     * @param fileDisposition information about the Multipart with file
     * @param formData        the form data
     * @param uriInfo         all info on URI
     * @return the internal id
     * @throws IkatsException if the result couldn't be read from the file parameter
     */
    @POST
    @Path("/{processId}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public String importProcessResult(
            @PathParam("processId") String processId,
            @FormDataParam("file") InputStream fileis,
            @FormDataParam("file") FormDataContentDisposition fileDisposition,
            FormDataMultiPart formData,
            @Context UriInfo uriInfo) throws IkatsDaoException, IkatsException {
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
            } else if (key.equals("fileSize")) {
                fileSize = new Long(Integer.parseInt((params.get(key).get(0).getValue())));
            } else if (key.equals("file")) {
                // do nothing it is the file inputStream, not a tag.
            }
        }
        String id;
        try {
            id = processDataManager.importProcessData(fileis, fileSize, processId, fileType, fileName);
        } catch (IOException e) {
            throw new IkatsException("Could not import result", e);
        } finally {
            chrono.stop(logger);
        }
        return id;
    }

    /**
     * lance l'import d'un resultat de type fichier.
     *
     * @param processId the processId
     * @param name      the name of the processResult
     * @param json      the contained json
     * @param sizeParam the size of the json string
     * @param uriInfo   all info on URI
     * @return the internal id
     */
    @POST
    @Path("/{processId}/JSON")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_JSON)
    public String importProcessResultAsJson(@PathParam("processId") String processId,
                                            @FormParam("name") String name,
                                            @FormParam("json") String json,
                                            @FormParam("size") String sizeParam,
                                            @Context UriInfo uriInfo) throws IkatsDaoException, IOException {

        Chronometer chrono = new Chronometer(uriInfo.getPath(), true);
        logger.info("processId : " + processId);
        Long size = Long.parseLong(sizeParam);
        String type = "JSON";
        InputStream is = new ByteArrayInputStream(json.getBytes());
        String id;
        id = processDataManager.importProcessData(is, size, processId, type, name);
        chrono.stop(logger);
        return id;
    }

}

