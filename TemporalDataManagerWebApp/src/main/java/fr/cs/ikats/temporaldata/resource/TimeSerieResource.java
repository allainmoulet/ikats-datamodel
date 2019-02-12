/**
 * Copyright 2018 CS Systèmes d'Information
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.cs.ikats.temporaldata.resource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;
import org.glassfish.jersey.internal.util.collection.StringKeyIgnoreCaseMultivaluedMap;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoInvalidValueException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingResource;
import fr.cs.ikats.datamanager.client.opentsdb.ApiResponse;
import fr.cs.ikats.datamanager.client.opentsdb.ApiStatus;
import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.datamanager.client.opentsdb.ImportResult;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.metadata.model.MetadataCriterion;
import fr.cs.ikats.temporaldata.business.DataSetManager;
import fr.cs.ikats.temporaldata.business.FilterOnTsWithMetadata;
import fr.cs.ikats.temporaldata.business.TSInfo;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.ImportException;
import fr.cs.ikats.temporaldata.exception.ImportFileNotFoundException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.utils.Chronometer;
import fr.cs.ikats.ts.dataset.DataSetFacade;

/**
 * Time Series resource : This class hosts all the operations available on Time Series.
 */
@Path("ts")
public class TimeSerieResource extends AbstractResource {

    static final public String ACTIVATE_OPENTSDB_IMPORT_FLAG_NAME = "activateOpenTsdbImport";
    private static final String IKATSDATA_IMPORT_ROOT_PATH = "/IKATSDATA/";

    private static Logger logger = Logger.getLogger(TimeSerieResource.class);

    /**
     * default constructor
     */
    public TimeSerieResource() {
        super();
    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent to the client as "text/plain" media type.
     *
     * @return String that will be returned as a text/plain response.
     */
    @GET
    @Path("/capabilities")
    @Produces(MediaType.TEXT_PLAIN)
    public String getCapabilities() {
        return "This is the TS endpoint. Try /lookup/{metrique}, or extract/metric/{metrique}, or extract/tsuid to search\n"
                + " Import TS with /putlocal/{dataset} or /put/{dataset}/{metric} or /put/{metric}";
    }

    /**
     * @param metrique metric of TS
     * @param uriInfo  the URI params where to find the tags
     * @return the JSON reponse.
     * @throws ResourceNotFoundException if no TS matches the metric and the tags
     */
    @GET
    @Path("/lookup/{metrique}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getTS(@PathParam("metrique") String metrique, @Context UriInfo uriInfo)
            throws ResourceNotFoundException {
        String response = null;
        MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters(true);
        try {
            response = getTemporalDataManager().getTS(metrique, queryParams);
        } catch (UnsupportedEncodingException e) {
            throw new ResourceNotFoundException("Get TS returned exception", e);
        }
        return response;
    }

    /**
     * get the TSinformation for a tsuid :<br>
     * Response returns a JSON like this
     * <p>
     *
     * <pre>
     * {"tsuid":"0000110000030003F20000040003F1",
     *  "funcId":"A320001_1_WS1",
     *  "metric":"WS1",
     *  "tags":{"flightIdentifier":"1",
     *          "aircraftIdentifier":"A320001"}
     * }
     * </pre>
     *
     * @param tsuid the tsuid
     * @return a TSInfo
     * @throws ResourceNotFoundException if getTS sends an error
     * @throws IkatsException            if result cannot be parsed
     */
    @GET
    @Path("/tsuid/{tsuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public TSInfo getTSInfo(@PathParam("tsuid") String tsuid) throws IkatsDaoException, ResourceNotFoundException, IkatsException {
        String response = null;
        response = getTemporalDataManager().getMetaData(tsuid);

        TSInfo returnValue = new TSInfo();

        JSONParser parser = new JSONParser();
        try {
            JSONObject resultObject = (JSONObject) parser.parse(response);
            returnValue.setTsuid((String) resultObject.get("tsuid"));
            returnValue.setMetric((String) ((JSONObject) resultObject.get("metric")).get("name"));
            JSONArray res = (JSONArray) resultObject.get("tags");
            for (int i = 0; i < res.size() / 2; i++) {
                JSONObject itemkey = (JSONObject) res.get(i * 2);
                JSONObject itemValue = (JSONObject) res.get(i * 2 + 1);
                String name = (String) itemkey.get("name");
                String value = (String) itemValue.get("name");
                returnValue.addTag(name, value);
            }
        } catch (ParseException e) {
            throw new IkatsException("Error parsing response from db", e);
        }

        FunctionalIdentifier id = getMetadataManager().getFunctionalIdentifierByTsuid(tsuid);
        if (id != null) {
            returnValue.setFuncId(id.getFuncId());
        }

        return returnValue;
    }

    /**
     * get all the TSinformation
     *
     * @return a list of TSInfo
     * @throws ResourceNotFoundException if getTS sends an error
     * @throws IkatsException            if result cannot be parsed
     */
    @GET
    @Path("tsuid")
    @Produces(MediaType.APPLICATION_JSON)
    public List<TSInfo> getAllTS() throws ResourceNotFoundException, IkatsException {
        String response = null;
        try {
            response = getTemporalDataManager().getTS("*", null);
        } catch (UnsupportedEncodingException e) {
            throw new ResourceNotFoundException("Get TS returned exception", e);
        }

        // Parse the json to extract the data and build the response
        // Format is
        // [{'tsuid':tsuid1,'metric':metric1},{'tsuid':tsuid2,'metric':metric2},...]
        List<TSInfo> returnValue = new ArrayList<TSInfo>();

        JSONParser parser = new JSONParser();
        try {
            JSONObject resultObject = (JSONObject) parser.parse(response);
            if (resultObject.get("results") instanceof JSONArray) {
                JSONArray res = (JSONArray) resultObject.get("results");
                for (int i = 0; i < res.size(); i++) {
                    JSONObject item = (JSONObject) res.get(i);
                    String tsuid = (String) item.get("tsuid");
                    String metric = (String) item.get("metric");
                    TSInfo tsinfo = new TSInfo(metric, tsuid, null, null);
                    returnValue.add(tsinfo);
                }
            }
        } catch (ParseException e) {
            throw new IkatsException("Error parsing response from db", e);
        }

        return returnValue;
    }

    /**
     * @param metrique                          metric name
     * @param startDate                         start date
     * @param endDate                           end date
     * @param urlOptions                        other url options
     * @param tags                              the tags of the TS
     * @param aggregationMethod                 the aggregation method
     * @param downSampler                       the downsampling method
     * @param downSamplerPeriod                 the period
     * @param downSamplingAdditionalInformation if min/max/sd must be add to the response.
     * @return JSON representation of the TS data
     */
    @GET
    @Path("extract/metric/{metrique}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getTS(@PathParam("metrique") String metrique, @QueryParam("sd") String startDate,
                        @QueryParam("ed") String endDate, @QueryParam("o") String urlOptions, @QueryParam("t") String tags,
                        @QueryParam("ag") String aggregationMethod, @QueryParam("ds") String downSampler,
                        @QueryParam("dp") String downSamplerPeriod,
                        @QueryParam("di") @DefaultValue("false") boolean downSamplingAdditionalInformation) {
        Chronometer chrono = new Chronometer("QueryResource:getTS", true);
        String response;
        try {
            Response webResponse = getTemporalDataManager().getTS(metrique, startDate, endDate, urlOptions, tags,
                    aggregationMethod, downSampler, downSamplerPeriod, downSamplingAdditionalInformation);
            response = webResponse.readEntity(String.class);

        } catch (IkatsWebClientException e) {
            logger.error("Error while retrieving time series :", e);
            response = e.getMessage();
        }
        chrono.stop(logger);
        return response;
    }

    /**
     * @param tsuid             the tsuid
     * @param startDate         the start date
     * @param endDate           the end date
     * @param urlOptions        other URL options
     * @param aggregationMethod the aggregation method
     * @param downSampler       the downsampling method
     * @param downSamplerPeriod the period
     * @return the JSON representation of TS data
     * @throws Exception if error occurs
     */
    @GET
    @Path("extract/tsuid")
    @Produces(MediaType.APPLICATION_JSON)
    public String getTSFromTSUID(@QueryParam("tsuid") List<String> tsuid, @QueryParam("sd") String startDate,
                                 @QueryParam("ed") String endDate, @QueryParam("o") String urlOptions,
                                 @QueryParam("ag") String aggregationMethod, @QueryParam("ds") String downSampler,
                                 @QueryParam("dp") String downSamplerPeriod) throws ResourceNotFoundException {
        Chronometer chrono = new Chronometer("QueryResource:getTS", true);
        String response;
        try {
            Response webResponse = getTemporalDataManager().getTSFromTSUID(tsuid, startDate, endDate, urlOptions,
                    aggregationMethod, downSampler, downSamplerPeriod);
            if (webResponse.getStatus() > ApiStatus.CODE_200.value()) {
                throw new ResourceNotFoundException(webResponse.readEntity(String.class));
            }
            response = webResponse.readEntity(String.class);
        } catch (IkatsWebClientException e) {
            logger.error(e);
            response = e.getMessage();
        }
        chrono.stop(logger);
        return response;
    }

    /**
     * HTTP Import resource. File is located on an readable file system on server side. metric and dataset are part of
     * the resource. Other tags must be added as query parameters. They will be set as it into the JSON ( or line)
     * representation of the data sent to the db API.
     *
     * @param metric     value of dataset
     * @param tsFilepath path of the file in the local file system on the server.
     * @param uriInfo    the URI requested, used to get the tags as query parameters
     * @return import is OK
     * @throws ImportException when error occurs
     */
    @PUT
    @Path("{metric}")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse importTSLocal(@PathParam("metric") String metric, @FormParam("file") String tsFilepath,
                                     @FormParam("funcId") String funcId, MultivaluedMap<String, String> formParams, @Context UriInfo uriInfo)
            throws ImportException {

        // Check parameters

        // Start to process the request
        FileInputStream fileIS;
        File tsFile = new File(IKATSDATA_IMPORT_ROOT_PATH + tsFilepath);
        try {
            if (!tsFile.exists() || !tsFile.canRead()) {
                // Handled with status NOT_FOUND 404
                throw new ImportFileNotFoundException("Can't access " + tsFile.getAbsolutePath());
            }

            fileIS = new FileInputStream(tsFile);
        } catch (FileNotFoundException e) {
            // Handled with status NOT_FOUND 404
            throw new ImportFileNotFoundException("File not found " + tsFile.getAbsolutePath(), e);
        }

        String filename = tsFile.getName();

        return doImport(filename, metric, funcId, formParams, fileIS);

    }

    @DELETE
    @Path("/{tsuid}")
    public Response removeTimeSeries(@PathParam("tsuid") String tsuid)
            throws ResourceNotFoundException, IkatsWebClientException, IkatsDaoException {
        // delete timeseries data from database
        try {
            DataSetManager datasetManager = new DataSetManager();
            if (datasetManager.getContainers(tsuid).isEmpty()) {
                // always with status 200 or exception raised
                Response response = getTemporalDataManager().deleteTS(tsuid);
                // delete associated meta data
                getMetadataManager().deleteMetaData(tsuid);
                // delete associated functional identifier
                FunctionalIdentifier func_id = getMetadataManager().getFunctionalIdentifierByTsuid(tsuid);

                if (func_id != null) {
                    getMetadataManager().deleteFunctionalIdentifier(tsuid);
                    logger.info("Functional Identifier deleted:" + func_id);
                }
                String msg = response.getEntity().toString();
                return Response.status(Status.NO_CONTENT).entity(msg).build();
            } else {
                return Response.status(Status.CONFLICT).build();
            }
        } catch (ResourceNotFoundException e) {
            // kept different kind of errors : see error handlers dealing with
            // specific status
            // ResourceNotFoundException => Status.MISSING_RESSOURCE
            throw new ResourceNotFoundException("Failed removeTimeSeries with tsuid=" + tsuid, e);
        } catch (IkatsDaoException e) {
            // IkatsDaoException => Status.MISSING_RESSOURCE
            throw new IkatsDaoException("Failed removeTimeSeries with tsuid=" + tsuid
                    + " : failed deleting associated metadata or functional id", e);
        } catch (IkatsWebClientException ew) {
            // IkatsDaoException => server error Status.BAD_REQUEST
            throw new IkatsWebClientException("Failed removeTimeSeries with tsuid=" + tsuid, ew);
        }

    }

    @GET
    @Path("/{tsuid}/ds")
    public Response getDsTimeSeries(@PathParam("tsuid") String tsuid)
            throws IkatsDaoMissingResource, IkatsDaoException {
        FunctionalIdentifier funcId = getMetadataManager().getFunctionalIdentifierByTsuid(tsuid);
        if (funcId == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        DataSetFacade facade = new DataSetFacade();
        List<String> result = facade.getDataSetNamesForTsuid(tsuid);
        if (result.isEmpty()) {
            return Response.status(Status.NO_CONTENT).build();
        } else {
            return Response.status(Status.OK).entity(result.toString()).build();
        }
    }

    /**
     * override of the import without parameter dataset
     *
     * @param metric          value of metric
     * @param fileis          file InputStream read from multipart body
     * @param fileDisposition file information
     * @param uriInfo         the URI requested, used to get the tags as query parameters
     * @param formData        the form information
     * @return an ImportResult
     * @throws ImportException if error occurs
     */
    @POST
    @Path("/put/{metric}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse importTSFromHTTP(@PathParam("metric") String metric, @FormDataParam("file") InputStream fileis,
                                        @FormDataParam("file") FormDataContentDisposition fileDisposition, FormDataMultiPart formData,
                                        @Context UriInfo uriInfo) throws ImportException {
        String filename = fileDisposition.getFileName();

        // remove the file parameter and get the funcId
        Map<String, List<FormDataBodyPart>> fields = formData.getFields();
        fields.remove("file");
        List<FormDataBodyPart> funcIdFormValues = fields.get("funcId");
        String funcId = (funcIdFormValues != null) ? funcIdFormValues.get(0).getValue() : null;

        // transform the FormFataMultiPart into tags, with multivalued key taken into account
        MultivaluedMap<String, String> tags = new StringKeyIgnoreCaseMultivaluedMap<String>();
        fields.forEach((k, v) -> v.forEach(p -> tags.add(k, p.getValue())));

        // Do the import
        return doImport(filename, metric, funcId, tags, fileis);
    }

    /**
     * Do the import job : retrieve params and parse it to get: -funcId -startDate -endDate -tags Map<String, List
     * <String>> but values are constraint to one Import in openTSDB if funID provided Import startdate, enddate and
     * tags in pgsql
     *
     * @param filename   filename
     * @param metric     value of metric
     * @param funcId     the functional identifier to use for import
     * @param formParams the form information
     * @return ImportResult
     * @throws ImportException if problems occurs
     */
    private ApiResponse doImport(String filename, String metric, String funcId,
                                 MultivaluedMap<String, String> formParams, InputStream tsStream) throws ImportException {

        logger.info("Import file: " + filename);
        logger.info("Metric: " + metric);
        logger.info("Provided FunctionalIdentifier: " + funcId);

        if (funcId == null || funcId.isEmpty()) {
            // no import if no functional identifier provided into form
            throw new ImportException("No functional id provided in the form or is null, import canceled");
        }

        // Throw exception for a non valid funcId (openTSDB tag format)
        try {
            getTemporalDataManager().validateFuncId(funcId);
        } catch (InvalidValueException e1) {
            throw new ImportException(e1.getMessage(), e1);
        }

        Chronometer chrono = new Chronometer("TimeSeriResource.doImport|TS -> TSDB", false);
        List<Future<ImportResult>> resultats = new ArrayList<Future<ImportResult>>();
        Map<String, String> tags = new HashMap<String, String>();
        ImportResult importResult = null;

        try {
            // Prepare the tags map
            for (String key : formParams.keySet()) {
                if (key.equals("file")) {
                    // skip
                    continue;
                }

                if (formParams.get(key).size() > 0) {
                    for (String value : formParams.get(key)) {
                        if (!tags.containsKey(key)) {
                            tags.put(key, value);
                            logger.info("Tag : " + key + " - " + value);
                        } else {
                            // List<String> for tag value
                            // kept to avoid too much modifications
                            // in sub-function and interfaces, but we force
                            // the list to have only one value
                            logger.warn("Tag already exist: " + key + " ");
                        }
                    }
                }
            }
            logger.info("Tags: " + tags.toString());

            // import into openTSDB
            chrono.start();
            long[] dates = getTemporalDataManager().launchImportTasks(metric, tsStream, resultats, tags, filename);
            importResult = getTemporalDataManager().parseImportResults(metric, resultats, tags, dates[0], dates[1]);
            chrono.stop(logger);

            importResult.setFuncId(funcId);

            if (importResult.getTsuid() == null || importResult.getTsuid().isEmpty()) {
                String message = "TS not imported or no tsuid returned";
                StringBuilder sb = new StringBuilder(message);
                sb.append("OpenTSDB return code: ").append(importResult.getStatusCode());
                sb.append("Return summary: ").append(importResult.getSummary());
                sb.append("file: ").append(filename);

                logger.debug(sb.toString());
                throw new ImportException(message);
            }

            chrono = new Chronometer("TimeSeriResource.doImport|Metas -> SGBD", false);
            // store functional identifier
            try {
                getMetadataManager().persistFunctionalIdentifier(importResult.getTsuid(), importResult.getFuncId());
            } catch (IkatsDaoConflictException e) {
                logger.error("", e);
                // Functional Identifier already exists : adding data to existing timeseries
            }

            // import metadatas in postgreSQL only if import in openTSDB succeed
            // store metadata metric
            try {
                getMetadataManager().persistMetaData(importResult.getTsuid(), "metric", metric, "string");
            } catch (IkatsDaoConflictException e) {
                logger.error("", e);
                // metric already exists : adding data to existing timeseries
            }

            // store tags as metadata
            try {
                for (Map.Entry<String, String> theTag : tags.entrySet()) {
                    getMetadataManager().persistMetaData(importResult.getTsuid(), theTag.getKey(), theTag.getValue(),
                            "string");
                }
            } catch (IkatsDaoConflictException e) {
                logger.error("", e);
                // Metadata already exists : adding data to existing timeseries
            }

            // first date is the start_date
            // update in the case start date already exists: specifically needed for concurrent writters

            MetaData metadata = getMetadataManager().getMetaData(importResult.getTsuid(), "ikats_start_date");
            if (metadata == null) {
                getMetadataManager().persistMetaData(importResult.getTsuid(), "ikats_start_date",
                        Long.toString(dates[0]), "date");
            } else if (dates[0] < Long.valueOf(metadata.getValue()).longValue()) {
                getMetadataManager().updateMetaData(importResult.getTsuid(), "ikats_start_date",
                        Long.toString(dates[0]));
            }

            // last date is the end_date
            // update in the case end date already exists
            metadata = getMetadataManager().getMetaData(importResult.getTsuid(), "ikats_end_date");
            if (metadata == null) {
                getMetadataManager().persistMetaData(importResult.getTsuid(), "ikats_end_date", Long.toString(dates[1]),
                        "date");
            } else if (dates[1] > Long.valueOf(metadata.getValue()).longValue()) {
                getMetadataManager().updateMetaData(importResult.getTsuid(), "ikats_end_date", Long.toString(dates[1]));
            }

            chrono.stop(logger);

        } catch (ImportException e) {
            logger.error("Error during import:", e);
            throw e;
        } catch (IkatsDaoException e) {
            ImportException le = new ImportException("DAO Error during import:", e);
            logger.error(le);
            throw le;
        } catch (Exception e) {
            throw new ImportException("Unknown Error during import", e);
        } finally {
            try {
                tsStream.close();
            } catch (IOException ioe) {
                // That not an application problem, but a problem in the system.
                // If that exception is raised : there will be more urgent and critical problems in the system.
                // so do not try to do other thing here.
                logger.warn("TS stream exception on close", ioe);
            }
        }

        return importResult;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<FunctionalIdentifier> searchTsMatchingMetadataCriteria(FilterOnTsWithMetadata filterByMeta)
            throws IkatsDaoException {

        List<MetadataCriterion> metaCriteria = filterByMeta.getCriteria();
        List<FunctionalIdentifier> subSetList = filterByMeta.getTsList();

        if ((metaCriteria != null) && (!metaCriteria.isEmpty())) {
            if ((subSetList != null) && (subSetList.size() > 0)) {
                return getMetadataManager().searchFunctionalIdentifiers(filterByMeta);
            } else {
                throw new IkatsDaoInvalidValueException("Not implemented: filtering dataset not handled.");
            }
        } else {
            // no criteria defined !
            //
            if ((subSetList == null) || subSetList.isEmpty()) {
                // no subset defined + no criteria defined => error
                throw new IkatsDaoInvalidValueException(
                        "Not yet implemented: filter on metadata without criteria AND without defined subset.");
            } else {
                logger.warn(
                        "searchTsMatchingMetadataCriteria: applying zero filter on input subset has no effect => return the subset defined in input");
                return subSetList;
            }
        }
    }
}
