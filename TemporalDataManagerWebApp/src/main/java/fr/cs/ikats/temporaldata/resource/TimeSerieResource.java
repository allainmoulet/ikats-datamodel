package fr.cs.ikats.temporaldata.resource;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoInvalidValueException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
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
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.utils.Chronometer;
import fr.cs.ikats.ts.dataset.DataSetFacade;
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

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Time Series resource : This class hosts all the operations available on Time
 * Series.
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
     * Method handling HTTP GET requests. The returned object will be sent to
     * the client as "text/plain" media type.
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
    public String getTS(@PathParam("metrique") String metrique, @Context UriInfo uriInfo) throws ResourceNotFoundException {
        String response = null;
        MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters(true);
        try {
            response = temporalDataManager.getTS(metrique, queryParams);
        } catch (IkatsWebClientException e) {
            throw new ResourceNotFoundException("Get TS returned exception", e);
        } catch (UnsupportedEncodingException e) {
            throw new ResourceNotFoundException("Get TS returned exception", e);
        }
        // TODO : ne pas renvoyer la reponse brute, mais un JSON standardisé
        return response;
    }

    /**
     * get the TSinformation for a tsuid :<br>
     * Response returns a JSON like this
     * <p>
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
    public TSInfo getTSInfo(@PathParam("tsuid") String tsuid) throws ResourceNotFoundException, IkatsException {
        String response = null;
        try {
            response = temporalDataManager.getMetaData(tsuid);
        } catch (IkatsWebClientException e) {
            throw new ResourceNotFoundException("Get TS returned exception", e);
        }

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

        FunctionalIdentifier id = metadataManager.getFunctionalIdentifierByTsuid(tsuid);
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
            response = temporalDataManager.getTS("*", null);
        } catch (IkatsWebClientException e) {
            throw new ResourceNotFoundException("Get TS returned exception", e);
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
    public String getTS(@PathParam("metrique") String metrique, @QueryParam("sd") String startDate, @QueryParam("ed") String endDate,
                        @QueryParam("o") String urlOptions, @QueryParam("t") String tags, @QueryParam("ag") String aggregationMethod,
                        @QueryParam("ds") String downSampler, @QueryParam("dp") String downSamplerPeriod,
                        @QueryParam("di") @DefaultValue("false") boolean downSamplingAdditionalInformation) {
        Chronometer chrono = new Chronometer("QueryResource:getTS", true);
        String response;
        try {
            Response webResponse = temporalDataManager.getTS(metrique, startDate, endDate, urlOptions, tags, aggregationMethod, downSampler,
                    downSamplerPeriod, downSamplingAdditionalInformation);
            response = webResponse.readEntity(String.class);

        } catch (IkatsWebClientException e) {
            e.printStackTrace();
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
    public String getTSFromTSUID(@QueryParam("tsuid") List<String> tsuid, @QueryParam("sd") String startDate, @QueryParam("ed") String endDate,
                                 @QueryParam("o") String urlOptions, @QueryParam("ag") String aggregationMethod, @QueryParam("ds") String downSampler,
                                 @QueryParam("dp") String downSamplerPeriod) throws Exception {
        Chronometer chrono = new Chronometer("QueryResource:getTS", true);
        String response;
        try {
            Response webResponse = temporalDataManager.getTSFromTSUID(tsuid, startDate, endDate, urlOptions, aggregationMethod, downSampler,
                    downSamplerPeriod);
            if (webResponse.getStatus() > ApiStatus.CODE_200.value()) {
                throw new ResourceNotFoundException(webResponse.readEntity(String.class));
            }
            response = webResponse.readEntity(String.class);
        } catch (IkatsWebClientException e) {
            e.printStackTrace();
            response = e.getMessage();
        }
        chrono.stop(logger);
        return response;
    }

    /**
     * HTTP Import resource. File is located on an readable file system on
     * server side. metric and dataset are part of the resource. Other tags must
     * be added as query parameters. They will be set as it into the JSON ( or
     * line) representation of the data sent to the db API.
     *
     * @param metric     value of dataset
     * @param tsFilepath path of the file in the local file system on the server.
     * @param uriInfo    the URI requested, used to get the tags as query parameters
     * @return import is OK
     * @throws Exception when error occurs
     */
    @PUT
    @Path("{metric}")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse importTSLocal(
            @PathParam("metric") String metric,
            @FormParam("file") String tsFilepath,
            @FormParam("funcId") String funcId,
            MultivaluedMap<String, String> formParams,
            @Context UriInfo uriInfo) throws Exception {

        // Check parameters
        File tsFile = new File(IKATSDATA_IMPORT_ROOT_PATH + tsFilepath);
        if (!tsFile.exists() || !tsFile.canRead()) {
            // FIXME a changer par une exception plus précise et/ou un retour d'état 404 ou 50X pour ressource inaccessible
            throw new ImportException("Can't access " + tsFile.getAbsolutePath());
        }

        // Start to process the request
        FileInputStream fileIS = new FileInputStream(tsFile);
        String filename = tsFile.getName();

        return doImport(filename, metric, funcId, formParams, fileIS);

    }

    @DELETE
    @Path("/{tsuid}")
    public Response removeTimeSeries(@PathParam("tsuid") String tsuid) throws ResourceNotFoundException, IkatsWebClientException, IkatsDaoException {
        // delete timeseries data from database
        try {
            DataSetManager datasetManager = new DataSetManager();
            if (datasetManager.getContainers(tsuid).isEmpty()) {
                // always with status 200 or exception raised
                Response response = temporalDataManager.deleteTS(tsuid);
                // delete associated meta data
                metadataManager.deleteMetaData(tsuid);
                // delete associated functional identifier
                FunctionalIdentifier func_id = metadataManager.getFunctionalIdentifierByTsuid(tsuid);
                logger.info("Functional Identifier :" + func_id.toString());
                if (func_id != null) {
                    metadataManager.deleteFunctionalIdentifier(tsuid);
                }
                return Response.status(Status.NO_CONTENT).entity(response.readEntity(String.class)).build();
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
            throw new IkatsDaoException("Failed removeTimeSeries with tsuid=" + tsuid + " : failed deleting associated metadata or functional id", e);
        } catch (IkatsWebClientException ew) {
            // IkatsDaoException => server error Status.BAD_REQUEST
            throw new IkatsWebClientException("Failed removeTimeSeries with tsuid=" + tsuid, ew);
        } catch (Throwable i) {
            // or else ...
            // see error handlers dealing with specific status
            // WebApplicationException => server error
            // Status.INTERNAL_SERVER_ERROR
            throw new WebApplicationException("Failed removeTimeSeries with tsuid=" + tsuid, i);
        }

    }

    @GET
    @Path("/{tsuid}/ds")
    public Response getDsTimeSeries(@PathParam("tsuid") String tsuid) throws IkatsDaoMissingRessource, IkatsDaoException {
        FunctionalIdentifier funcId = metadataManager.getFunctionalIdentifierByTsuid(tsuid);
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
                                        @FormDataParam("file") FormDataContentDisposition fileDisposition, FormDataMultiPart formData, @Context UriInfo uriInfo)
            throws ImportException {
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
     * Do the import job : retrieve params and parse it to get: -funcId
     * -startDate -endDate -tags Map<String, List<String>> but values are
     * constraint to one Import in openTSDB if funID provided Import startdate,
     * enddate and tags in pgsql
     *
     * @param filename   filename
     * @param metric     value of metric
     * @param funcId     the functional identifier to use for import
     * @param formParams the form information
     * @return ImportResult
     * @throws ImportException if problems occurs
     */
    private ApiResponse doImport(
            String filename,
            String metric,
            String funcId,
            MultivaluedMap<String, String> formParams,
            InputStream tsStream) throws ImportException {

        logger.info("Import file: " + filename);
        logger.info("Metric: " + metric);
        logger.info("Provided FunctionalIdentifier: " + funcId);

        if (funcId == null || funcId.isEmpty()) {
            // no import if no functional identifier provided into form
            throw new ImportException("No functional id provided in the form or is null, import canceled");
        }

        // Throw exception for a non valid funcId (openTSDB tag format)
        try {
            temporalDataManager.validateFuncId(funcId);
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
                            // modif agn 04/24: List<String> for tag value
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
            long[] dates = temporalDataManager.launchImportTasks(metric, tsStream, resultats, tags, filename);
            // Thread.sleep(TemporalDataApplication.getApplicationConfiguration().getLongValue(ApplicationConfiguration.DB_FLUSHING_INTERVAL));
            importResult = temporalDataManager.parseImportResults(metric, resultats, tags, dates[0], dates[1]);
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
                metadataManager.persistFunctionalIdentifier(importResult.getTsuid(), importResult.getFuncId());
            } catch (IkatsDaoConflictException e) {
                // Functional Identifier already exists : adding data to existing timeseries
            }

            // import metadatas in postgreSQL only if import in openTSDB succeed
            // store metadata metric
            try {
                metadataManager.persistMetaData(importResult.getTsuid(), "metric", metric, "string");
            } catch (IkatsDaoConflictException e) {
                // metric already exists : adding data to existing timeseries
            }

            // store tags as metadata
            try {
                for (Map.Entry<String, String> theTag : tags.entrySet()) {
                    metadataManager.persistMetaData(importResult.getTsuid(), theTag.getKey(), theTag.getValue(), "string");
                }
            } catch (IkatsDaoConflictException e) {
                // Metadata already exists : adding data to existing timeseries
            }

            // first date is the start_date
            // update in the case start date already exists
            try {
                MetaData metadata = metadataManager.getMetaData(importResult.getTsuid(), "ikats_start_date");
                if (dates[0] < Long.valueOf(metadata.getValue()).longValue()) {
                    metadataManager.updateMetaData(importResult.getTsuid(), "ikats_start_date", Long.toString(dates[0]));
                }
            } catch (IkatsDaoMissingRessource e) {
                metadataManager.persistMetaData(importResult.getTsuid(), "ikats_start_date", Long.toString(dates[0]), "date");
            }
            // last date is the end_date
            // update in the case end date already exists
            try {
                MetaData metadata = metadataManager.getMetaData(importResult.getTsuid(), "ikats_end_date");
                if (dates[1] > Long.valueOf(metadata.getValue()).longValue()) {
                    metadataManager.updateMetaData(importResult.getTsuid(), "ikats_end_date", Long.toString(dates[1]));
                }
            } catch (IkatsDaoMissingRessource e) {
                metadataManager.persistMetaData(importResult.getTsuid(), "ikats_end_date", Long.toString(dates[1]), "date");
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
    public List<FunctionalIdentifier> searchTsMatchingMetadataCriteria(FilterOnTsWithMetadata filterByMeta) throws IkatsDaoException {

        List<MetadataCriterion> metaCriteria = filterByMeta.getCriteria();
        String datasetName = filterByMeta.getDatasetName();
        List<FunctionalIdentifier> subSetList = filterByMeta.getTsList();

        if ((metaCriteria != null) && (!metaCriteria.isEmpty())) {
            if ((subSetList != null) && (subSetList.size() > 0)) {
                return metadataManager.searchFunctionalIdentifiers(filterByMeta);
            } else {
                // Review#156358 begin minor: you can keep your code ...
                //    I would have kept original code below, moving the "Not yet implemented" error in the MetadataManager::searchFunctionalIdentifiers(FilterOnTsWithMetadata)
                // Review#156358 end

                throw new IkatsDaoInvalidValueException("Not implemented: filtering dataset not handled.");
            }
        } else {
            // no criteria defined !
            //
            if ((subSetList == null) || subSetList.isEmpty()) {
                // no subset defined + no criteria defined => error
                throw new IkatsDaoInvalidValueException("Not yet implemented: filter on metadata without criteria AND without defined subset.");
            } else {
                logger.warn(
                        "searchTsMatchingMetadataCriteria: applying zero filter on input subset has no effect => return the subset defined in input");
                return subSetList;
            }
        }
    }
}
