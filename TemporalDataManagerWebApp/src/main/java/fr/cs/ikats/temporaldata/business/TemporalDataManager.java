
package fr.cs.ikats.temporaldata.business;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import fr.cs.ikats.datamanager.DataManagerException;
import fr.cs.ikats.datamanager.client.RequestSender;
import fr.cs.ikats.datamanager.client.importer.IImportSerializer;
import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.datamanager.client.opentsdb.ImportResult;
import fr.cs.ikats.datamanager.client.opentsdb.ResponseParser;
import fr.cs.ikats.temporaldata.application.ApplicationConfiguration;
import fr.cs.ikats.temporaldata.application.TemporalDataApplication;
import fr.cs.ikats.temporaldata.business.internal.DataBaseClientManager;
import fr.cs.ikats.temporaldata.business.internal.ImportSerializerFactory;
import fr.cs.ikats.temporaldata.exception.ImportException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.utils.ExecutorManager;

/**
 * the business layer for Temporal Data management.
 * 
 */

public class TemporalDataManager {

	/** Timeout for atomic TS import, in milliseconds */
	private static final int IMPORT_FULL_TS_TIMEOUT = 45000;

	/** Wait time before checks between TS chunks, in milliseconds */ 
	private static final long IMPORT_CHECK_TS_CHUNKS_WAIT = 1000;

	private static Logger logger = Logger.getLogger(TemporalDataManager.class);

    /**
     * the URL builder instance
     */
    private DataBaseClientManager urlBuilder;

    private Pattern tsuidPattern = Pattern.compile(".*tsuids\":\\[\"(\\w*)\"\\].*");

    private Pattern funcIdPattern = Pattern.compile("[a-zA-Z0-9_]*");

    /**
     * default constructor for instanciation
     */
    public TemporalDataManager() {
        urlBuilder = new DataBaseClientManager();
    }

    /**
     * get the application configuration
     * 
     * @return the ApplicationConfiguration
     */
    protected ApplicationConfiguration getConfig() {
        return TemporalDataApplication.getApplicationConfiguration();
    }

    /**
     * return the DB Host value from configuration
     * 
     * @return the host of TS DB
     */
    protected String getHost() {
        return getConfig().getStringValue(ApplicationConfiguration.HOST_DB_API);
    }

    /**
     * return the URL base for DB HTTP API.
     * 
     * @return the DB HTTP API
     */
    protected String getURLDbApiBase() {
        return getConfig().getStringValue(ApplicationConfiguration.URL_DB_API_BASE);
    }

    /**
     * private method to get the import factory from Spring Context.
     * 
     * @return the serializer factory from Spring Context
     */
    private ImportSerializerFactory getImportFactory() {
        return TemporalDataApplication.getApplicationConfiguration().getSpringContext().getBean(ImportSerializerFactory.class);

    }

    /**
     * FIXME : cette fonction ne valide pas un pattern de Fonctional Identifier IKATS, mais simplement les caractères autorisés pour les tags OpenTSDB
     * validate the funcId value for the given tsuid
     * 
     * @param funcId
     *            the value to validate
     * @return true
     * @throws InvalidValueException
     *             if validation fails
     */
    public boolean validateFuncId(String funcId) throws InvalidValueException {
        Matcher matcher = funcIdPattern.matcher(funcId);
        if (matcher.matches()) {
            return true;
        }
        else {
            throw new InvalidValueException("TimeSerie", "FuncId", funcIdPattern.pattern(), funcId, null);
        }
    }

    /**
     * launch all the necessary import tasks
     * 
     * @param metric
     *            the metric name
     * @param fileis
     *            the input stream of data
     * @param resultats
     *            the results
     * @param tags
     *            the tags
     * @param fileName
     *            the imported file name
     * @throws Exception
     *             if problems occurred while launching tasks
     * @throws ImportException
     *             if data cannot be imported ( mostly problem of file format)
     * @return a date array with start date and end date for this file
     * @throws DataManagerException 
     * @throws IOException 
     */
    public long[] launchImportTasks(String metric, InputStream fileis, List<Future<ImportResult>> resultats, Map<String, String> tags,
            String fileName) throws ImportException, DataManagerException, IOException {
    	
        ExecutorService executorService = ExecutorManager.getInstance().getExecutorService(ApplicationConfiguration.IMPORT_THREAD_POOL_NAME);
        // FIXME FTO : to be static. But getConfig has to be static.
        int numberOfPointsByImport = getConfig().getIntValue(ApplicationConfiguration.IMPORT_NB_POINTS_BY_BATCH);
        
        IImportSerializer jsonizer = null;
        
        jsonizer = getImportFactory().getBetterSerializer(fileName, metric, fileis, tags);
        if (jsonizer == null) {
            throw new ImportException("Input file format is not recognized by any serializers");
        }

        // loop to submit import request for each TS chunk
        while (jsonizer.hasNext()) {
        	String json = jsonizer.next(numberOfPointsByImport);
        	ImportTSChunkTask task = new ImportTSChunkTask(json);
            logger.info("submit import task");

            Future<ImportResult> futureResult;
			try {
				futureResult = executorService.submit(task);
			} catch (RejectedExecutionException re) {
				// in case of exception : register the error into the result set.
				ImportResult result = new ImportResult();
				result.setSummary("RejectedExecutionException while submitting task: " + re.getMessage());
				result.addError("details", ExceptionUtils.getStackTrace(re));
				futureResult = CompletableFuture.completedFuture(result);
			}
			resultats.add(futureResult);
        }

        // FIXME FTO voir si la portion de code suivante devrait former endpoints permettant un traitement asynchorne des retours d'import... 
    	
    	// to simulate the timeout with check at each loop for diff between time started and time now
    	int timeout = IMPORT_FULL_TS_TIMEOUT;
    	long dateTimeout = new Date().getTime() + timeout;
    	
    	while (true) {
    		// check if there is a task that is not completed
    		boolean allResultsDone = true;
        	for (Future<ImportResult> future : resultats) {
				if (! future.isDone()) {
					allResultsDone = false;
					break;
				}
			}
        	
        	if (allResultsDone) {
        		break;
        	}
        	
        	// check for timeout
        	if (new Date().getTime() > dateTimeout) {
        		// cancel all tasks
            	for (Future<ImportResult> future : resultats) {
    				if (! future.isDone()) {
    					future.cancel(true);
    					break;
    				}
    			}
            	
            	// out of loop
        		break;
        	} else {
        		// sleep a moment before restart the loop again
        		try {
					Thread.sleep(IMPORT_CHECK_TS_CHUNKS_WAIT);
				} catch (InterruptedException e) {
					// do nothing because there is not impact here
					logger.warn("Interrupted while waiting in loop of launchImportTasks for ImportResults to be available", e);
				}
        	}
    	}
    	
    	// return executor service into the pool 
    	ExecutorManager.getInstance().returnExecutorService(ApplicationConfiguration.IMPORT_THREAD_POOL_NAME, executorService);
    	
        return jsonizer.getDates();
    }

    /**
     * inner class : an import task using the generated JSON to send a request
     * to openTSDB API
     */
    protected class ImportTSChunkTask implements Callable<ImportResult> {
    	
        /** JSON import data */
        private String json;

        /**
         * build the json input string from the given IImportSerializer
         * 
         * @param reader
         *            Component
         * @throws ImportException
         *             if the task cannot be created
         */
        public ImportTSChunkTask(String json) throws ImportException {
           this.json = json;
        }

        /**
         * send the import request with json data. {@inheritDoc}
         */
        @Override
        public ImportResult call() {
            String host = getHost();
            ImportResult importResult = null;
            try {
                // json = jsonizer.next();
                if (json != null && !json.isEmpty()) {
                    String url = "http://" + host + getURLDbApiBase() + getConfig().getStringValue(ApplicationConfiguration.URL_DB_API_IMPORT);
                    logger.debug("sending request to url " + url);
                    Response response = RequestSender.sendPUTJsonRequest(url, json);
                    importResult = ResponseParser.parseImportResponse(response, response.getStatus());
                    logger.debug("Import task finished with result : " + importResult);
                }
                else {
                    logger.error("JSON data is empty");
            		importResult = new ImportResult();
            		importResult.setSummary("JSON data is empty");
                }
            }
            catch (Throwable e) {
                logger.error("Exception occured while sending json to db", e);
        		importResult = new ImportResult();
        		importResult.setSummary("Exception occured while sending json to db: " + e.getMessage());
        		importResult.addError("details", ExceptionUtils.getStackTrace(e));
            }
            
            return importResult;
        }

    }

    /**
     * get and parse the import result from all the Callable future results of
     * the Import tasks. generate an ImportResult with the following information
     * : tsuid, number of success, number of errors.
     * 
     * @param metric
     *            the metric
     * @param resultats
     *            the Future Results from the tasks
     * @param tags
     *            the queryParams ( tags)
     * @param startDate
     *            date of the first imported point.
     * @param endDate
     *            date of the last imported point
     * @return an ImportResult
     * @throws InterruptedException
     *             if task result cannot be retrieved
     * @throws ExecutionException
     *             if task cannot be executed
     * @throws IkatsWebClientException
     *             if TSUID cannot be retrieved
     */
    public ImportResult parseImportResults(String metric, List<Future<ImportResult>> resultats, Map<String, String> tags, Long startDate,
            Long endDate) throws InterruptedException, ExecutionException, IkatsWebClientException {
        ImportResult resultatTotal = new ImportResult();
        long success = 0L;
        for (Future<ImportResult> resultat : resultats) {
            ImportResult importResult = resultat.get();
            if (importResult != null) {
                success = success + importResult.getNumberOfSuccess();
                resultatTotal.addErrors(importResult.getErrors());
            }

        }
        // ad the tag map
        StringBuilder tagSb = new StringBuilder("{");
        tags.forEach((k, v) -> tagSb.append(k).append("=").append(v).append(","));
        // if no tags, set the metric as tag
        if (tags.isEmpty()) {
            tagSb.append("metric=").append(metric).append(",");
        }
        // remove the trailing "," char
        tagSb.replace(tagSb.lastIndexOf(","), tagSb.length(), "}");

        // launch a getTS request
        String tsuid = getTSUID(metric, startDate, endDate, tagSb.toString());
        resultatTotal.setNumberOfSuccess(success);
        resultatTotal.setSummary("Import of TS : " + tsuid);
        resultatTotal.setTsuid(tsuid);
        resultatTotal.setStartDate(startDate);
        resultatTotal.setEndDate(endDate);
        logger.info(resultatTotal.toString());
        return resultatTotal;
    }

    /**
     * get the TS from database.
     * 
     * @param metrique
     *            metrcic name
     * @param queryParams
     *            the queryParams
     * @return the JSON representation of the TS
     * @throws UnsupportedEncodingException
     *             if loookup request cannot be encoded
     * @throws IkatsWebClientException
     *             if lookup request fails
     * @throws ResourceNotFoundException
     *             if lookup request returns status over 200
     */
    public String getTS(String metrique, MultivaluedMap<String, String> queryParams)
            throws UnsupportedEncodingException, IkatsWebClientException, ResourceNotFoundException {
        String url;
        Response response;
        url = "http://" + getHost() + getURLDbApiBase() + urlBuilder.generateLookupRequest(metrique, queryParams);
        logger.debug(url);
        response = RequestSender.sendGETRequest(url, getHost());
        if (response.getStatus() > 200) {
            throw new ResourceNotFoundException("Unable to find resource : " + response.readEntity(String.class));
        }
        return response.readEntity(String.class);
    }

    /**
     * get the metadata for a tsuid
     * 
     * @param tsuid
     *            the tsuid
     * @return an object containiong
     * @throws IkatsWebClientException
     *             if request cannot be send
     * @throws ResourceNotFoundException
     *             if tsuid does not exists
     */
    public String getMetaData(String tsuid) throws IkatsWebClientException, ResourceNotFoundException {
        String url;
        Response response;
        url = "http://" + getHost() + getURLDbApiBase() + urlBuilder.generateUIDMetaRequest(tsuid);
        logger.debug(url);
        response = RequestSender.sendGETRequest(url, getHost());
        if (response.getStatus() > 200) {
            throw new ResourceNotFoundException("Unable to find TSUID : " + response.readEntity(String.class));
        }
        // return "{\"tsuid\": \""+tsuid+"\",\"metric\": {\"uid\": \"00002A\",
        // \"type\": \"METRIC\", \"name\": \"sys.cpu.0\", \"description\":
        // \"System CPU Time\", \"notes\": \"\",\"created\": 1350425579,
        // \"custom\": null, \"displayName\": \"\"},\"tags\": [{\"uid\":
        // \"000001\",\"type\": \"TAGK\",\"name\": \"host\",\"description\":
        // \"Server Hostname\",\"notes\": \"\",\"created\":
        // 1350425579,\"custom\": null, \"displayName\": \"Hostname\" }, {
        // \"uid\": \"000001\", \"type\": \"TAGV\", \"name\":
        // \"web01.mysite.com\", \"description\": \"Website hosting server\",
        // \"notes\": \"\", \"created\": 1350425579, \"custom\": null,
        // \"displayName\": \"Web Server 01\" } ], \"description\": \"Measures
        // CPU activity\", \"notes\": \"\", \"created\": 1350425579, \"units\":
        // \"\", \"retention\": 0, \"max\": \"NaN\", \"min\": \"NaN\",
        // \"custom\": { \"owner\": \"Jane Doe\", \"department\":
        // \"Operations\", \"assetTag\": \"12345\" }, \"displayName\": \"\",
        // \"dataType\": \"absolute\", \"lastReceived\": 1350425590,
        // \"totalDatapoints\": 12532}";

        return response.readEntity(String.class);

    }

    /**
     * get the TS from data base for tsuid parameters.
     * 
     * @param tsuid
     *            the requested tsuid
     * @param startDate
     *            the start date
     * @param endDate
     *            the end date
     * @param urlOptions
     *            other options
     * @param aggregationMethod
     *            the aggregation method ( can be null, sum is used)
     * @param downSampler
     *            downsampling
     * @param downSamplerPeriod
     *            the period
     * @return the Response
     * @throws IkatsWebClientException
     *             if lookup request fails
     */
    public Response getTSFromTSUID(List<String> tsuid, String startDate, String endDate, String urlOptions, String aggregationMethod,
            String downSampler, String downSamplerPeriod) throws IkatsWebClientException {
        String url;
        url = "http://" + getHost() + getURLDbApiBase()
                + urlBuilder.generateQueryTSUIDUrl(tsuid, aggregationMethod, startDate, endDate, urlOptions, downSampler, downSamplerPeriod);
        logger.debug(url);
        Response webResponse = RequestSender.sendGETRequest(url, getHost());
        return webResponse;
    }

    /**
     * get the TSUID for metric, tags and start/end date.
     * request is done for counting the numberb of points 
     * in the interval [startDate .. endDate]
     * 
     * @param metric
     *            the metric name
     * @param date
     *            the end date of the timeseries
     * @param tags
     *            the tags
     * @return the TSUID
     * @throws IkatsWebClientException
     *             if request cannot be generated or sent
     */
    public String getTSUID(String metric, Long startDate, Long endDate, String tags) throws IkatsWebClientException {
        String tsuid = null;
        String url = "http://" + getHost() + getURLDbApiBase()
                + urlBuilder.generateMetricQueryUrl(metric, tags, "sum", "count", "100y", Long.toString(startDate), Long.toString(endDate), "show_tsuids");
        Response webResponse = RequestSender.sendGETRequest(url, getHost());
        String str = webResponse.readEntity(String.class);
        logger.debug("GET TSUID response : " + str);
        Matcher matcher = tsuidPattern.matcher(str);
        if (matcher.matches()) {
            tsuid = matcher.group(1);
        }
        return tsuid;

    }

    /**
     * 
     * @param tsuid
     *            the tsuid
     * @param metric
     *            associated to tsuid
     * @param tags
     *            associated to tsuid
     * @return the deleted points
     * @throws IkatsWebClientException
     *             if errors
     * @throws ResourceNotFoundException
     *             if tsuid not found
     */
    public Response deleteTS(String tsuid) throws ResourceNotFoundException, IkatsWebClientException {
        ArrayList<String> listTsuid = new ArrayList<String>();
        listTsuid.add(tsuid);
        String url = "http://" + getHost() + getURLDbApiBase() + urlBuilder.generateQueryTSUIDUrl(listTsuid, null, "0", null, null, null, null);
        logger.debug(url);
        Response response = RequestSender.sendDELETERequest(url);
        if (response.getStatus() != 200) {
            throw new ResourceNotFoundException("Unable to find TSUID : " + response.readEntity(String.class));
        }

        return response;

    }

    /**
     * get the TS from database for the query parameters
     * 
     * @param metrique
     *            the metric name
     * @param startDate
     *            the start date
     * @param endDate
     *            the end date
     * @param urlOptions
     *            other options
     * @param tags
     *            the tags to request
     * @param aggregationMethod
     *            aggragation method
     * @param downSampler
     *            downsampling
     * @param downSamplerPeriod
     *            the period
     * @param downSamplingAdditionalInformation
     *            if min/max/ sd must be added to the response.
     * @throws IkatsWebClientException
     *             if request cannot be generated or sent
     * @return the Response
     */
    public Response getTS(String metrique, String startDate, String endDate, String urlOptions, String tags, String aggregationMethod,
            String downSampler, String downSamplerPeriod, boolean downSamplingAdditionalInformation) throws IkatsWebClientException {
        String url;
        String host = getHost();
        if (downSamplingAdditionalInformation) {
            logger.info("Additionnal information asked : dev,min and max aggregation method added to query");
            url = "http://" + host + getURLDbApiBase()
                    + urlBuilder.generateMetricQueryUrl(metrique, tags, aggregationMethod, downSampler, downSamplerPeriod, startDate, endDate,
                            urlOptions)
                    + "&" + urlBuilder.generateMetricQueryForQueryRequest(aggregationMethod, "dev", downSamplerPeriod, metrique, tags) + "&"
                    + urlBuilder.generateMetricQueryForQueryRequest(aggregationMethod, "min", downSamplerPeriod, metrique, tags) + "&"
                    + urlBuilder.generateMetricQueryForQueryRequest(aggregationMethod, "max", downSamplerPeriod, metrique, tags);
            logger.debug(url);

        }
        else {
            url = "http://" + host + getURLDbApiBase() + urlBuilder.generateMetricQueryUrl(metrique, tags, aggregationMethod, downSampler,
                    downSamplerPeriod, startDate, endDate, urlOptions);
        }
        logger.debug(url);
        Response webResponse = RequestSender.sendGETRequest(url, host);

        return webResponse;
    }
}
