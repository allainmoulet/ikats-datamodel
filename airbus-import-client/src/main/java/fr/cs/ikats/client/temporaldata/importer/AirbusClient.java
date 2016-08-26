package fr.cs.ikats.client.temporaldata.importer;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import fr.cs.ikats.datamanager.client.opentsdb.ImportResult;
import fr.cs.ikats.temporaldata.utils.Chronometer;

/**
 * This client list all files in a directory organized like this :
 * <pre>
 * DATA_SET_NAME_1
 *     DAR
 *        AIRCRAFT_1 
 *           METRIC
 *              raw_1.csv
 *              raw_n.csv
 * </pre>
 * where raw_n.csv contains a full timeserie in a comprehensive format by IKATS.
 * and DATA_SET_NAME_1, AIRCRAFT_1 and METRIC can be everything.
 * 
 * Theses values are used 
 * <ul>
 * <li>to create tags and metric information for storage in opentsdb</li>
 * <li>to create the Fucntional identifier of the TimeSerie according to the funcId.pattern of the client.properties
 * configuration file.
 * </ul>
 */
public class AirbusClient {

    /**
     * Prefix of logged messages: this may be useful for grep
     */
    static public final String SUCCESS_MSG_PREFIXE = "Successful import => ";
    
    /**
     * Prefix of logged messages: this may be useful for grep
     */
    static public final String FAILURE_MSG_PREFIXE = "Failed import => ";
    
    /**
     * Value returned by ImportCallable when logOnly is true
     */
    static public final String LOGGED_ONLY_TSUID = "";
    
    /**
     * Initial value for String returned by ImportCallable: this may be returned if the import service
     * met unexpected exception, error
     */
    static public final String UNDEFINED_TSUID = null;

    /**
     * Waiting time for import timeout
     */
	private static final long WAIT_FOR_IMPORT_TIMEOUT = 180;
    
    /**
     * url to the TemporalDataManager Webapp
     */
    final private String appUrl;
    /**
     * name of the tag used to store the aircraftId
     */
    final private String aircraftTagName;
    /**
     * name of the tag used to store the flightIdentifier
     */
    final private String flightIdentifierTagName;
    /**
     * pattern used to generate the functional Id
     */
    final private String funcIdPattern;
    /**
     * private static logger.
     */
    private static Logger logger = Logger.getLogger(AirbusClient.class);

    /**
     * the ExecutorService to use to submit import tasks
     */
    ExecutorService service;

    /**
     * default constructor, using the Configuration objet to set the properties
     * from configuration.
     * @param config the configuration
     */
    public AirbusClient(AirbusClientConfiguration config) {
        appUrl = config.getStringValue("appUrl");
        aircraftTagName = config.getStringValue("tag.aircraftId");
        flightIdentifierTagName = config.getStringValue("tag.flightId");
        funcIdPattern = config.getStringValue("funcId.pattern");
    }

    /**
     * stop the client, shutdown the executor
     */
    protected void stop() {
    	if (service != null && !service.isTerminated()) {
	        List<Runnable> notTerminatedTasks = service.shutdownNow();
	        if (notTerminatedTasks.isEmpty() != true) {
	        	logger.warn("There are some tasks to wait for termination. Waiting for 15sec");
	        	try {
					if (service.awaitTermination(15, TimeUnit.SECONDS) != true) {
			        	logger.warn("Timeout occurs while waiting for termination");
					}
				} catch (InterruptedException ie) {
					logger.error("ExecutorService interrupted while waiting for termination", ie);
				}
	        }
    	}
    }
    
    /**
     * build the fonctional ID from all the path information
     * 
     * @param dataset
     * @param aircraftId
     * @param metric
     * @param flightId
     * @return
     */
    private String buildFuncId(String dataset, String aircraftId, String metric, String flightId) {
        String funcId = funcIdPattern;
        funcId = funcId.replace("{dataset}", dataset);
        funcId = funcId.replace("{aircraftId}", aircraftId);
        funcId = funcId.replace("{flightId}", flightId);
        funcId = funcId.replace("{metric}", metric);
        return funcId;
    }

    /**
     * get the tags values and functionalidentifier from the file Path
     * and launch import
     * 
     * @param rootDirectory the root directory where the structured directory are found.
     * @param relativePath the relative path of the file to import.
     * @return the tsuid generated by the TemporalDataWebApp
     */
    private ImportResult launchImportRequestForAirbusFile(File rootDirectory, String relativePath) {
        String[] subdirs = StringUtils.splitPreserveAllTokens(relativePath, "/");

        String dataset = subdirs[0];
        String aircraftId = subdirs[2];
        String metric = subdirs[3];
        String flightId = subdirs[4].substring("raw_".length(), subdirs[4].lastIndexOf(".csv"));

        String funcId = buildFuncId(dataset, aircraftId, metric, flightId);

        return doImport(rootDirectory, relativePath, dataset, aircraftId, metric, flightId, funcId);
    }

    /**
     * log the file and the extracted informations
     * @param rootDirectory the root directory where the structured directory are found.
     * @param relativePath the relative path of the file to import.
     */
    private void logAirbusFile(File rootDirectory, String relativePath) {
        String[] subdirs = StringUtils.splitPreserveAllTokens(relativePath, "/");

        String dataset = subdirs[0];
        String aircraftId = subdirs[2];
        String metric = subdirs[3];
        String flightId = subdirs[4].substring("raw_".length(), subdirs[4].lastIndexOf(".csv"));
        String funcId = buildFuncId(dataset, aircraftId, metric, flightId);
        logger.info("launching import on file " + relativePath + " : dataset=" + dataset + " aircraftId=" + aircraftId + " metric=" + metric
                + " flightId=" + flightId +" => funcId="+funcId);
    }

    /**
     * launch the HTTP request for import and parse the response.
     * @param rootDirectory the root directory where the structured directory are found.
     * @param relativePath the relative path of the file to import.
     * @param dataset name of the dataset
     * @param aircraftId identifier of the aircraft
     * @param metric name of the metric
     * @param flightId id of the flight
     * @param functionalIdentifier the functional Identifier
     * @return the internal tsuid for the file
     */
    protected ImportResult doImport(File rootDirectory, String relativePath, String dataset, String aircraftId, String metric, String flightId,
            String functionalIdentifier) {
        
        String url = appUrl + "/ts/" + metric;
        File localFile = new File(rootDirectory, relativePath);
        File distantFile = new File(relativePath);

        MultivaluedMap<String, String> formParams = new MultivaluedHashMap<String, String>();
        
        formParams.add("tsFile", distantFile.getPath());
        formParams.add("funcId", functionalIdentifier);
        
        // add a field with aircraftId if SET
        if(!aircraftTagName.equals(AirbusClientConfiguration.NOT_SET)) {
        	formParams.add(aircraftTagName, aircraftId);
        }
        // add a field with flightId if SET
        if(!flightIdentifierTagName.equals(AirbusClientConfiguration.NOT_SET))  {
        	formParams.add(flightIdentifierTagName, flightId);
        }
        
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(JacksonFeature.class);
        //clientConfig.register(LoggingFilter.class);
        JerseyClient client = JerseyClientBuilder.createClient(clientConfig);
        
        // Build the PUT request
        WebTarget target = client.target(url);
        logger.debug("sending url : " + target.getUri());
        Response response = target.request(MediaType.APPLICATION_JSON_TYPE).put(Entity.entity(formParams, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
        
        // Read response
        ImportResult result = response.readEntity(ImportResult.class);
        if(response.getStatus() > 200) {
            logger.error( FAILURE_MSG_PREFIXE + localFile.getAbsolutePath());
            // Error message set in fr.cs.ikats.temporaldata.exception.ImportExceptionHandler.toResponse(ImportException)
            logger.error("Response summary: " + result.getSummary() + "| Details: " + result.getErrors().get("details"));
            throw new Error("Response status " + response.getStatus());
        } else {
            logger.info( SUCCESS_MSG_PREFIXE + localFile.getAbsolutePath());
            logger.info(result);
        }
        
        return result;
    }

    /**
     * import the corresponding dataset.
     * @param dataSetName the name of the dataset
     * @param tsuidList the list of tsuids 
     * @param update indicate if it is an update or a creation
     */
    protected void importDataSet(String dataSetName, List<String> tsuidList,boolean update) {
        if(tsuidList!=null && !tsuidList.isEmpty()) {
            
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.register(MultiPartFeature.class);
            Client client = ClientBuilder.newClient(clientConfig);
    
            Response response = null;
            
            Form form = new Form();
            
            StringBuilder sb = new StringBuilder();
            for(int j=0;j<tsuidList.size();j++) {               
                sb.append(tsuidList.get(j));
                if(j<tsuidList.size()-1) {
                    sb.append(",");  
                }
            }
            
            form.param("description", "DataSet Created By import tool");
            form.param("tsuidList", sb.toString());
            if(update) {
                String url = appUrl + "/dataset/" + dataSetName;
                WebTarget target = client.target(url);
                response = target.request(MediaType.APPLICATION_FORM_URLENCODED).put(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
            } else {
                String url = appUrl + "/dataset/import/" + dataSetName;
                WebTarget target = client.target(url);
                response = target.request().post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
            }
            logger.info("Reponse status = "+response.getStatus());
            if(!update && response.getStatus()==Response.Status.CREATED.getStatusCode()) {
                logger.info("DataSet : " + dataSetName+" created for "+tsuidList.size()+" TS");
            } else if(update && response.getStatus()==Response.Status.NO_CONTENT.getStatusCode()) {
                logger.info("DataSet : " + dataSetName+" update with "+tsuidList.size()+" TS");
            } else {
                logger.info("Error while updating/creating dataset");
            }
        } else {
            logger.info("No Dataset imported because no TS imported");
        }
        
    }

    /**
     * import all the file found in the directory of the given datasetName
     * @param datasetName the dataset name
     * @param rootPath the rootpath of import
     * @param logOnly to only log files and information extracted.
     * @return the number of successfull imports
     */
    public List<String> importFullDirectory(String datasetName, String rootPath, boolean logOnly) {
        Chronometer chrono = new Chronometer("import", true);
        int count = 0;
        int errors = 0;
        List<String> tsuidList = new ArrayList<String>();

        File rootDir = new File(rootPath);
        File dataSetDir = new File(rootDir.getAbsolutePath() + "/"+datasetName + "/DAR/");

        // Prepare a sequential, single threaded executor service and submit tasks
        service = Executors.newSingleThreadExecutor();
        List<ImportCallable> importTasks = new ArrayList<ImportCallable>();

		if (dataSetDir.exists()) {
			// list all the aircraftIdentifiers :
			for (File airCraftDir : dataSetDir.listFiles()) {
				String airCraftId = airCraftDir.getName();
				for (File metricDir : airCraftDir.listFiles()) {
					String metric = metricDir.getName();
					for (File flightFile : metricDir.listFiles()) {
						if (flightFile.canRead() && flightFile.getName().startsWith("raw")) {
							ImportCallable importTask = new ImportCallable(datasetName, rootDir, airCraftId, metric, flightFile.getName(), logOnly);
							importTasks.add(importTask);
							importTask.futureResult = service.submit(importTask);
						}
					}
				}
			}
		} else {
			logger.error("No subdirectory of " + rootPath + " found with name " + datasetName);
		}
        
		// 
		long timeoutForEachTS = WAIT_FOR_IMPORT_TIMEOUT * 1000;
		long dateTimeout = new Date().getTime() + timeoutForEachTS * importTasks.size();
		
		while(true) {
    		// check if there is a task that is not completed
    		boolean allResultsDone = true;
    		for (ImportCallable importCallable : importTasks) {
				if (!importCallable.futureResult.isDone()) {
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
        		for (ImportCallable importCallable : importTasks) {
    				if (!importCallable.futureResult.isDone()) {
    					importCallable.futureResult.cancel(true);
    					break;
    				}
    			}
            	
            	// out of loop
        		break;
        	} else {
        		// sleep a moment before restart the loop again
        		try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// do nothing because there is not impact here
					logger.warn("Interrupted while waiting in loop of importFullDirectory for ImportResults to be available", e);
				}
        	}
		}
				
		
		// Read the results
        ImportResult currentResult = new ImportResult();
    	for (ImportCallable importTask : importTasks) {
    		if (importTask.futureResult.isDone()) {
    			try {
					currentResult = importTask.futureResult.get(WAIT_FOR_IMPORT_TIMEOUT, TimeUnit.SECONDS);
        			if (! currentResult.getErrors().containsKey(ImportCallable.class.getName())) {
						tsuidList.add(currentResult.getTsuid());
						count++;
        			} else {
    	    			errors++;
        				logger.error("Error #" + errors + " import task not done. " + currentResult.getErrors().get(ImportCallable.class.getName()));
        			}
				} catch (InterruptedException e) {
	    			errors++;
					logger.error("Error #" + errors + " interrupted while waiting for task. File: " + importTask.flightFileName);
				} catch (ExecutionException e) {
	    			errors++;
					logger.error("Error #" + errors + " import has an ExecutionException. File: " + importTask.flightFileName);
					logger.debug("ExecutionException: " + e.getMessage());
				} catch (TimeoutException e) {
	    			errors++;
					logger.error("Error #" + errors + " import task Timeout. File: " + importTask.flightFileName);
				}
    		} else {
    			errors++;
				logger.error("Error #" + errors + " import task not done. File: " + importTask.flightFileName);
    		}
		}
        
        if(errors>0) {
            logger.error(errors+" errors encountered, see previous logs for details");
            logger.warn(count + " files imported");
        } else {
            if(logOnly) {
                logger.info(count + " files identified for import");
            } else {
                logger.info("All " + count + " files imported");
            }
        }
        
        chrono.stop(logger);
        return tsuidList;
    }
    
    
    
    /**
     * inner class for the ImportCallable
     */
    class ImportCallable implements Callable<ImportResult> {

        private String datasetName;
        private File rootDir;
        private String airCraftId;
        private String metric;
        private String flightFileName;
        private boolean logOnly;
        
        Future<ImportResult> futureResult;

        /**
         * constructor
         * @param datasetName datasetName the dataset name
         * @param rootDir the root directory
         * @param airCraftId the aircraftId
         * @param metric the metric
         * @param flightFileName the filghtFileName
         * @param logOnly only log
         */
        public ImportCallable(String datasetName, File rootDir, String airCraftId, String metric, String flightFileName, boolean logOnly) {
            super();
            this.datasetName = datasetName;
            this.rootDir = rootDir;
            this.airCraftId = airCraftId;
            this.metric = metric;
            this.flightFileName = flightFileName;
            this.logOnly = logOnly;
        }

        @Override
        public ImportResult call() {

        	ImportResult result = new ImportResult();
        	String filePath= "";
        	
            try {
                filePath= datasetName + "/DAR/" + airCraftId + "/" + metric + "/" + flightFileName;
                if (!logOnly) {
                	logAirbusFile(rootDir, filePath);
                    result = launchImportRequestForAirbusFile(rootDir, filePath);
                }
            }
            catch (Exception exc) {
                logger.error( FAILURE_MSG_PREFIXE + filePath );
                logger.error( "ImportCallable caught exception: ", exc );
                result.addError(ImportCallable.class.getName(), FAILURE_MSG_PREFIXE + filePath + "| Exception: " + exc);
            }
            catch (Error err) {
                logger.error( FAILURE_MSG_PREFIXE + filePath );
                logger.error( "ImportCallable caught error: ", err );
                result.addError(ImportCallable.class.getName(), FAILURE_MSG_PREFIXE + filePath + "| Exception: " + err);
            }
            return result;
        }
    }
 
}
