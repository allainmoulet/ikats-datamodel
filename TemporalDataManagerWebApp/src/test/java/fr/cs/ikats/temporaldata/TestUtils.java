package fr.cs.ikats.temporaldata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.internal.util.collection.StringKeyIgnoreCaseMultivaluedMap;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition.FormDataContentDispositionBuilder;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.json.simple.JSONObject;
import org.mockito.Mockito;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.fasterxml.jackson.databind.util.ISO8601DateFormat;

import fr.cs.ikats.datamanager.DataManagerException;
import fr.cs.ikats.datamanager.client.opentsdb.ApiResponse;
import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.datamanager.client.opentsdb.ImportResult;
import fr.cs.ikats.temporaldata.business.TemporalDataManager;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.ImportException;
import fr.cs.ikats.temporaldata.exception.ImportExceptionHandler;
import fr.cs.ikats.temporaldata.resource.TimeSerieResource;

public class TestUtils {

	private static final Logger LOGGER = Logger.getLogger(TestUtils.class);
	final public static DateFormat AIRBUS_RAW_CSV_DATA_FORMAT = getDateFormat();
	static String metriquesInt[] = { "poc3.arctan", "poc3.cosinus", "poc3.sinusoide" };
	static String metriquesDev[] = { "poc.cosinus.lineaire", "poc3.cosinus", "poc3.sinusoide" };

	static String TAG1_K = "tag1";
	static String TAG1_V = "val1";
	static String TAG2_K = "tag2";
	static String TAG2_V = Long.toString(System.currentTimeMillis());

	Random myRandom;

	final private static DateFormat getDateFormat() {
		return new DateFormat() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 7495728837748985475L;

			@Override
			public Date parse(String source, ParsePosition pos) {
				Date date = null;
				try {
					DateFormat format = new ISO8601DateFormat();
					date = format.parse(source, pos);
					if (date == null) {
						format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
						date = format.parse(source, pos);
					}
					if (date == null) {
						format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
						date = format.parse(source, pos);
					}
					if (date == null) {
						format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S");
						date = format.parse(source, pos);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				return date;
			}

			@Override
			public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
				DateFormat format = new ISO8601DateFormat();
				return format.format(date, toAppendTo, fieldPosition);
			}
		};
	}

	public TestUtils() {
		myRandom = new Random();
	}

	/**
	 * Prepare File instance for a test case.
	 * @param testCaseName name of the test case needing the resource (used in cas of error)
	 * @param resourcePath path of test file
	 * @return
	 * @throws IOException error getting the resource
	 */
	public File getTestFile(String testCaseName, String resourcePath) throws IOException {
		Resource resource = new ClassPathResource(resourcePath);
		File file = null;
		try {
			file = resource.getFile();
		} catch (IOException e1) {
			LOGGER.error("Error in: " + testCaseName + ": getting File for resource /data/test_import.csv",
					e1);
			throw e1;
		}
		return file;
	}


	public Client getClientWithJSONFeature() {
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.register(MultiPartFeature.class).register(JacksonFeature.class);
		clientConfig.register(LoggingFilter.class);
		JerseyClient client = JerseyClientBuilder.createClient(clientConfig);
		return client;
	}
 
	/**
	 * send GET request with specific media-type encoding and specific Client.
	 * Note: sendGETRequest with default client or/and default media-type are simpler.
	 * @param mediaType
	 * @param client
	 * @param url
	 * @return
	 * @throws IkatsWebClientException
	 */
	public Response sendGETRequest(String mediaType, Client client, String url)
			throws IkatsWebClientException { 
		LOGGER.debug("Sending GET request to url : " + url);
		Response response = null;
		try {
			WebTarget target = client.target(url);
			if (mediaType != null) {
				response = target.request(mediaType).get();
			} else {
				response = target.request().get();
			}
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return response;
	}

	/**
	 * Send GET resquest with specific media-type encoding, and default client.
	 * @param mediaType
	 * @param url
	 * @return
	 * @throws IkatsWebClientException
	 */
	public Response sendGETRequest(String mediaType, String url)
			throws IkatsWebClientException { 
		Client client = getClientWithJSONFeature();
		LOGGER.debug("Sending GET request to url : " + url);
		Response response = null;
		try {
			WebTarget target = client.target(url);
			if (mediaType != null) {
				response = target.request(mediaType).get();
			} else {
				response = target.request().get();
			}
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return response;
	}
	
	/**
	 * Send GET resquest with  default media-type and default client.
	 * @param mediaType
	 * @param url
	 * @return
	 * @throws IkatsWebClientException
	 */
	public Response sendGETRequest(String url)
			throws IkatsWebClientException { 	 
		return sendGETRequest(null, url);
	}
	
	public Response sendDeleteRequest(Client client, String url) throws IkatsWebClientException {
		LOGGER.debug("Sending DELETE request to url : " + url);
		Response response = null;
		try {
			WebTarget target = client.target(url);
			response = target.request().delete();
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return response;
	}

	/**
	 * send GET request and return JSON format response
	 * 
	 * @param url
	 * @param host
	 * @return
	 * @throws IkatsWebClientException
	 */
	public Response sendPUTRequest(Entity<?> entity, String mediaType, Client client, String url, String host)
			throws IkatsWebClientException {
		LOGGER.debug("Sending PUT request to url : " + url);
		Response response = null;
		try {
			WebTarget target = client.target(url);
			if (mediaType != null) {
				response = target.request(mediaType).put(entity);
			} else {
				response = target.request().put(entity);
			}
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return response;
	}
 
	/**
	 * @see TestUtils#doLaunch(File, String, boolean, int, boolean) with addFuncId set to true by default
	 */
	protected ImportResult doImport(File file, String url, boolean withTags, int statusExpected) {
		return doImport(file, url, withTags, statusExpected, true);
	}

	/**
	 * @param file
	 *            : the file to import
	 * @param url
	 *            : url to reach
	 * @param withTags
	 *            : add the tags
	 * @param statusExpected
	 *            : the expected return status
	 * @param addFuncId
	 *            : true to add the funcId part into request
	 */
	protected ImportResult doImport(File file, String url, boolean withTags, int statusExpected, boolean addFuncId) {
		Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).register(JacksonFeature.class)
				.build();
		WebTarget target = client.target(url);

		// build form param
		final FormDataMultiPart multipart = new FormDataMultiPart();

		FileDataBodyPart fileBodyPart = new FileDataBodyPart("file", file);
		multipart.bodyPart(fileBodyPart);
		if (withTags) {
			multipart.field(TAG1_K, TAG1_V);
			multipart.field(TAG2_K, TAG2_V);
		}
		if (addFuncId) {
			// add the multipart field funcId, required
			multipart.field("funcId", "FuncID_" + Long.toString(System.currentTimeMillis()));
		}
		LOGGER.info("sending url : " + url);
		Response response = target.request().post(Entity.entity(multipart, multipart.getMediaType()));
		LOGGER.info("parsing response of " + url);
		LOGGER.info(response);
		int status = response.getStatus();
		ImportResult result = response.readEntity(ImportResult.class);
		LOGGER.info(result);
		// check expected status
		assertEquals(statusExpected, status);

		return result;
	}

	/**
	 * Tests the TS import service: TimeSerieResource::importTSLocal()/importTSFromHTTP() according to url.
	 * <br/>
	 * This test is stubbing the opentsdb underlying services:
	 * <ul>
	 * <li>see {@link TemporalDataManager#launchImportTasks(String, java.io.InputStream, List, java.util.Map, String)}
	 * </li>
	 * <li>see {@link TemporalDataManager#parseImportResults(String, List, java.util.Map, Long, Long)}</li>
	 * </ul>
	 * 
	 * @param file
	 *            : the file to import (used by request encoding, effective content is ignored)
	 * @param url
	 *            : url of the HTTP request matching the importTSLocal() or importTSFromHTTP()
	 * @param tsuidStubbed
	 *            : stubbed tsuid, as it is not really created by opentsdb: used by the metadata creation
	 * @param withTags
	 *            : add the tags: true will ass tags
	 * @param statusExpected
	 *            : the expected return status: tested Http response status
	 * @param addFuncId
	 *            : true to add the funcId part into request (nominal test), or false (test is then KO)
	 * @throws IOException
	 * @throws DataManagerException
	 * @throws ImportException
	 * @throws IkatsWebClientException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	protected ImportResult doImportStubbedOpenTSDB(File file, String url, String tsuidStubbed, boolean withTags,
			int statusExpected, boolean addFuncId) throws ImportException, DataManagerException, IOException,
			InterruptedException, ExecutionException, IkatsWebClientException {
		
		// Purpose of doImportStubbedOpenTSDB is to stub theses 2 lines:
		//
		// long[] dates =getTemporalDataManager().launchImportTasks(metric, tsStream, resultats, tags, filename);
		// importResult =getTemporalDataManager().parseImportResults(metric, resultats, tags, dates[0], dates[1]);

		// stubbed funcId whether generated or not
		String testedFuncId = null;
		// stubbed dates
		long startDate = 100;
		long endDate = 2000;
		long[] stubbedDates = { startDate, endDate };

		// metric as defined by url
		String metric = url.substring(url.lastIndexOf('/')+1);

		// Stub the TemporalDataManager, injected into AbstractResource::getTemporalDataManager()
		// => the aim is to stub services writting into opentsb
		TemporalDataManager mockedTdm = Mockito.spy(TemporalDataManager.class);

		// stubbed: launchImportTasks
		doReturn(stubbedDates).when(mockedTdm).launchImportTasks(Mockito.anyString(), Mockito.any(), Mockito.any(),
				Mockito.any(), Mockito.any());

		// stubbed result from: mockedTdm.parseImportResults()
		ImportResult stubbedImportResult = new ImportResult();
		if (addFuncId) {
			testedFuncId = "FuncID_" + Long.toString(System.currentTimeMillis());
			stubbedImportResult.setStartDate(startDate);
			stubbedImportResult.setEndDate(endDate);
			stubbedImportResult.setTsuid(tsuidStubbed);
			stubbedImportResult.setFuncId(testedFuncId);
			stubbedImportResult.setNumberOfFailed(0);
			stubbedImportResult.setNumberOfSuccess(1);
		} else {
			// Simulate the error when funcId is not provided ...
			stubbedImportResult.setNumberOfFailed(1);
			stubbedImportResult.setNumberOfSuccess(0);
			stubbedImportResult.addError("ErrKey_stub_missing_FuncId", "Stubbed error: missing funcId");
		}
		doReturn(stubbedImportResult).when(mockedTdm).parseImportResults(Mockito.anyString(), Mockito.any(),
				Mockito.any(), Mockito.any(), Mockito.any());
 
		// Preparing the request
		//
		 
		// TODO 163211 or later: Finally: inject mockedTdm into the TimeSerieResource, 
		// in the webapp tested by grizzly client (requires extra works setting up the test, injecting the mock ...)
        //
		//		Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).register(JacksonFeature.class)
        //		.build();
        //      WebTarget target = client.target(url);
		//		LOGGER.info("sending url : " + url);
		//		Response response = target.request().post(Entity.entity(multipart, multipart.getMediaType()));
		//		LOGGER.info("parsing response of " + url);
		//		LOGGER.info(response);
		//      int status = response.getStatus();
		//      ImportResult result = response.readEntity(ImportResult.class);
		//
		// => done presently: test directly on TimeSerieRessource
		
		TimeSerieResource testedResource = new TimeSerieResource();
        testedResource.setTemporalDataManager( mockedTdm );		
        ApiResponse apiResponse = null;
        int status = -1;
        ImportResult result = null;
        try {
        	if ( url.indexOf("ts/put" ) >= 0 )
     		{
        		// build form param
        		final FormDataMultiPart multipart = new FormDataMultiPart();
        	 

        		FileDataBodyPart fileBodyPart = new FileDataBodyPart("file", file);
        		multipart.bodyPart(fileBodyPart);
        		if (withTags) {
        			multipart.field(TAG1_K, TAG1_V);
        			multipart.field(TAG2_K, TAG2_V);
        		}
        		if (addFuncId) {
        			// add the multipart field funcId, required
        			multipart.field("funcId", testedFuncId);
        		}
        		
             	FormDataContentDispositionBuilder builder = FormDataContentDisposition.name("doImportStubbedOpenTSDB");
     			FormDataContentDisposition formDataContentDisposition = 
     					builder.fileName(file.getName()).creationDate(new Date()).build();
     			 apiResponse = testedResource.importTSFromHTTP(metric, 
     					 									   new FileInputStream(file), 
     					 									   formDataContentDisposition, 
     					 									   multipart,  
     					 									   null);
     		}
     		else
     		{
     			// build the MultivaluedMap param ...
     			MultivaluedMap<String, String> tags = new StringKeyIgnoreCaseMultivaluedMap<String>();
     			if (withTags) {
     			tags.add(TAG1_K, TAG1_V);
     	        tags.add(TAG2_K, TAG2_V);
     			}
     			if ( addFuncId ) 
     			{
     				tags.add( "funcId", testedFuncId);
     			}
     			apiResponse = testedResource.importTSLocal(metric, 
     					 								   file.getCanonicalPath(), 
     					 								   testedFuncId, 
     					 								   tags, null);
     		}
        	// Tests that expected nominal result is ImportResult
        	assertTrue( apiResponse instanceof ImportResult );
        	result = (ImportResult) apiResponse;
        	
        	// TODO 163211 or later: status should be read in Response
        	status = 200;
        	
		} catch (ImportException e) {
			//  TODO 163211 or later: status should be read in Response
			ImportExceptionHandler errorHandler = new ImportExceptionHandler();
			Response resp = errorHandler.toResponse(e);
			status = resp.getStatus();
			Object resultObj = resp.getEntity();
			assertTrue( resultObj == null || resultObj instanceof ImportResult );
		    result = (ImportResult) resultObj;
		}
        
		LOGGER.info(result);
		// check expected status
		assertEquals(statusExpected, status);

		return result;
	}

	/**
	 * 
	 * @param tsuid
	 *            the tsuid
	 * @param startDate
	 *            the start date
	 * @param endDate
	 *            the end date
	 * @return the JSON representation of TS data
	 * @throws Exception
	 *             if error occurs
	 */
	public String getTSFromFile(String filePath, String startDate, String endDate, String metric) throws Exception {
		String response;
		JSONObject jo = new JSONObject();
		jo.put("metric", metric);
		jo.put("tags", "");
		jo.put("aggregateTags", "");
		jo.put("dps", ExtractCSVDataToJson(filePath, startDate, endDate));
		response = "[" + jo.toJSONString() + "]";
		return response;
	}

	public final static JSONObject ExtractCSVDataToJson(String filePath, String startDate, String endDate)
			throws IkatsException {

		Long start = Long.valueOf(startDate).longValue();
		Long end = Long.valueOf(endDate).longValue();
		JSONObject dps = new JSONObject();
		BufferedReader reader = null;

		try {
			reader = new BufferedReader(new FileReader(filePath));

			// read the first line because it is the header line
			String currentLine = reader.readLine();
			long lineNumber = 1;

			Long currentDate = -1l;
			boolean out_of_range = false;

			// Number will be either Long or Double ... according to the parsed
			// value
			Number currentValue = null;

			long nbPointsEvaluated = 0;
			long nbsuccess = 0;

			while (currentLine != null) {
				boolean hasParsedCurrentPointWithSuccess = true;
				currentLine = reader.readLine();
				lineNumber++;
				if ((currentLine != null) && (currentLine.trim().length() > 0)) {
					nbPointsEvaluated++;
					StringTokenizer fields = new StringTokenizer(currentLine, ";");
					String rawDate = fields.nextToken();
					String rawValue = fields.nextToken();

					// Parse the date ...
					try {
						currentDate = AIRBUS_RAW_CSV_DATA_FORMAT.parse(rawDate).getTime();
						if ((currentDate < start) || (currentDate > end)) {
							out_of_range = true;
						} else {
							out_of_range = false;
						}
					} catch (Throwable e) {
						String msg = "Error parsing date field from CSV at line=" + lineNumber + " date=" + rawDate;
						hasParsedCurrentPointWithSuccess = false;
						LOGGER.error(msg, e);
					}

					// Parse the value: either a long or a double ...
					// => deal with Long
					try {
						// pass previous value currentValue to the method: will
						// determine the parsed type after
						// the first point
						currentValue = parseValue(rawValue, currentValue);
					} catch (IkatsException e) {
						String msg = "Error parsing value field from CSV at line=" + lineNumber + " value=" + rawValue;
						hasParsedCurrentPointWithSuccess = false;
						LOGGER.error(msg, e);
					}

					// update statistics

					if (hasParsedCurrentPointWithSuccess) {
						nbsuccess++; // nb of points successfully parsed
						if (!out_of_range) {
							String currentTimeStamp = currentDate.toString();
							dps.put(currentTimeStamp, currentValue);
						}
					}
				} else if (currentLine == null) // EOF only
				{
					LOGGER.info("<<<< extracted CSV: filename=" + filePath);
					LOGGER.info("  - nb lines=" + lineNumber);
					LOGGER.info("  - evaluated points=" + nbPointsEvaluated);
					LOGGER.info("  - nb point successfully parsed=" + nbsuccess);
					LOGGER.info("  - nb point kept (in range)=" + dps.size());
					LOGGER.info("extracted CSV: filename=" + filePath + ">>>>");
					if (dps.size() == 0) {
						dps.put("WARNING", "--- aucune donnee extraite ---");
					}
				}
			}
		} catch (Throwable e) {
			LOGGER.error("Caught throwable while parsing CSV", e);
			dps.put("error", e.getMessage());
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (Throwable e) {
					// Do not try more
				}
			}
		}

		return dps;
	}

	private static Number parseValue(String rawValue, Number previousValue) throws IkatsException {
		Number parsedValue = null;
		if (previousValue == null) {
			try {
				// attempt from Long
				parsedValue = Long.parseLong(rawValue);

			} catch (NumberFormatException e) {
				try {
					// attempt from Double
					parsedValue = Double.parseDouble(rawValue);
				} catch (NumberFormatException e2) {
					throw new IkatsException("Failed to parse the value from CSV: first value");
				}
			}
		} else // once previousValue is defined: keep same type of Number
		{
			try {
				if (previousValue instanceof Long) {
					parsedValue = Long.parseLong(rawValue);
				} else if (previousValue instanceof Double) {
					parsedValue = Double.parseDouble(rawValue);
				} else {
					throw new IkatsException(
							"Failed to parse the value from CSV: unexpected instance of previousValue");
				}
			} catch (NumberFormatException e) {
				throw new IkatsException("Failed to parse the value from CSV: after first value");
			}
		}
		return parsedValue;
	}
}
