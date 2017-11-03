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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
 * @author Maxime PERELMUTER <maxime.perelmuter@c-s.fr>
 * 
 */

package fr.cs.ikats.temporaldata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.HttpServer;

import fr.cs.ikats.common.junit.CommonTest;
import fr.cs.ikats.datamanager.client.RequestSender;
import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.datamanager.client.opentsdb.ImportResult;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.temporaldata.application.ApplicationConfiguration;

/**
 * This superclass is designed for the junit tests of IKATS web services: it
 * provides static services initializing the Grizzly server, when activated in
 * the test configuration. Each JUnit test on Web service will inherit from this
 * class. This class is also a subclass of CommonTest which provides the
 * standard logger for IKATS Junbit tests.
 * 
 *
 */
public abstract class AbstractRequestTest extends CommonTest {

    /**
     * In addition to superclass logger (CommonTest::getLogger), this log is
     * needed for static methods
     */
    private static final Logger STATIC_LOGGER = Logger.getLogger(AbstractRequestTest.class);

    /**
     * Enum to chec kthe state of the web server used by the test
     * 
     */
    public enum ServerStatus {
        STOPPED, LAUNCHED, ERROR
    };

    /**
     * Using the serverState, we want to avoid to launch grizzly twice ... or
     * stop twice
     */
    protected static ServerStatus serverState = ServerStatus.STOPPED;

    protected static HttpServer server;
    protected static TestUtils utils;
    protected static ApplicationConfiguration config;
    protected static CompositeConfiguration testConfig;

    /**
     * 
     * @return
     */
    protected static String getAPIURL() {
        return testConfig.getString("testAPIURL");
    }

    /**
     * Static initialization of the JUnit class: calls implSetupBeforClass() and
     * logs
     * 
     * @param junitClassInfo
     */
    public static void setUpBeforClass(String junitClassInfo) {

        try {
            STATIC_LOGGER.setLevel(Level.INFO);
            STATIC_LOGGER.info(DECO_JUNIT_CLASS_LINE + " setUpBeforeClass: " + junitClassInfo + " " + DECO_JUNIT_CLASS_LINE);
            implSetupBeforClass();

        }
        catch (Throwable e) {
            STATIC_LOGGER.error(DECO_JUNIT_CLASS_LINE + "Failure on setUpBeforeClass: " + junitClassInfo + " " + DECO_JUNIT_CLASS_LINE, e);
        }

    }

    /**
     * The implementation manages the test.properties, and the start of grizzly
     * server
     * @throws ConfigurationException 
     * @throws IkatsWebClientException 
     */
    protected static void implSetupBeforClass() throws ConfigurationException, IkatsWebClientException {
        // init test configuration
        String propertiesFile = "test.properties";

        // This part is shared by all the tests
        if (testConfig == null) {

            testConfig = new CompositeConfiguration();
            testConfig.addConfiguration(new SystemConfiguration());
            testConfig.addConfiguration(new PropertiesConfiguration(propertiesFile));
        }

        utils = new TestUtils();

        if (serverState == ServerStatus.STOPPED) {
            try {
                server = ServerMain.startServer(testConfig.getString("grizlyServerURL"));
                STATIC_LOGGER.info(String.format("Jersey app started with WADL available at " + "%sapplication.wadl\nHit enter to stop it...",
                        testConfig.getString("grizlyServerURL")));

                serverState = ServerStatus.LAUNCHED;
            }
            catch (Throwable e) {

                serverState = ServerStatus.ERROR;
                throw new IkatsWebClientException("Failed to lauch Grizzly server => set serverState to ERROR");
            }
        }
        else {
            String prevState = serverState.toString();
            serverState = ServerStatus.ERROR;
            throw new IkatsWebClientException(
                    "Unexpected server state " + prevState + " just before trying to launch Grizzly server => set serverState to ERROR");
        }

        config = new ApplicationConfiguration();
    }

    protected static String getHost() {
        return config.getStringValue(ApplicationConfiguration.HOST_DB_API);
    }

    /**
     * Static ending of the JUnit class: calls implTearDownAfterClass() and logs
     * 
     * @param junitClassInfo
     * @throws IkatsWebClientException 
     */
    public static void tearDownAfterClass(String junitClassInfo) throws IkatsWebClientException {

        STATIC_LOGGER.setLevel(Level.INFO);
        STATIC_LOGGER.info(DECO_JUNIT_CLASS_LINE + " tearDownAfterClass: " + junitClassInfo + " " + DECO_JUNIT_CLASS_LINE);
        implTearDownAfterClass();
    }

    /**
     * Stops the grizzly server if needed
     * @throws IkatsWebClientException 
     * 
     */
    protected static void implTearDownAfterClass() throws IkatsWebClientException {

        if (serverState == ServerStatus.LAUNCHED) {
            try {
                ServerMain.stopServer(server);
                serverState = ServerStatus.STOPPED;
            }
            catch (Throwable e) {

                serverState = ServerStatus.ERROR;
                throw new IkatsWebClientException("JUnit failure: Failed to stop the Grizzly server => set serverState to ERROR");
            }
        }
        else {
            String prevState = serverState.toString();
            serverState = ServerStatus.ERROR;
            throw new IkatsWebClientException("JUnit failure: Unexpected server state " + prevState
                    + " just before trying to stop Grizzly server => set serverState to ERROR");
        }
    }

    /**
     * launch a request for meta data import.
     * 
     * @param tsuid
     * @param name
     * @param value
     */
    protected int launchMetaDataImport(String tsuid, String name, String value) {
        String url = getAPIURL() + "/metadata/import/" + tsuid + "/" + name + "/" + value;
        int httpStatus = 500;
        try {
            Response response = RequestSender.sendPOSTRequest(url, null);
            Integer status = response.getStatus();

            STATIC_LOGGER.info(this.getClass().getSimpleName() + " " + status);
            if (status != null) {
                httpStatus = status.intValue();
            }

        }
        catch (Throwable e) {
            STATIC_LOGGER.error(this.getClass().getSimpleName() + " Error occured: launchMetaDataImport(" + tsuid + "," + name + "," + value + ")",
                    e);
        }
        return httpStatus;
    }

    /**
     * Create if possible every metadata specified for the defined tsuid, and
     * then return a report
     * 
     * @param tsuid
     * @param aircraftid
     *            value of created meta named AircraftIdentifier, and dtyped
     *            string
     * @param flightid
     *            value of created meta named FlightIdentifier, and dtyped
     *            string
     * @param metric
     *            value of created meta named FlightIdentifier, and dtyped
     *            string
     * @param ata
     *            value of created meta named ata, and dtyped string
     * @param big_field
     *            value of created meta named big_field, and dtyped complex
     * @param startdate
     *            long value of created meta named ikats_start_date, and dtyped
     *            date
     * @param enddate
     *            long value of created meta named ikats_end_date, and dtyped
     *            date
     * @param nbpoints
     *            long value of created meta named qual_nb_points, and dtyped
     *            number
     * @param mean
     *            double value of created meta named qual_average_value, and
     *            dtyped number
     * @param var
     *            double value of created meta named qual_variance, and dtyped
     *            number
     * @param verbose
     *            flag must be true in order to log all status (error or info !)
     * @return report is a map of request status claissified by names of created
     *         meta
     */

    protected Map<String, Integer> createMetadataSet(String tsuid, String aircraftid, String flightid, String metric, String ata, String complex,
            long startdate, long enddate, long nbpoints, double mean, double var, boolean verbose) {

        Map<String, Integer> reportMap = new HashMap<String, Integer>();

        reportMap.put("AircraftIdentifier", launchMetaDataImport(tsuid, "AircraftIdentifier", aircraftid, "string", false));

        String context = "prepare meta for " + tsuid;
        Integer ignoredStatus = 200;
        evaluateReport(reportMap, context, ignoredStatus);

        reportMap.put("FlightIdentifier", launchMetaDataImport(tsuid, "FlightIdentifier", flightid, "string", false));

        reportMap.put("metric", launchMetaDataImport(tsuid, "metric", metric, "string", false));

        reportMap.put("ata", launchMetaDataImport(tsuid, "ata", ata, "string", false));

        reportMap.put("big_field", launchMetaDataImport(tsuid, "big_field", complex, "complex", false));

        reportMap.put("ikats_start_date", launchMetaDataImport(tsuid, "ikats_start_date", "" + new Long(startdate), "date", false));

        reportMap.put("ikats_end_date", launchMetaDataImport(tsuid, "ikats_end_date", "" + new Long(enddate), "date", false));

        reportMap.put("qual_nb_points", launchMetaDataImport(tsuid, "qual_nb_points", "" + new Long(nbpoints), "number", false));

        reportMap.put("qual_average_value", launchMetaDataImport(tsuid, "qual_average_value", "" + new Double(mean), "number", false));

        reportMap.put("qual_average_value", launchMetaDataImport(tsuid, "qual_variance", "" + new Double(var), "number", false));

        return reportMap;
    }

    protected void evaluateReport(Map<String, Integer> reportMap, String context, Integer ignoredStatus) {
        for (Map.Entry<String, Integer> status : reportMap.entrySet()) {
            if (!ignoredStatus.equals(status.getValue())) {
                getLogger().error("In context [" + context + "]: report has unexpected status=" + status.getValue() + " instead of " + ignoredStatus
                        + " for " + status.getKey());
            }
        }
    }

    protected int launchMetaDataImport(String tsuid, String name, String value, String dtype, boolean verbose) {
        String url = getAPIURL() + "/metadata/import/" + tsuid + "/" + name + "/" + value + "?dtype=" + dtype;
        int httpStatus = 500;
        try {
            Response response = RequestSender.sendPOSTRequest(url, null);
            Integer status = response.getStatus();

            if (verbose) {
                STATIC_LOGGER.info(this.getClass().getSimpleName() + " " + status);
            }
            if (status != null) {
                httpStatus = status.intValue();
            }

        }
        catch (Throwable e) {
            if (verbose) {
                STATIC_LOGGER
                        .error(this.getClass().getSimpleName() + " Error occured: launchMetaDataImport(" + tsuid + "," + name + "," + value + ")", e);
            }

        }
        return httpStatus;
    }

    /**
     * launch a request for meta data update.
     * 
     * @param tsuid
     * @param name
     * @param value
     */
    protected int launchMetaDataUpdate(String tsuid, String name, String value) {
        String url = getAPIURL() + "/metadata/" + tsuid + "/" + name + "/" + value;

        int httpStatus = 500;
        try {
            Response response = RequestSender.sendPUTRequest(url, Entity.json(new MetaData()));
            if (response != null) {
                Integer status = response.getStatus();
                STATIC_LOGGER.info(this.getClass().getSimpleName() + " " + status);
                if (status != null) {
                    httpStatus = status.intValue();
                }
            }

        }
        catch (Throwable e) {
            STATIC_LOGGER.error(this.getClass().getSimpleName() + " Error occured: launchMetaDataImport(" + tsuid + "," + name + "," + value + ")",
                    e);
        }
        return httpStatus;
    }

    /**
     * Do an import of functional Identifier
     * 
     * @param tsuid
     *            the TSUID
     * @param funcId
     *            the functional identifier
     * @param fails
     *            if it must fail
     * @param NumberOfSuccess
     *            number of success expected
     * @return the response.
     */
    protected Response doFuncIdImport(String tsuid, String funcId, boolean fails, long NumberOfSuccess) {
        String url = getAPIURL() + "/metadata/funcId/" + tsuid + "/" + funcId;
        Response response = null;
        try {
            response = RequestSender.sendPOSTRequest(url, null);
            if (response.getStatus() <= Status.CREATED.getStatusCode()) {
                ImportResult result = response.readEntity(ImportResult.class);
                assertEquals(NumberOfSuccess, result.getNumberOfSuccess());

                STATIC_LOGGER.info(this.getClass().getSimpleName() + " " + response.getStatus());
            }
        }
        catch (Throwable e) {

            if (!fails) {
                STATIC_LOGGER.error(this.getClass().getSimpleName() + " Error: " + url);
                STATIC_LOGGER.error(this.getClass().getSimpleName() + " Error occured: doFuncIdImport(" + tsuid + "," + funcId + ", false)", e);
                fail();
            }
        }
        return response;
    }

    /**
     * Retrieve File matching the resource, or die ...
     * 
     * @param testCaseName
     * @param resourceRelativePath
     *            the relative path of the resource
     * @return the File associated to the resource
     * @throws IOException
     *             failed to retrieve the expected resource
     */
    protected File getFileMatchingResource(String testCaseName, String resourceRelativePath) throws IOException {

        // TODO remonter getFileMatchingResource dans superclasse CommonTest
        // en se passant des classes spring ... source de complications
        // potentielles alors
        // qu'en standard java on sait bien gerer (ResourceBundle ...)
        org.springframework.core.io.Resource resource = new org.springframework.core.io.ClassPathResource(resourceRelativePath);

        File file = null;
        try {
            file = resource.getFile();

        }
        catch (IOException e1) {
            getLogger().error("Error in: " + testCaseName + ": getting File for resource" + resourceRelativePath, e1);
            throw e1;
        }
        return file;
    }
   
}

