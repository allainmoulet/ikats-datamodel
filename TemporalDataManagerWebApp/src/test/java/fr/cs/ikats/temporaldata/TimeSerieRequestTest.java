/**
 * $Id$
 * @author ikats
 */
package fr.cs.ikats.temporaldata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.glassfish.jersey.client.ClientConfig;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.expr.SingleValueComparator;
import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.metadata.model.MetadataCriterion;
import fr.cs.ikats.metadata.model.MetaData.MetaType;
import fr.cs.ikats.temporaldata.business.DataSetManager;
import fr.cs.ikats.temporaldata.business.FilterOnTsWithMetadata;
import fr.cs.ikats.temporaldata.business.MetaDataManager;
import fr.cs.ikats.temporaldata.business.TSInfo;

/**
 * Test on webService timeseries operations.
 */
public class TimeSerieRequestTest extends AbstractRequestTest {

    private static Logger logger = Logger.getLogger(TimeSerieRequestTest.class);

    @BeforeClass
    public static void setUpBeforClass() {
        AbstractRequestTest.setUpBeforClass("TimeSerieRequestTest");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        AbstractRequestTest.tearDownAfterClass("TimeSerieRequestTest");
    }

    @Test
    public void testGetAllTS() {

        String testCaseName = "testGetAllTS";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            String url = getAPIURL() + "/ts/tsuid";
            Response response = null;
            logger.info(url);

            response = utils.sendGetRequest(url, null);
            if (response.getStatus() <= 200) {
                GenericType<List<TSInfo>> type = new GenericType<List<TSInfo>>() {
                };
                List<TSInfo> result = response.readEntity(type);

                // There should be results
                assertTrue(result.size() > 0);
                assertTrue(result.get(0).getTsuid() != "");
                assertTrue(result.get(0).getMetric() != "");

            }
            else {
                throw new IkatsWebClientException();
            }

            endNominal(testCaseName);
        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    @Test
    public void testDeleteTS() {
        String testCaseName = "testDeleteTS";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            Resource resource = new ClassPathResource("/data/test_import.csv");
            File file = null;
            try {
                file = resource.getFile();
            }
            catch (IOException e1) {
                e1.printStackTrace();
            }

            /* import of a timeseries */
            String metric = "testmetric";
            String url = getAPIURL() + "/ts/put/" + metric;
            String retour = utils.doImport(file, url, true, 200);

            /* retrieval of the tsuid from import task */
            String tsuid = retour.split(":")[3].split("\"")[1];

            url = getAPIURL() + "/ts/" + tsuid;
            Response response = null;
            logger.info(url);
            Client client = utils.getClientWithJSONFeature();

            // delete request for tsuid
            response = utils.sendDeleteRequest(client, url);
            assertEquals(204, response.getStatus());

            endNominal(testCaseName);
        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    @Test
    public void testgetDsTimeSeries() {

        String testCaseName = "testgetDsTimeSeries";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            // create tsuids
            MetaDataManager metadataManager = new MetaDataManager();
            metadataManager.persistFunctionalIdentifier("tsuid1", "funcId_1");
            metadataManager.persistFunctionalIdentifier("tsuid2", "funcId_2");
            metadataManager.persistFunctionalIdentifier("tsuid3", "funcId_3");
            metadataManager.persistFunctionalIdentifier("tsuid4", "funcId_4");

            // create datasets
            DataSetManager datasetManager = new DataSetManager();
            List<String> tsuids = new ArrayList<String>();
            tsuids.add("tsuid1");
            tsuids.add("tsuid2");
            datasetManager.persistDataSet("dataSet_11", "test", tsuids);
            tsuids.add("tsuid3");
            datasetManager.persistDataSet("dataSet_22", "test", tsuids);

            // expected code 200 : ok with two datasets
            String url = getAPIURL() + "/ts/tsuid2/ds";
            Response response = null;
            logger.info(url);

            String fake_host = "null";
            response = utils.sendGetRequest(url, fake_host);
            logger.info(response);
            assertEquals(200, response.getStatus());
            assertEquals("[dataSet_11, dataSet_22]", response.readEntity(String.class));

            // expected code 200 : ok with one dataset
            url = getAPIURL() + "/ts/tsuid3/ds";
            response = null;
            logger.info(url);

            response = utils.sendGetRequest(url, fake_host);
            assertEquals(response.getStatus(), 200);
            assertEquals(response.readEntity(String.class), "[dataSet_22]");

            // expected code 204 : no dataset found
            url = getAPIURL() + "/ts/tsuid4/ds";
            response = null;
            logger.info(url);

            response = utils.sendGetRequest(url, fake_host);
            assertEquals(response.getStatus(), 204);

            // expected code 404 : tsuid not found
            url = getAPIURL() + "/ts/tsuid5/ds";
            response = null;
            logger.info(url);

            response = utils.sendGetRequest(url, fake_host);
            assertEquals(response.getStatus(), 404);

            endNominal(testCaseName);
        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }

    }
    
    @Test
    public void testUtilsGetTSFromFile() throws IkatsDaoConflictException, IkatsDaoException {

        String testCaseName = "testUtilsGetTSFromFile";
        boolean isNominal = true; // does not throw exception

        try {
            start(testCaseName, isNominal);

            File file = getFileMatchingResource(testCaseName, "/data/test_import.csv");

            logger.info("CSV input file : " + file.getAbsolutePath());
                        
            String startDate = "0";
            String endDate = "1463490182112";
            String metric = "testmetric";
            String reponse = utils.getTSFromFile(file.toString(), startDate, endDate, metric);

            JSONParser parser = new JSONParser();
            JSONObject resultObject =  (JSONObject) ((ArrayList) parser.parse(reponse)).get(0);
            JSONObject data = (JSONObject) resultObject.get("dps");
            String metricResult = (String) resultObject.get("metric");

            assertEquals(metric, metricResult);
            assertEquals(25, data.size());
            
            //logger.info(reponse.readEntity(String.class));

            endNominal(testCaseName);

        } 
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }

    }

    @Test
    public void testSearchTsMatchingMetadataCriteria() {
        String testCaseName = "testSearchTsMatchingMetadataCriteria";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            long startdate = 1444442242424l;
            long enddate = 1444442242499l;
            int nbpoints = 4000;
            double mean = 10.5;
            double var = 0.8;

            Map<String, Integer> report;

            String ts1 = "ts1_" + testCaseName;
            report = createMetadataSet(ts1, "Airc1", "1111", "TuParam1", "ata1", "complex1", startdate, enddate, nbpoints, mean, var, false);
            evaluateReport(report, "create meta for " + ts1, 200);

            String ts2 = "ts2_" + testCaseName;
            report = createMetadataSet(ts2, "Airc1", "2222", "TuParam2", "ata1", "complex2", startdate + 5, enddate - 10, nbpoints + 10, mean + 1,
                    var + 0.1, false);
            evaluateReport(report, "create meta for " + ts2, 200);

            String ts3 = "ts3_" + testCaseName;
            report = createMetadataSet(ts3, "Airc1", "3333", "TuParam3", "ata2", "complex3", startdate + 10, enddate, nbpoints * 2, mean, var, false);
            evaluateReport(report, "create meta for " + ts3, 200);

            String ts4 = "ts4_" + testCaseName;
            report = createMetadataSet(ts4, "Airc2", "4444", "TuParam3", "ata2", "complex4", startdate + 10, enddate, nbpoints * 2, mean + 4, var,
                    false);
            evaluateReport(report, "create meta for " + ts4, 200);

            String url = getAPIURL() + "/ts";

            FilterOnTsWithMetadata lFilter = new FilterOnTsWithMetadata();
            ArrayList<FunctionalIdentifier> scope = new ArrayList<FunctionalIdentifier>();
            scope.add(new FunctionalIdentifier(ts1, "f_" + ts1));
            scope.add(new FunctionalIdentifier(ts2, "f_" + ts2));
            scope.add(new FunctionalIdentifier(ts4, "f_" + ts4));
            scope.add(new FunctionalIdentifier(ts3, "f_" + ts3));

            lFilter.setTsList(scope);

            // discarding the ts4 : mean <= 11.5
            MetadataCriterion critNumber = new MetadataCriterion("qual_average_value", SingleValueComparator.LE.getText(), "" + (mean + 1));

            // discarding ts1 : start >= startdate + 5
            MetadataCriterion critDate = new MetadataCriterion("ikats_start_date", SingleValueComparator.GE.getText(), "" + (startdate + 5));

            // discarding ts2: FlightIdentifier like '%3%'
            MetadataCriterion critString = new MetadataCriterion("FlightIdentifier", SingleValueComparator.LIKE.getText(), "%3%");

            // => should return ts3
            FunctionalIdentifier[] expectedFuncIdFound = new FunctionalIdentifier[] { new FunctionalIdentifier(ts3, "f_" + ts3) };

            // POST resquest ...
            ArrayList<MetadataCriterion> listCrit = new ArrayList<MetadataCriterion>();
            listCrit.add(critNumber);
            listCrit.add(critDate);
            listCrit.add(critString);
            lFilter.setCriteria(listCrit);

            getLogger().info(testCaseName + " : Sending POST request to url : " + url + " with ArrayList<FunctionalIdentifier> ...");

            ClientConfig clientConfig = new ClientConfig();
            Client client = ClientBuilder.newClient(clientConfig);
            Response response = null;

            WebTarget target = client.target(url);

            Entity<FilterOnTsWithMetadata> lEntityFilter = Entity.entity(lFilter, MediaType.APPLICATION_JSON_TYPE);
            Builder reqBuilder = target.request();
            response = reqBuilder.post(lEntityFilter);

            getLogger().info(testCaseName + " response status= " + response.getStatus());
            assertEquals(200, response.getStatus());
            List<FunctionalIdentifier> res = response.readEntity(new GenericType<List<FunctionalIdentifier>>() {
            });

            assertTrue("Not empty", (res != null) && (res.size() > 0));

            for (FunctionalIdentifier functionalIdentifier : res) {
                getLogger().info(testCaseName + " : result item=" + functionalIdentifier);
            }
            getLogger().info(  MetaType.string.name() );
            getLogger().info(  MetaType.date.name() );
            getLogger().info(  MetaType.number.name() );
            getLogger().info(  MetaType.complex.name() );

            // check expected result
            assertEquals(expectedFuncIdFound.length, res.size());
            assertEquals(expectedFuncIdFound[0].getTsuid(), res.get(0).getTsuid());
            assertEquals(expectedFuncIdFound[0].getFuncId(), res.get(0).getFuncId());

            endNominal(testCaseName);
        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }

    }
    
    @Test
    public void testSearchTsMatchingMetadataCriteria_2() {
        String testCaseName = "testSearchTsMatchingMetadataCriteria_2";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            long startdate = 1444442242424l;
            long enddate = 1444442242499l;
            int nbpoints = 4000;
            double mean = 10.5;
            double var = 0.8;

            Map<String, Integer> report;

            String ts1 = "ts1_" + testCaseName;
            report = createMetadataSet(ts1, "Airc1", "1111", "TuParam1", "ata1", "complex1", startdate, enddate, nbpoints, mean, var, false);
            evaluateReport(report, "create meta for " + ts1, 200);

            String ts2 = "ts2_" + testCaseName;
            report = createMetadataSet(ts2, "Airc1", "2222", "TuParam2", "ata1", "complex2", startdate + 5, enddate - 10, nbpoints + 10, mean + 1,
                    var + 0.1, false);
            evaluateReport(report, "create meta for " + ts2, 200);

            String ts3 = "ts3_" + testCaseName;
            report = createMetadataSet(ts3, "Airc1", "3333", "TuParam3", "ata2", "complex3", startdate + 10, enddate, nbpoints * 2, mean, var, false);
            evaluateReport(report, "create meta for " + ts3, 200);

            String ts4 = "ts4_" + testCaseName;
            report = createMetadataSet(ts4, "Airc2", "2222", "TuParam3", "ata2", "complex4", startdate + 10, enddate, nbpoints * 2, mean + 4, var,
                    false);
            evaluateReport(report, "create meta for " + ts4, 200);

            String url = getAPIURL() + "/ts";

            FilterOnTsWithMetadata lFilter = new FilterOnTsWithMetadata();
            ArrayList<FunctionalIdentifier> scope = new ArrayList<FunctionalIdentifier>();
            scope.add(new FunctionalIdentifier(ts1, "f_" + ts1));
            scope.add(new FunctionalIdentifier(ts2, "f_" + ts2));
            scope.add(new FunctionalIdentifier(ts4, "f_" + ts4));
            scope.add(new FunctionalIdentifier(ts3, "f_" + ts3));

            lFilter.setTsList(scope);
            
            // discarding ts3 + ts4: start <> startdate + 10
            MetadataCriterion critDate = new MetadataCriterion("ikats_start_date", SingleValueComparator.NEQUAL.getText(), "" + (startdate + 10));

            // keep ts1: start = startdate
            MetadataCriterion critDate2 = new MetadataCriterion("ikats_start_date", SingleValueComparator.EQUAL.getText(), "" + (startdate));

            // discarding ts2: FlightIdentifier = '1111'
            MetadataCriterion critString = new MetadataCriterion("FlightIdentifier", SingleValueComparator.EQUAL.getText(), "1111");

            // => should return ts1 
            FunctionalIdentifier[] expectedFuncIdFound = new FunctionalIdentifier[] { new FunctionalIdentifier(ts1, "f_" + ts1) };

            // POST resquest ...
            ArrayList<MetadataCriterion> listCrit = new ArrayList<MetadataCriterion>();
            listCrit.add(critDate2);
            listCrit.add(critDate);
            listCrit.add(critString);
            lFilter.setCriteria(listCrit);

            getLogger().debug(testCaseName + " : Sending POST request to url : " + url + "with ArrayList<FunctionalIdentifier> ...");

            ClientConfig clientConfig = new ClientConfig();
            Client client = ClientBuilder.newClient(clientConfig);
            Response response = null;

            WebTarget target = client.target(url);

            Entity<FilterOnTsWithMetadata> lEntityFilter = Entity.entity(lFilter, MediaType.APPLICATION_JSON_TYPE);
            Builder reqBuilder = target.request();
            response = reqBuilder.post(lEntityFilter);

            getLogger().info(testCaseName + " response status= " + response.getStatus());
            assertEquals(200, response.getStatus());
            List<FunctionalIdentifier> res = response.readEntity(new GenericType<List<FunctionalIdentifier>>() {
            });

            assertTrue("Not empty", (res != null) && (res.size() > 0));

            for (FunctionalIdentifier functionalIdentifier : res) {
                getLogger().info(testCaseName + " : result item=" + functionalIdentifier);
            }
            
            // check expected result
            assertEquals(expectedFuncIdFound.length, res.size());             
            assertEquals(expectedFuncIdFound[0].getTsuid(), res.get(0).getTsuid());
            assertEquals(expectedFuncIdFound[0].getFuncId(), res.get(0).getFuncId());
 
            endNominal(testCaseName);
        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }

    }
    
}
