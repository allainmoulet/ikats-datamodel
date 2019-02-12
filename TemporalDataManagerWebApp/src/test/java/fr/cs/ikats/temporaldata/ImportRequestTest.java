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

package fr.cs.ikats.temporaldata;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import fr.cs.ikats.datamanager.client.opentsdb.ImportResult;

import static org.junit.Assert.assertEquals;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ImportRequestTest extends AbstractRequestTest {

    @BeforeClass
    public static void setUpBeforClass() {
        AbstractRequestTest.setUpBeforClass(ImportRequestTest.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        AbstractRequestTest.tearDownAfterClass(ImportRequestTest.class.getSimpleName());
    }

    /**
     * Test import of a TS from a csv simple file
     */
    @Test
    public void importTSWithDataSetAndTags_DG() throws Exception {
        String testCaseName = "importTSWithDataSetAndTags_DG";
        File file = utils.getTestFile(testCaseName, "/data/test_import_bad_format.csv");

        getLogger().info("CSV input file : " + file.getAbsolutePath());
        String metric = "testmetric";
        String url = getAPIURL() + "/ts/put/" + metric;
        utils.doImport(file, url, true, 400);
    }

    /**
     * Test import of a TS from a csv simple file
     */
    @Test
    public void importTSWithDataSetAndTags() throws Exception {
        String testCaseName = "importTSWithDataSetAndTags";
        File file = utils.getTestFile(testCaseName, "/data/test_import.csv");

        getLogger().info("CSV input file : " + file.getAbsolutePath());
        String metric = "testmetric";
        String url = getAPIURL() + "/ts/put/" + metric;

        utils.doImportStubbedOpenTSDB(file, url, testCaseName, true, 200, true);
    }

    /**
     * Test import of a TS - check of start/end date calculation and DB saving
     * UPDATE agn 04/25: also check metric and tags
     */
    @Test
    public void checkMetadataCompletenessOfImport() throws Exception {

        String testCaseName = "checkMetadataCompletenessOfImport";
        File file = utils.getTestFile(testCaseName, "/data/test_import.csv");

        /* import of the timeseries */
        getLogger().info("CSV input file : " + file.getAbsolutePath());
        String metric = "testmetric";
        String url = getAPIURL() + "/ts/put/" + metric;
        ImportResult resImport = utils.doImportStubbedOpenTSDB(file, url, testCaseName, true, 200, true);

        /*
         * retrieval of the tsuid, start_date and end_date from import task
         */
        String tsuid = resImport.getTsuid();
        getLogger().info("tsuid = " + tsuid);
        String startDateImport = Long.toString(resImport.getStartDate());
        getLogger().info("start date from import = " + startDateImport);
        String endDateImport = Long.toString(resImport.getEndDate());
        getLogger().info("end date from import = " + endDateImport);

        /* getting the metadata of the tsuid in database */
        url = getAPIURL() + "/metadata/list/json?tsuid=" + tsuid;
        Client client = utils.getClientWithJSONFeature();
        WebTarget target = client.target(url);

        Response response = target.request().get();
        getLogger().info("parsing response of " + target.getUri());

        /* retrieving start/end dates from response */
        @SuppressWarnings("unchecked")
        ArrayList<HashMap<String, String>> result = (ArrayList<HashMap<String, String>>) response
                .readEntity(ArrayList.class);
        String startDateMeta = "";
        String endDateMeta = "";
        String metricMeta = "";
        String tag1Meta = "";
        String tag2Meta = "";
        getLogger().info(TestUtils.TAG2_K);
        getLogger().info(result.toString());
        for (HashMap<String, String> map : result) {
            if (map.get("name").equals("ikats_start_date")) {
                startDateMeta = (String) map.get("value");
                getLogger().info("start date from database = " + startDateMeta);
            }
            if (map.get("name").equals("ikats_end_date")) {
                endDateMeta = (String) map.get("value");
                getLogger().info("end date from database = " + endDateMeta);
            }
            if (map.get("name").equals("metric")) {
                metricMeta = (String) map.get("value");
                getLogger().info("metric from database = " + metricMeta);
            }
            if (map.get("name").equals(TestUtils.TAG1_K)) {
                tag1Meta = (String) map.get("value");
                getLogger().info(TestUtils.TAG1_K + " from database = " + tag1Meta);
            }
            if (map.get("name").equals(TestUtils.TAG2_K)) {
                tag2Meta = (String) map.get("value");
                getLogger().info(TestUtils.TAG2_K + " from database = " + tag2Meta);
            }
        }

        /* check status */
        assertEquals(response.getStatus(), 200);

        /* check dates from import equal dates from database */
        assertEquals(startDateImport, startDateMeta);
        assertEquals(endDateImport, endDateMeta);

        /* check metric from import equal metric from database */
        assertEquals(metric, metricMeta);

        /*
         * check tags from import equal tags from database, defined in
         * TestUtils
         */
        assertEquals(tag1Meta, TestUtils.TAG1_V);
        assertEquals(tag2Meta, TestUtils.TAG2_V);
    }

    /**
     * Test import of a TS from a csv simple file
     */
    @Test
    public void importTSWithDataSet() throws Exception {
        String testCaseName = "importTSWithDataSet";
        File file = utils.getTestFile(testCaseName, "/data/test_import.csv");

        getLogger().info("CSV input file : " + file.getAbsolutePath());
        String metric = "testmetric2";
        String url = getAPIURL() + "/ts/put/" + metric;

        utils.doImportStubbedOpenTSDB(file, url, testCaseName, false, 200, true);
    }

    /**
     * Test import of a TS from a csv simple file
     */
    @Test
    public void importTSWithoutDataSetAndTags() throws Exception {

        String testCaseName = "importTSWithoutDataSetAndTags";
        File file = utils.getTestFile(testCaseName, "/data/test_import.csv");

        getLogger().info("CSV input file : " + file.getAbsolutePath());
        String metric = "testmetric3";
        String url = getAPIURL() + "/ts/put/" + metric;

        utils.doImportStubbedOpenTSDB(file, url, testCaseName, true, 200, true);
    }

    /**
     * Test import of a TS from a csv simple file
     */
    @Test
    public void importTSWithoutDataSet() throws Exception {
        String testCaseName = "importTSWithoutDataSet";
        File file = utils.getTestFile(testCaseName, "/data/test_import.csv");

        getLogger().info("CSV input file : " + file.getAbsolutePath());
        String metric = "testmetric4";
        String url = getAPIURL() + "/ts/put/" + metric;

        utils.doImportStubbedOpenTSDB(file, url, testCaseName, false, 200, true);
    }

    /**
     * Test import of a TS without functional identifier. must fail with a 400
     * error code.
     */
    @Test
    public void importTSWithoutFuncId() throws Exception {

        String testCaseName = "importTSWithoutFuncId";
        File file = utils.getTestFile(testCaseName, "/data/test_import.csv");

        getLogger().info("CSV input file : " + file.getAbsolutePath());
        String metric = "testmetric4";
        String url = getAPIURL() + "/ts/put/" + metric;

        utils.doImportStubbedOpenTSDB(file, url, testCaseName, false, 400, false);
    }

}
