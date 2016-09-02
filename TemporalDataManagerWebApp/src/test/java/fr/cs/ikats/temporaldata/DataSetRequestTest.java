/**
 * $Id$
 */
package fr.cs.ikats.temporaldata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import fr.cs.ikats.datamanager.client.opentsdb.ImportResult;
import fr.cs.ikats.temporaldata.business.DataSetManager;
import fr.cs.ikats.temporaldata.business.DataSetWithFids;
import fr.cs.ikats.temporaldata.business.MetaDataManager;
import fr.cs.ikats.ts.dataset.model.DataSet;

/**
 * Test on webService dataset operations. This class is using standard services
 * from superclass AbstractRequestTest, and super-superclass CommonTest.
 */
public class DataSetRequestTest extends AbstractRequestTest {

    @BeforeClass
    public static void setUpBeforClass() {
        AbstractRequestTest.setUpBeforClass(DataSetRequestTest.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        AbstractRequestTest.tearDownAfterClass(DataSetRequestTest.class.getSimpleName());
    }

    @Test
    public void testImportDataSet() {
        String testCaseName = "testImportDataSet";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            String dataSetId = "ZmonDataSet_" + testCaseName;

            String tsuid1 = "tsuid_Z1" + testCaseName;
            String funcId1 = "funcId1" + testCaseName;
            doFuncIdImport(tsuid1, funcId1, false, 1);
            String tsuid2 = "tsuid_Z2" + testCaseName;
            String funcId2 = "funcId2" + testCaseName;
            doFuncIdImport(tsuid2, funcId2, false, 1);
            String tsuid3 = "MAM1" + testCaseName;
            String funcId3 = "funcIdMAM1" + testCaseName;
            doFuncIdImport(tsuid3, funcId3, false, 1);
            String tsuid4 = "MAM2" + testCaseName;
            String funcId4 = "funcIdMAM2" + testCaseName;
            doFuncIdImport(tsuid4, funcId4, false, 1);

            launchDataSetImport(dataSetId, "une description qui ne doit pas être trop longue", tsuid1 + "," + tsuid2 + "," + tsuid3 + "," + tsuid4);

            removeDataSet(dataSetId, false);

            endNominal(testCaseName);
        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }

    }

    @Test
    public void testUpdateDataSet() {

        String testCaseName = "testUpdateDataSet";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);
            String dataSetId = "ZmonDataSet_" + testCaseName;

            String tsuid1 = "tsuid_Z1" + testCaseName;
            String funcId1 = "funcId1" + testCaseName;
            doFuncIdImport(tsuid1, funcId1, false, 1);
            String tsuid2 = "tsuid_Z2" + testCaseName;
            String funcId2 = "funcId2" + testCaseName;
            doFuncIdImport(tsuid2, funcId2, false, 1);
            String tsuid3 = "MAM1" + testCaseName;
            String funcId3 = "funcIdMAM1" + testCaseName;
            doFuncIdImport(tsuid3, funcId3, false, 1);
            String tsuid4 = "MAM2" + testCaseName;
            String funcId4 = "funcIdMAM2" + testCaseName;
            doFuncIdImport(tsuid4, funcId4, false, 1);
            String tsuid5 = "MAM3" + testCaseName;
            String funcId5 = "funcIdMAM3" + testCaseName;
            doFuncIdImport(tsuid5, funcId5, false, 1);

            launchDataSetImport(dataSetId, "une description qui ne doit pas être trop longue", tsuid1 + "," + tsuid2);
            DataSetWithFids dataset = getDataSet(dataSetId, MediaType.APPLICATION_JSON);
            assertEquals(2, dataset.getTsuidsAsString().size());
            launchDataSetUpdate(dataSetId, "autre Description", tsuid3 + "," + tsuid4 + "," + tsuid5, "replace");
            DataSetWithFids dataset2 = getDataSet(dataSetId, MediaType.APPLICATION_JSON);
            assertEquals("autre Description", dataset2.getDescription());
            assertEquals(3, dataset2.getTsuidsAsString().size());

            // ... more detailed tests in DAO Dataset projet
            removeDataSet(dataSetId, false);

            endNominal(testCaseName);
        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    @Test
    public void testUpdateDataSetWithAppendMode() {

        String testCaseName = "testUpdateDataSetWithAppendMode";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);
            String dataSetId = "ZmonDataSet_" + testCaseName;

            String tsuid1 = "tsuid_Z1" + testCaseName;
            String funcId1 = "funcId1" + testCaseName;
            doFuncIdImport(tsuid1, funcId1, false, 1);
            String tsuid2 = "tsuid_Z2" + testCaseName;
            String funcId2 = "funcId2" + testCaseName;
            doFuncIdImport(tsuid2, funcId2, false, 1);
            String tsuid3 = "MAM1" + testCaseName;
            String funcId3 = "funcIdMAM1" + testCaseName;
            doFuncIdImport(tsuid3, funcId3, false, 1);
            String tsuid4 = "MAM2" + testCaseName;
            String funcId4 = "funcIdMAM2" + testCaseName;
            doFuncIdImport(tsuid4, funcId4, false, 1);
            String tsuid5 = "MAM3" + testCaseName;
            String funcId5 = "funcIdMAM3" + testCaseName;
            doFuncIdImport(tsuid5, funcId5, false, 1);

            launchDataSetImport(dataSetId, "une description qui ne doit pas être trop longue", tsuid1 + "," + tsuid2);
            DataSetWithFids dataset = getDataSet(dataSetId, MediaType.APPLICATION_JSON);
            assertEquals(2, dataset.getTsuidsAsString().size());
            launchDataSetUpdate(dataSetId, "autre Description", tsuid3 + "," + tsuid4 + "," + tsuid5, "append");
            DataSetWithFids dataset2 = getDataSet(dataSetId, MediaType.APPLICATION_JSON);
            assertEquals("autre Description", dataset2.getDescription());
            assertEquals(5, dataset2.getTsuidsAsString().size());

            // ... more detailed tests in DAO Dataset projet
            removeDataSet(dataSetId, true);

            endNominal(testCaseName);
        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    public void testALotOfDataSetimport() {

        String testCaseName = "testALotOfDataSetimport";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            for (int i = 0; i < 100; i++) {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < 200; j++) {
                    sb.append("tsuid" + j);
                    if (j < 199) {
                        sb.append(",");
                    }
                }
                launchDataSetImport("monDataSet" + i, "une description qui ne doit pas etre trop longue", sb.toString());
            }

            endNominal(testCaseName);

        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    @Test
    public void testGetDataSet() {

        String testCaseName = "testGetDataSet";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            String prefix = testCaseName;
            doFuncIdImport(prefix + "tsuid1", prefix + "funcId1", false, 1);
            doFuncIdImport(prefix + "tsuid2", prefix + "funcId2", false, 1);
            doFuncIdImport(prefix + "MAM1", prefix + "funcIdMAM1", false, 1);
            doFuncIdImport(prefix + "MAM2", prefix + "funcIdMAM2", false, 1);

            launchDataSetImport(prefix + "QmonDataSet", "une description qui ne doit pas être trop longue",
                    prefix + "tsuid1," + prefix + "tsuid2," + prefix + "MAM1," + prefix + "MAM2");

            DataSetWithFids dataset = getDataSet(prefix + "QmonDataSet", MediaType.APPLICATION_JSON);
            assertEquals(prefix + "funcId1", dataset.getFid(prefix + "tsuid1"));
            assertEquals(prefix + "funcId2", dataset.getFid(prefix + "tsuid2"));
            assertEquals(prefix + "funcIdMAM1", dataset.getFid(prefix + "MAM1"));
            assertEquals(prefix + "funcIdMAM2", dataset.getFid(prefix + "MAM2"));

            DataSetWithFids dataset2 = getDataSet("monDataSet___14", MediaType.APPLICATION_JSON);
            assertNull(dataset2);

            endNominal(testCaseName);

        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    @Test
    public void testRemoveTSFromDataSet() {

        String testCaseName = "testRemoveTSFromDataSet";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            String prefix = testCaseName;
            doFuncIdImport(prefix + "tsuid1", prefix + "funcId1", false, 1);
            doFuncIdImport(prefix + "tsuid2", prefix + "funcId2", false, 1);
            doFuncIdImport(prefix + "MAM1", prefix + "funcIdMAM1", false, 1);
            doFuncIdImport(prefix + "MAM2", prefix + "funcIdMAM2", false, 1);

            launchDataSetImport(prefix + "QmonDataSet", "une description qui ne doit pas être trop longue",
                    prefix + "tsuid1," + prefix + "tsuid2," + prefix + "MAM1," + prefix + "MAM2");

            DataSetWithFids dataset = getDataSet(prefix + "QmonDataSet", MediaType.APPLICATION_JSON);
            assertEquals(dataset.getFids().size(), 4);

            DataSetManager datasetmanager = new DataSetManager();
            datasetmanager.removeTSFromDataSet(prefix + "MAM1", prefix + "QmonDataSet");

            dataset = getDataSet(prefix + "QmonDataSet", MediaType.APPLICATION_JSON);
            assertEquals(dataset.getFids().size(), 3);

            endNominal(testCaseName);

        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    @Test
    public void testRemoveDataSet() throws IkatsDaoMissingRessource, IkatsDaoException {

        String testCaseName = "testRemoveDataSet";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            // Prepare data
            doFuncIdImport(testCaseName + "tsuid11", testCaseName + "funcId11", false, 1);
            doFuncIdImport(testCaseName + "tsuid12", testCaseName + "funcId12", false, 1);
            doFuncIdImport(testCaseName + "MAM30", testCaseName + "funcId_MAM30", false, 1);
            launchMetaDataImport(testCaseName + "tsuid11", "meta1", "value1");
            launchMetaDataImport(testCaseName + "tsuid12", "meta2", "value2");
            launchDataSetImport(testCaseName + "monDataSet___1", "une description qui ne doit pas être trop longue",
                    testCaseName + "tsuid11," + testCaseName + "tsuid12," + testCaseName + "MAM30");

            // SOFT remove dataset
            Response reponse = removeDataSet(testCaseName + "monDataSet___1", false);

            // check return code is 204
            assertEquals(reponse.getStatus(), 204);
            // check dataset does not exist any more
            assertNull(getDataSet(testCaseName + "monDataSet___1", MediaType.APPLICATION_JSON));

            // check timeseries still exist
            MetaDataManager metadataManager = new MetaDataManager();

            assertNotNull(metadataManager.getFunctionalIdentifierByTsuid(testCaseName + "tsuid11"));
            assertNotNull(metadataManager.getFunctionalIdentifierByTsuid(testCaseName + "tsuid12"));
            assertNotNull(metadataManager.getFunctionalIdentifierByTsuid(testCaseName + "MAM30"));

            // metadata still exist
            assertNotNull(metadataManager.getList(testCaseName + "tsuid11"));
            assertNotNull(metadataManager.getList(testCaseName + "tsuid12"));

            endNominal(testCaseName);
        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    @Test
    public void testDeepRemoveDataSet() throws IkatsDaoMissingRessource, IkatsDaoException {

        String testCaseName = "testDeepRemoveDataSet";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            // creation by import of a tsuid in database
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
            /* retrieval of the tsuid1 from import task */
            ImportResult retour = utils.doImport(file, url, true, 200);
            getLogger().info("retour ts_1 [" + retour + "]");
            String tsuid1 = retour.getTsuid();
            getLogger().info("tsuid ts_1 [" + tsuid1);
            /* import of a timeseries */
            String metric2 = "testmetric2";
            String url2 = getAPIURL() + "/ts/put/" + metric2;
            /* retrieval of the tsuid2 from import task */
            ImportResult retour2 = utils.doImport(file, url2, true, 200);
            String tsuid2 = retour2.getTsuid();

            /* import of a timeseries */
            String metric3 = "testmetric";
            String url3 = getAPIURL() + "/ts/put/" + metric3;
            /* retrieval of the tsuid3 from import task */
            ImportResult retour3 = utils.doImport(file, url3, true, 200);
            String tsuid3 = retour3.getTsuid();

            MetaDataManager metadataManager = new MetaDataManager();
            assertNotNull(metadataManager.getFunctionalIdentifierByTsuid(tsuid1));
            assertNotNull(metadataManager.getFunctionalIdentifierByTsuid(tsuid2));
            assertNotNull(metadataManager.getFunctionalIdentifierByTsuid(tsuid3));

            // Prepare data
            String dataSetId = "monDataSet_" + testCaseName;
            launchDataSetImport(dataSetId, "une description qui ne doit pas être trop longue", "" + tsuid1 + "," + tsuid2 + "," + tsuid3 + "");

            // deep remove dataset
            Response reponse = removeDataSet(dataSetId, true);

            // check return code is 204
            assertEquals(reponse.getStatus(), 204);
            // check dataset does not exist any more
            assertNull(getDataSet(dataSetId, MediaType.APPLICATION_JSON));

            // check timeseries do not exist any more
            assertNull(metadataManager.getFunctionalIdentifierByTsuid(tsuid1));
            assertNull(metadataManager.getFunctionalIdentifierByTsuid(tsuid2));
            assertNull(metadataManager.getFunctionalIdentifierByTsuid(tsuid3));

            // metadata do not exist any more
            try {
                Object listMeta = metadataManager.getList(tsuid1);
            }
            catch (IkatsDaoMissingRessource e) {
                getLogger().info("OK: no metadata associated to :" + tsuid1);
            }
            catch (Throwable e) {
                throw new Exception("There are still metadata associated to :" + tsuid1, e);
            }
            try {
                Object listMeta = metadataManager.getList(tsuid2);
            }
            catch (IkatsDaoMissingRessource e) {
                getLogger().info("OK: no metadata associated to :" + tsuid2);
            }
            catch (Throwable e) {
                throw new Exception("There are still metadata associated to :" + tsuid2, e);
            }
            try {
                Object listMeta = metadataManager.getList(tsuid3);
            }
            catch (IkatsDaoMissingRessource e) {
                getLogger().info("OK: no metadata associated to :" + tsuid3);
            }
            catch (Throwable e) {
                throw new Exception("There are still metadata associated to :" + tsuid3, e);
            }

            endNominal(testCaseName);
        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    @Test
    public void testReturnCode404RemoveDataSet() throws IkatsDaoMissingRessource, IkatsDaoException {

        String testCaseName = "testReturnCode404RemoveDataSet";
        boolean isNominal = false;
        try {
            start(testCaseName, isNominal);

            // remove dataset which does not exist
            Response reponse = removeDataSet("dataset_qui_n_existe_pas", true);
            assertTrue(reponse.getStatus() == 404);

            // deep remove dataset which does not exist
            reponse = removeDataSet("dataset_qui_n_existe_pas", false);
            assertTrue(reponse.getStatus() == 404);

            endNominal(testCaseName);

        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    @Test
    public void testGetSummary() {

        String testCaseName = "testGetSummary";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            String prefix = "DatasetRequestTestGetSummary";

            String[][] nameAndDescInputs = { { prefix + "DataSet7", "desc DataSet7" }, { prefix + "DataSet2", "desc DataSet2" },
                    { prefix + "DataSet3", "desc DataSet3" }, { prefix + "DataSet4", "desc DataSet4" }, { prefix + "DataSet5", "desc DataSet5" },
                    { prefix + "DataSet6", "desc DataSet6" } };

            String prefixFid = "FuncId";

            String[] importedTsuids = { prefix + "tsuid1", prefix + "tsuid2", prefix + "MAM1", prefix + "MAM2" };

            for (int ii = 0; ii < importedTsuids.length; ii++) {
                doFuncIdImport(importedTsuids[ii], prefixFid + importedTsuids[ii], false, 1);
            }

            for (int i = 0; i < nameAndDescInputs.length; i++) {

                String[] currentInput = nameAndDescInputs[i];

                String contentDesc = getPrefixedListAsString("", importedTsuids, ",");

                launchDataSetImport(currentInput[0], currentInput[1], contentDesc);

                DataSetWithFids dataset = getDataSet(currentInput[0], MediaType.APPLICATION_JSON);
                getLogger().info(dataset.toDetailedString());

                assertEquals("Test on first imported fid for currentContent[0]", prefixFid + importedTsuids[0], dataset.getFid(importedTsuids[0]));

            }

            List<DataSet> datasets = getAllDataSetSummary(MediaType.APPLICATION_JSON);

            // It is needed to watch only the dataset from this test:
            //
            List<DataSet> goodDatasets = new ArrayList<DataSet>();
            assertNotNull(datasets);
            int countDatasets = 0;
            for (DataSet dataSet : datasets) {

                if (dataSet.getName().startsWith(prefix)) {
                    assertNotNull(dataSet.getName());
                    assertNotNull(dataSet.getDescription());

                    getLogger().info("[testGetSummary] " + dataSet.toDetailedString(false));
                    countDatasets++;
                    goodDatasets.add(dataSet);
                    int matched = 0;
                    for (int i = 0; i < nameAndDescInputs.length; i++) {
                        String[] currentInput = nameAndDescInputs[i];
                        if (dataSet.getName().equals(currentInput[0]) && dataSet.getDescription().equals(currentInput[1])) {
                            matched++;
                        }
                    }
                    assertEquals("Dataset summary matches exactly each name + desc", 1, matched);
                }
            }
            assertEquals("Dataset summary matches expected count for TU", nameAndDescInputs.length, countDatasets);

            endNominal(testCaseName);

        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);

        }
    }

    private String getPrefixedListAsString(String testPrefix, String[] values, String sep) {
        StringBuilder buff = new StringBuilder("");
        boolean start = true;
        for (int i = 0; i < values.length; i++) {
            if (!start) {
                buff.append(sep);
            }
            else {
                start = false;
            }

            buff.append(testPrefix);
            buff.append(values[i]);
        }
        return buff.toString();
    }

    /**
     * launch a request for meta data import.
     * 
     * @param tsuid
     * @param name
     * @param value
     */
    private void launchDataSetImport(String dataSetId, String description, String tsuids) {
        String url = getAPIURL() + "/dataset/import/" + dataSetId;

        try {
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.register(MultiPartFeature.class);
            Client client = ClientBuilder.newClient(clientConfig);

            Response response = null;
            WebTarget target = client.target(url);
            Form form = new Form();
            form.param("description", description);
            form.param("tsuidList", tsuids);
            response = target.request().post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
            getLogger().info(response.getStatusInfo());
            getLogger().info(response);
            assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
        }
        catch (Throwable e) {
            throw e;
        }
    }

    /**
     * 
     * @param dataSetId
     * @param description
     * @param tsuids
     * @param mode
     *            "replace" or "append"
     * @throws Exception
     */
    private void launchDataSetUpdate(String dataSetId, String description, String tsuids, String mode) throws Exception {
        String url = getAPIURL() + "/dataset/" + dataSetId;

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(MultiPartFeature.class);
        Client client = ClientBuilder.newClient(clientConfig);

        Response response = null;
        Form form = new Form();
        form.param("description", description);
        form.param("tsuidList", tsuids);
        url = url + "?updateMode=" + mode;
        response = utils.sendPUTRequest(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE), MediaType.APPLICATION_FORM_URLENCODED,
                client, url, "");
        getLogger().info(response.getStatusInfo());
        getLogger().info(response);
    }

    private List<DataSet> getAllDataSetSummary(String mediaType) {
        List<DataSet> result = null;

        String url = getAPIURL() + "/dataset";
        try {
            Client client = utils.getClientWithJSONFeature();

            Response response = utils.sendGETRequest(mediaType, client, url, "172.28.0.56");
            getLogger().info(url + " : response status" + response.getStatus());
            if (response.getStatus() <= 200) {
                result = response.readEntity(new GenericType<List<DataSet>>() {
                });
            }
        }
        catch (Throwable e) {
            getLogger().error("Failure: getAllDataSetSummary()", e);
            result = null;
        }
        return result;

    }

    private DataSetWithFids getDataSet(String datasetId, String mediaType) {
        DataSetWithFids result = null;

        String url = getAPIURL() + "/dataset/" + datasetId;
        try {
            Client client = utils.getClientWithJSONFeature();

            Response response = utils.sendGETRequest(mediaType, client, url, "172.28.0.56");
            getLogger().info(url + " : response status" + response.getStatus());
            if (response.getStatus() <= 200) {
                result = response.readEntity(DataSetWithFids.class);
            }
        }
        catch (Throwable e) {
            e.printStackTrace();
            getLogger().error("Failure: getDataSet()", e);
            result = null;
        }
        return result;

    }

    private Response removeDataSet(String datasetId, boolean deep) {
        String result = null;
        String url = getAPIURL() + "/dataset/" + datasetId;
        if (deep) {
            url = url + "?deep=true";
        }
        Response response = null;
        try {
            Client client = utils.getClientWithJSONFeature();

            response = utils.sendDeleteRequest(client, url);
            getLogger().info(url + " : response status" + response.getStatus());
            if (response.getStatus() <= 200) {
                result = response.readEntity(String.class);
                getLogger().info(url + " : result" + result);
            }
        }
        catch (Throwable e) {
            getLogger().error("Failure: removeDataSet()", e);
            response = null;
        }

        return response;
    }

}
