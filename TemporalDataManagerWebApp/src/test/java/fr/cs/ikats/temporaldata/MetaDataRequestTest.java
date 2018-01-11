/**
 * LICENSE:
 * --------
 * Copyright 2017-2018 CS SYSTEMES D'INFORMATION
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoInvalidValueException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingResource;
import fr.cs.ikats.datamanager.client.RequestSender;
import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.datamanager.client.opentsdb.ImportResult;
import fr.cs.ikats.lang.StringUtils;
import fr.cs.ikats.metadata.MetaDataFacade;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.temporaldata.business.FilterFunctionalIdentifiers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test on webService metadata operations.
 */
public class MetaDataRequestTest extends AbstractRequestTest {

    @BeforeClass
    public static void setUpBeforClass() {
        AbstractRequestTest.setUpBeforClass(MetaDataRequestTest.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        AbstractRequestTest.tearDownAfterClass(MetaDataRequestTest.class.getSimpleName());
    }

    /**
     * Test Import of metaData
     *
     * Since [#142998] corrected codes: 204 => 409
     *
     * @throws Exception
     */
    @Test
    public void testImportMetaData() throws Exception {

        // Meta data imported
        assertEquals(200, launchMetaDataImport("09Q99AJJZJALAA0098AZFJAANABABAB", "meta1", "value1"));
        // Can't import same meta again (already exist)
        assertEquals(409, launchMetaDataImport("09Q99AJJZJALAA0098AZFJAANABABAB", "meta1", "value2"));
        // New meta imported
        assertEquals(200, launchMetaDataImport("09Q99AJJZJALAA0098AZFJAANABABAB", "meta2", "value1"));
        // Can't import same meta again (already exist)
        assertEquals(409, launchMetaDataImport("09Q99AJJZJALAA0098AZFJAANABABAB", "meta2", "value2"));
        // New meta imported
        assertEquals(200, launchMetaDataImport("09Q99AJJZJALAA0098AZFJAANABABAB", "meta3", "value3"));
        // Can't import same meta again (already exist)
        assertEquals(409, launchMetaDataImport("09Q99AJJZJALAA0098AZFJAANABABAB", "meta3", "value3"));

        listMetaData("09Q99AJJZJALAA0098AZFJAANABABAB", true);
        listMetaData("*", true);
        listMetaData("%2A", true);
        launchDeleteMetaData("09Q99AJJZJALAA0098AZFJAANABABAB", null, 3);
        listMetaData("09Q99AJJZJALAA0098AZFJAANABABAB", false);
    }

    /**
     * Test update of metaData
     *
     * Since [#142998] corrected codes: 204 => 409
     */
    @Test
    public void testUpdateMetaData() {

        // New meta imported
        assertEquals(200, launchMetaDataImport("Z09Q99AJJZJALAA0098AZFJAANABABAB", "meta1", "value1"));
        // Status.Conflict: Can't import same meta again (already exist)
        assertEquals(409, launchMetaDataImport("Z09Q99AJJZJALAA0098AZFJAANABABAB", "meta1", "value2"));
        // New meta imported
        assertEquals(200, launchMetaDataImport("Z09Q99AJJZJALAA0098AZFJAANABABAB", "meta2", "value2"));
        // Update 1st meta
        assertEquals(200, launchMetaDataUpdate("Z09Q99AJJZJALAA0098AZFJAANABABAB", "meta1", "value2"));
        // Update 1st meta with same value
        assertEquals(200, launchMetaDataUpdate("Z09Q99AJJZJALAA0098AZFJAANABABAB", "meta1", "value2"));
        // Status.NOT_KNOWN : Update failed: unknown resource for metadata
        // name
        assertEquals(404, launchMetaDataUpdate("Z09Q99AJJZJALAA0098AZFJAANABABAB", "unknown meta", "value2"));
        // Status.NOT_KNOWN : Update failed: unknown resource for TSUID
        assertEquals(404, launchMetaDataUpdate("unknownTSUID", "meta1", "value2"));
    }

    /**
     * testExportMetaDataCSVSynthetic tests the CSV synthetic format
     *
     * @throws Exception
     */
    @Test
    public void testExportMetaDataCSVSynthetic() throws Exception {

        assertEquals(200, launchMetaDataImport("ts1_1", "meta1", "value1"));
        assertEquals(200, launchMetaDataImport("ts1_2", "meta1", "value2"));
        assertEquals(200, launchMetaDataImport("ts1_3", "meta1", "value3"));
        assertEquals(200, launchMetaDataImport("ts1_1", "meta2", "value2"));
        assertEquals(200, launchMetaDataImport("ts1_4", "meta2", "value1"));
        doFuncIdImport("ts1_1", "FunctionalIdTs1", true, 1L);
        String response = listMetaData("ts1_2,ts1_1,ts1_3,ts1_4", true);

        assertTrue(response.contains(";meta2;meta1"));
        assertTrue(response.contains("\nNO_FUNC_ID_ts1_2;;value2"));
        assertTrue(response.contains("\nNO_FUNC_ID_ts1_3;;value3"));
        assertTrue(response.contains("\nNO_FUNC_ID_ts1_4;value1;"));
        assertTrue(response.contains("\nFunctionalIdTs1;value2;value1"));
    }

    /**
     * testReadMetaData tests the CSV format based on the following requests
     * "ts1,ts2,ts3" "*" "ts1,ts1"
     *
     * @throws Exception
     */
    @Test
    public void testReadMetaData() throws Exception {

        assertEquals(200, launchMetaDataImport("ts1", "meta1", "value1"));
        assertEquals(200, launchMetaDataImport("ts2", "meta1", "value2"));
        assertEquals(200, launchMetaDataImport("ts3", "meta1", "value3"));
        assertEquals(200, launchMetaDataImport("ts1", "meta2", "value2"));
        assertEquals(200, launchMetaDataImport("ts4", "meta1", "value1"));
        listMetaData("ts2,ts1,ts3", true);
        listMetaData("*", true, "json");
        listMetaData("ts1,ts1", true);
        listMetaData("notDefinedTS", false);
        launchDeleteMetaData("ts1", "meta1", 1L);
        listMetaData("ts1", true);
        launchDeleteMetaData("ts1", "meta2", 1L);
        listMetaData("ts1", false);
        launchDeleteMetaData("ts2", null, 1L);
        launchDeleteMetaData("ts3", null, 1L);
        launchDeleteMetaData("ts4", null, 1L);
    }

    /**
     * tests READ services on /metadata/funcId/: consuming Form input
     */
    @Test
    public void testReadFunctionalIdentifiersForm() throws Exception {

        launchReadFunctionalIdentifiers(false, new String[]{"tsuidM", "tsuidN", "tsuidO"}, new String[]{"funcM", "funcN", "funcO"});
    }

    /**
     * tests READ services on /metadata/funcId/ : consuming Json input
     *
     * @throws Exception
     */
    @Test
    public void testReadFunctionalIdentifiersJson() throws Exception {

        launchReadFunctionalIdentifiers(true, new String[]{"tsuidA", "tsuidB", "tsuidC"}, new String[]{"funcA", "funcB", "funcC"});
    }

    /**
     * @param b
     * @param strings
     * @param strings2
     */
    private void launchReadFunctionalIdentifiers(boolean isInputJson, String[] importedTsuids, String[] importedFuncIds) throws Exception {

        String[][] expectedRes = new String[importedFuncIds.length][2];
        for (int i = 0; i < importedFuncIds.length; i++) {
            doFuncIdImport(importedTsuids[i], importedFuncIds[i], false, 1L);
            expectedRes[i][0] = importedTsuids[i];
            expectedRes[i][1] = importedFuncIds[i];
        }
        // Test read of one funcId
        getFunctionalIdentifierWithTsuid(importedTsuids[0], true, importedFuncIds[0]);

        getFunctionalIdentifierWithTsuid("tsuidD", false, "");

        // Search by tsuids criterion
        // ----------------------------

        // expects status 200: everything matched
        getFunctionalIdentifiersWithTsuids(isInputJson, importedTsuids, false, expectedRes);

        // expects status 404: nothing matched
        getFunctionalIdentifiersWithTsuids(isInputJson, new String[]{"tsuidX", "tsuidY"}, true, new String[][]{});

        // expects status 200: partial match: found
        getFunctionalIdentifiersWithTsuids(isInputJson, new String[]{"tsuidX", "tsuidY", importedTsuids[0]}, false,
                new String[][]{{importedTsuids[0], importedFuncIds[0]}});
    }

    /**
     * Tests filter on FunctionalIdentifiers with tsuids criteria
     *
     * @param tsuids
     * @param isResultEmpty
     * @param expectedFuncIdFound
     */
    private void getFunctionalIdentifiersWithTsuids(boolean isInputJson, String[] tsuids, boolean isResultEmpty, String[][] expectedFuncIdFound)
            throws Exception {

        String lPrefixeLog = isInputJson ? "Json " : "Form ";

        // Prepare expected result reference map
        HashMap<String, String> resReferenceMap = new HashMap<String, String>();

        for (int i = 0; i < expectedFuncIdFound.length; i++) {
            resReferenceMap.put(expectedFuncIdFound[i][0], expectedFuncIdFound[i][1]);
        }

        getLogger().info("get functional IDs for tsuids=" + StringUtils.join(",", tsuids));
        String url = getAPIURL() + "/metadata/funcId";

        FilterFunctionalIdentifiers lFilter = new FilterFunctionalIdentifiers();
        ArrayList<String> mytsuids = new ArrayList<String>();
        for (String tsuid : tsuids) {
            mytsuids.add(tsuid);
        }
        lFilter.setTsuids(mytsuids);

        try {

            getLogger().debug(lPrefixeLog + "Sending POST request to url : " + url + "with input " + lFilter);

            ClientConfig clientConfig = new ClientConfig();
            Client client = ClientBuilder.newClient(clientConfig);
            Response response = null;
            try {
                WebTarget target = client.target(url);

                Entity<?> lEntity = prepareFunctionalIdentifierInput(isInputJson, lFilter);

                response = target.request().post(lEntity);

            } catch (Exception ePost) {
                getLogger().error(lPrefixeLog + "Error TU getFunctionalIds: posting request", ePost);
                throw ePost;
            }
            if (response != null) {
                // Commented: code below is Consuming the answer:
                // String rawResponse = ResponseParser.getJSONFromResponse(
                // response );
                // getLogger().info( "POST /metadata/funcId Response: " +
                // rawResponse
                // );

                // Below: we directly parse response into the expected type
                // List<FunctionalIdentifier
                //
                if (!isResultEmpty) {
                    assertEquals(response.getStatus(), 200);

                    List<FunctionalIdentifier> res = response.readEntity(new GenericType<List<FunctionalIdentifier>>() {
                    });

                    // Check that result size has same length than the
                    // expectedFuncIdFound
                    assertEquals(expectedFuncIdFound.length, res.size());

                    for (FunctionalIdentifier functionalIdentifier : res) {
                        getLogger().info(lPrefixeLog + "Eval result item: " + functionalIdentifier);

                        // Check that each result pair (tsuid, funcId) is
                        // matched by resReferenceMap
                        assertEquals(true, resReferenceMap.containsKey(functionalIdentifier.getTsuid()));

                        assertEquals(resReferenceMap.get(functionalIdentifier.getTsuid()), functionalIdentifier.getFuncId());
                    }
                } else {
                    assertEquals(404, response.getStatus());
                }
            } else {
                throw new Exception("response is null");
            }

            getLogger().info(lPrefixeLog + response.getStatus());
        } catch (Throwable e) {
            throw new Exception("TU: " + lPrefixeLog + "POST /metadata/funcId/ with filter: " + lFilter.toString(), e);
        }

    }

    /**
     * Tests read of FunctionalIdentifiers matching the tsuid
     *
     * @param tsuid
     * @param funcIdExists
     * @param expectedFuncId
     */
    private void getFunctionalIdentifierWithTsuid(String tsuid, boolean funcIdExists, String expectedFuncId) throws Exception {

        getLogger().info("get functional ID for tsuid=" + tsuid);
        String url = getAPIURL() + "/metadata/funcId/" + tsuid;

        Response response = RequestSender.sendGETRequest(url);

        getLogger().info(response.getStatus());
        if (funcIdExists) {
            // 200
            assertEquals(Status.OK.getStatusCode(), response.getStatus());

            FunctionalIdentifier res = response.readEntity(FunctionalIdentifier.class);
            getLogger().info(res);

            assertEquals(expectedFuncId, res.getFuncId());
        } else {
            // 404
            assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
        }

    }

    private Entity<?> prepareFunctionalIdentifierInput(boolean isJson, FilterFunctionalIdentifiers filter) {
        if (isJson) {
            Entity<FilterFunctionalIdentifiers> lEntityFilter = Entity.entity(filter, MediaType.APPLICATION_JSON_TYPE);
            return lEntityFilter;
        } else {
            Form form = new Form();
            if (filter.getTsuids() != null) {
                for (String lTsuid : filter.getTsuids()) {
                    form.param("tsuids", lTsuid);
                }
            }
            if (filter.getFuncIds() != null) {
                for (String lFuncId : filter.getFuncIds()) {
                    form.param("funcIds", lFuncId);
                }
            }

            Entity<Form> lEntityFilter = Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE);
            return lEntityFilter;
        }

    }

    private void launchDeleteMetaData(String tsuid, String name, long expected) throws Exception {

        String url = getAPIURL() + "/metadata/" + tsuid;
        if (name != null) {
            url = url + "/" + name;
        }

        Response response = RequestSender.sendDELETERequest(url);
        ImportResult result = response.readEntity(ImportResult.class);
        assertEquals(expected, result.getNumberOfSuccess());
    }

    /**
     * DG test for incorrect format for funcId parameter
     */
    @Test
    public void testImportFunctionalIdentifier_DG() {

        Response response = doFuncIdImport("tsuid1_func", "_$ùùùùù", true, 1L);
        assertEquals(Status.NOT_ACCEPTABLE.getStatusCode(), response.getStatus());
        getLogger().info(response);

        response = doFuncIdImport("tsuid1_func", "toto", true, 1L);
        response = doFuncIdImport("tsuid1_func", "toto", true, 0L);
    }

    /**
     * Test the import of meta data from a bad formatted CSV file
     * without update option => no meta created
     *
     * @throws IkatsDaoMissingResource
     */
    @Test
    public void testImportMetaDataCSVWithErrorsinCSV() throws IkatsDaoMissingResource {

        String testCaseName = "testImportMetaDataCSVWithErrorsinCSV";

        // Fill in some functional Ids to allow the import
        doFuncIdImport(testCaseName + "tsuid1", "MAM1", false, 1L);
        doFuncIdImport(testCaseName + "tsuid2", "MAM2", false, 1L);
        doFuncIdImport(testCaseName + "tsuid3", "MAM3", false, 1L);

        // Test with errors in csv => no metadata imported
        checkImportMetadataFromCsv("/data/metadata_import.csv", 0, false);
    }

    /**
     * Test the import of meta data from a CSV file
     * with update option => meta are created
     *
     * @throws IkatsDaoMissingResource
     * @throws IkatsDaoConflictException
     */
    @Test
    public void testImportMetaDataCSVWithUpdate() throws IkatsDaoException, IkatsDaoMissingResource, IkatsDaoConflictException {

        String testCaseName = "testImportMetaDataCSVWithUpdate";

        MetaDataFacade facade = new MetaDataFacade();

        // Test without functional ids :=> no metadata imported
        checkImportMetadataFromCsv("/data/metadata_import_update.csv", 0, true);

        // Fill in some functional Ids to allow the import
        String tsuid1 = testCaseName + "tsuid1";
        String funcId1 = testCaseName + "MAM1";

        String tsuid2 = testCaseName + "tsuid2";
        String funcId2 = testCaseName + "MAM2";

        String tsuid3 = testCaseName + "tsuid3";
        String funcId3 = testCaseName + "MAM3";

        doFuncIdImport(tsuid1, funcId1, false, 1L);
        doFuncIdImport(tsuid2, funcId2, false, 1L);
        doFuncIdImport(tsuid3, funcId3, false, 1L);

        assertEquals(200, launchMetaDataImport(tsuid1, "cycle", "aterrissage"));

        // Test with update : 6 metadata created + 1 metadata updated
        checkImportMetadataFromCsv("/data/metadata_import_update.csv", 7, true);

        // check update of md
        String metaVal = facade.getMetaData(tsuid1, "cycle").getValue();
        assertEquals("decollage", metaVal);
    }

    /**
     * Test the import of meta data from a CSV file
     * without update option => no meta are imported
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testImportMetaDataCSVWithoutUpdate() throws IkatsDaoException {

        String testCaseName = "testImportMetaDataCSVWithoutUpdate";

        MetaDataFacade facade = new MetaDataFacade();

        // Fill in some functional Ids to allow the import
        String tsuid1 = testCaseName + "tsuid1";
        String funcId1 = testCaseName + "MAM1";

        String tsuid2 = testCaseName + "tsuid2";
        String funcId2 = testCaseName + "MAM2";

        String tsuid3 = testCaseName + "tsuid3";
        String funcId3 = testCaseName + "MAM3";

        doFuncIdImport(tsuid1, funcId1, false, 1L);
        doFuncIdImport(tsuid2, funcId2, false, 1L);
        doFuncIdImport(tsuid3, funcId3, false, 1L);

        assertEquals(200, launchMetaDataImport(tsuid1, "cycle", "aterrissage"));

        // Test without update : one metadata already exists => no metadata imported
        checkImportMetadataFromCsv("/data/metadata_import_no_update.csv", 0, false);

        try {
            // check NON-import of md from file
            List<MetaData> metaList = facade.getMetaDataForTS(tsuid1);
            // check NON-update of md
            assertEquals("aterrissage", metaList.get(0).getValue());
        } catch (IkatsDaoMissingResource e) {
            getLogger().error("Exception catched : no metadata found => NOK");
            fail();
        }

        try {
            // check NON-import of md from file
            List metaList = facade.getMetaDataForTS(tsuid2);
            fail();
        } catch (IkatsDaoMissingResource e) {
            // ok no metadata found
            getLogger().info("Exception catched : no metadata found => OK");
        }

        try {
            // check NON-import of md from file
            List metaList = facade.getMetaDataForTS(tsuid3);
            fail();
        } catch (IkatsDaoMissingResource e) {
            // ok no metadata found
            getLogger().info("Exception catched : no metadata found => OK");
        }
    }

    /**
     * Test the import of meta data from a CSV file
     * containing metric without update option => meta are imported
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testImportMetaDataCSVWithMetric() throws IkatsDaoException {

        String testCaseName = "testImportMetaDataCSVWithMetric";
        MetaDataFacade facade = new MetaDataFacade();

        // Fill in some functional Ids to allow the import
        String tsuid1 = testCaseName + "tsuid1";
        String funcId1 = testCaseName + "MAM1";

        String tsuid2 = testCaseName + "tsuid2";
        String funcId2 = testCaseName + "MAM2";

        String tsuid3 = testCaseName + "tsuid3";
        String funcId3 = testCaseName + "MAM3";

        doFuncIdImport(tsuid1, funcId1, false, 1L);
        doFuncIdImport(tsuid2, funcId2, false, 1L);
        doFuncIdImport(tsuid3, funcId3, false, 1L);

        assertEquals(200, launchMetaDataImport(tsuid1, "metric", "METRICTEST"));
        assertEquals(200, launchMetaDataImport(tsuid3, "metric", "METRICTEST"));

        // check import of metadata by metric :
        // 1 metric of 2 ts x 2 meta => 4 metadata 
        // + 1 funcid x 2 meta 
        // = 8 metadata imported (cf. csv file)
        checkImportMetadataFromCsv("/data/metadata_import_metric.csv", 6, false);

        // check update of md for each tsuid
        List<MetaData> metaList = facade.getMetaDataForTS(tsuid1);
        assertEquals(3, metaList.size());
        List<MetaData> metaList2 = facade.getMetaDataForTS(tsuid2);
        assertEquals(2, metaList2.size());
        List<MetaData> metaList3 = facade.getMetaDataForTS(tsuid3);
        assertEquals(3, metaList3.size());
    }

    /**
     * tests READ services on /metadata/types/
     *
     * @throws IkatsWebClientException
     * @throws IkatsDaoException
     * @throws IkatsDaoInvalidValueException
     * @throws IkatsDaoConflictException
     */
    @Test
    public void testReadMetadataTypes() throws IkatsWebClientException, IkatsDaoConflictException, IkatsDaoInvalidValueException, IkatsDaoException {

        String url = getAPIURL() + "/metadata/types/";

        MetaDataFacade facade = new MetaDataFacade();
        facade.persistMetaData("tsuidA01", "thatsastring", "blabla", "string");
        facade.persistMetaData("tsuidA02", "thatsanumber", "12", "number");

        Response response = RequestSender.sendGETRequest(url);

        getLogger().info(response.getStatus());

        // 200
        assertEquals(Status.OK.getStatusCode(), response.getStatus());

        Map<String, String> res = response.readEntity(new GenericType<Map<String, String>>() {
        });

        assertTrue(res.containsKey("thatsastring"));
        assertEquals("string", res.get("thatsastring"));

        assertTrue(res.containsKey("thatsanumber"));
        assertEquals("number", res.get("thatsanumber"));
    }

    /**
     * Tests the import of a meta data file (CSV format) and checks if the
     * import count is equal to expectedImportCount
     *
     * @param resourcePath        path of file (CSV) to send containing the meta data
     * @param expectedImportCount expected count of imported meta data
     * @param update              if true, already existing metadata is updated
     *                            otherwise no metadata is imported if one of them already exists
     */
    private void checkImportMetadataFromCsv(String resourcePath, Integer expectedImportCount, boolean update) throws IkatsDaoMissingResource {

        // Prepare file to import
        Resource resource = new ClassPathResource(resourcePath);
        File file = null;
        try {
            file = resource.getFile();
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        // Prepare the request to send
        String url = getAPIURL() + "/metadata/import/file";
        FileDataBodyPart bodyPart = new FileDataBodyPart("file", file);
        final FormDataMultiPart multipart = new FormDataMultiPart();
        multipart.bodyPart(bodyPart);
        Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();

        // Send the import request
        WebTarget target = client.target(url).queryParam("details", "false").queryParam("update", update ? "true" : "false");

        getLogger().info("sending url : " + url);
        Integer result = 0;
        Response response = null;
        try {
            response = target.request().post(Entity.entity(multipart, multipart.getMediaType()));
            result = response.readEntity(Integer.class);
        } catch (Exception e) {
            // in that case no meta imported => result = 0
        }

        // Check that the import count is coherent with expected count
        getLogger().info("parsing response of " + url);
        getLogger().info(response);
        getLogger().info(result);
        assertEquals(expectedImportCount, result);
    }

    private String listMetaData(String tsuid, boolean TSexists) throws Exception {
        return listMetaData(tsuid, TSexists, "csv");
    }

    /**
     * display the metadata list for a given tsuid. Format is csv.
     *
     * @param tsuid
     */
    private String listMetaData(String tsuid, boolean TSexists, String format) throws Exception {
        getLogger().info("list meta data for tsuid=" + tsuid);
        String url;
        Response response = null;
        String result = "";
        if (format.equals("json")) {
            url = getAPIURL() + "/metadata/list/json?tsuid=" + tsuid;
        } else {
            url = getAPIURL() + "/metadata/list?tsuid=" + tsuid;
        }
        response = RequestSender.sendGETRequest(url);
        result = response.readEntity(String.class);
        getLogger().info(result);
        getLogger().info(response.getStatus());
        if (TSexists) {
            assertEquals(response.getStatus(), 200);
        } else {
            assertEquals(response.getStatus(), 404);
        }
        return result;
    }
}
