/**
 * Copyright 2018-2019 CS Systèmes d'Information
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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.datamanager.client.RequestSender;
import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.workflow.Workflow;
import fr.cs.ikats.workflow.WorkflowEntitySummary;
import fr.cs.ikats.workflow.WorkflowFacade;

import static org.junit.Assert.assertEquals;

public class WorkflowResourceTest extends AbstractRequestTest {
    /**
     * Facade to connect to Workflow DAO allowing to prepare and check database information
     */
    private WorkflowFacade Facade = new WorkflowFacade();

    /**
     * Verbs used for HTTP method for the API calling
     */
    private enum VERB {
        POST, GET, PUT, DELETE
    }

    /**
     * Standard API caller used only in this test
     *
     * @param verb Can be GET,POST,PUT,DELETE
     * @param url  complete url calling API
     * @param wf   Workflow information to provide
     * @return the HTTP response
     * @throws IkatsWebClientException if any exception occurs
     */
    private static Response callAPI(VERB verb, String url, Workflow wf) throws Exception {

        Response result;
        Entity<Workflow> wfEntity = Entity.entity(wf, MediaType.APPLICATION_JSON_TYPE);
        switch (verb) {
            case POST:
                result = RequestSender.sendPOSTRequest(getAPIURL() + url, wfEntity);
                break;
            case GET:
                result = RequestSender.sendGETRequest(getAPIURL() + url);
                break;
            case PUT:
                result = RequestSender.sendPUTRequest(getAPIURL() + url, wfEntity);
                break;
            case DELETE:
                result = RequestSender.sendDELETERequest(getAPIURL() + url);
                break;
            default:
                throw new Exception("Bad VERB");
        }

        return result;
    }

    /**
     * Test utils to add easily a new workflow in database
     *
     * @param number Test identifier for the workflow
     * @return the added workflow
     * @throws IkatsDaoException if something fails
     */
    private Workflow addWfToDb(Integer number) throws IkatsDaoException {

        Workflow wf = new Workflow();

        wf.setName("Workflow_" + number.toString());
        wf.setDescription("Description about Workflow_" + number.toString());
        wf.setMacroOp(false);
        wf.setRaw("Raw content " + number.toString());

        Facade.persist(wf);

        return wf;
    }

    @BeforeClass
    public static void setUpBeforClass() {
        AbstractRequestTest.setUpBeforClass(WorkflowResourceTest.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        AbstractRequestTest.tearDownAfterClass(WorkflowResourceTest.class.getSimpleName());
    }

    /**
     * At the beginning of every Unit test, clear the database
     */
    @Before
    public void setUp() throws IkatsDaoException {
        Facade.removeAllWorkflows();
    }

    /**
     * Workflow creation - Nominal case - Database is empty
     *
     * @throws Exception if test fails
     */
    @Test
    public void create_databaseEmpty_201() throws Exception {

        // PREPARE THE DATABASE
        // No data needed in database       

        // PREPARE THE TEST
        Workflow wf = new Workflow();
        wf.setName("My_Workflow");
        wf.setDescription("Description of my new workflow");
        wf.setRaw("Workflow content");

        // DO THE TEST
        Response response = callAPI(VERB.POST, "/wf/", wf);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(201, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);

        URI headerLocation = response.getLocation();
        Pattern pattern = Pattern.compile(".*/wf/([0-9]+)");
        Matcher matcher = pattern.matcher(headerLocation.toString());
        assertEquals(true, matcher.matches());
        String expectedId = matcher.group(1);
        assertEquals(getAPIURL() + "/wf/" + expectedId, headerLocation.toString());

        List<WorkflowEntitySummary> workflowList = Facade.listAllWorkflows();
        assertEquals(1, workflowList.size());

        Response response2 = callAPI(VERB.GET, "/wf/" + expectedId, wf);
        assertEquals(200, response2.getStatus());


    }

    /**
     * Workflow creation - Nominal case - Database contains Workflow
     *
     * @throws Exception if test fails
     */
    @Test
    public void create_databaseFilled_201() throws Exception {

        // PREPARE THE DATABASE
        addWfToDb(1);

        // PREPARE THE TEST
        Workflow wf = new Workflow();
        wf.setName("My_Workflow");
        wf.setDescription("Description of my new workflow");
        wf.setRaw("Workflow content");

        // DO THE TEST
        Response response = callAPI(VERB.POST, "/wf/", wf);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(201, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);

        URI headerLocation = response.getLocation();
        Pattern pattern = Pattern.compile(".*/wf/([0-9]+)");
        Matcher matcher = pattern.matcher(headerLocation.toString());
        assertEquals(true, matcher.matches());
        String expectedId = matcher.group(1);
        assertEquals(getAPIURL() + "/wf/" + expectedId, headerLocation.toString());

        List<WorkflowEntitySummary> workflowList = Facade.listAllWorkflows();
        assertEquals(2, workflowList.size());

        Response response2 = callAPI(VERB.GET, "/wf/" + expectedId, wf);
        assertEquals(200, response2.getStatus());

    }

    /**
     * Workflow creation - Robustness case - Bad Request
     * Empty workflow information
     *
     * @throws Exception if test fails
     */
    @Test
    public void create_400() throws Exception {

        // PREPARE THE DATABASE
        // No data needed in database

        // PREPARE THE TEST
        // No preparation needed

        // DO THE TEST
        Response response = callAPI(VERB.POST, "/wf/", null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(400, status);

        String body = response.readEntity(String.class);
        assertEquals("Wrong inputs", body);

    }

    /**
     * Workflow creation - Robustness case - Method not allowed
     * Creation are not authorized on REST "resources" (only allowed on "collections")
     *
     * @throws Exception if test fails
     */
    @Test
    public void create_405() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        addWfToDb(2);
        Integer id = addWfToDb(3).getId();

        // PREPARE THE TEST
        // Change the name
        Workflow wf = new Workflow();
        wf.setName("Different name");
        wf.setDescription("Different description");
        wf.setRaw("Different raw");


        // DO THE TEST
        Response response = callAPI(VERB.POST, "/wf/" + id.toString(), wf);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(405, status);

        String body = response.readEntity(String.class);
        assertEquals("HTTP 405 Method Not Allowed", body);
    }

    /**
     * Workflow creation - Robustness case - Conflict
     * Workflow with same name already exists in database
     *
     * @throws Exception if test fails
     */
    @Test
    public void create_409() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        Workflow wf = addWfToDb(2);
        addWfToDb(3);

        // PREPARE THE TEST
        // Change the name
        Workflow new_wf = new Workflow();
        new_wf.setName(wf.getName()); // already exists
        new_wf.setDescription("Different description");
        new_wf.setRaw("Different raw");


        // DO THE TEST
        Response response = callAPI(VERB.POST, "/wf", new_wf);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(409, status);

        String body = response.readEntity(String.class);
        assertEquals("Workflow " + new_wf.getName() + " already exists", body);
    }

    /**
     * Workflow list All - Nominal case
     *
     * @throws Exception if test fails
     */
    @Test
    public void listAll_200() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        List<Workflow> wfList = new ArrayList<>();
        wfList.add(addWfToDb(1));
        wfList.add(addWfToDb(2));
        wfList.add(addWfToDb(3));

        // PREPARE THE TEST
        // Fill in the workflow db

        // DO THE TEST
        Response response = callAPI(VERB.GET, "/wf/", null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(200, status);

        List<Workflow> readWorkflowList = response.readEntity(new GenericType<List<Workflow>>() {
        });
        assertEquals(wfList.size(), readWorkflowList.size());
        for (int i = 0; i < wfList.size(); i++) {
            assertEquals(wfList.get(i).getId(), readWorkflowList.get(i).getId());
            assertEquals(wfList.get(i).getName(), readWorkflowList.get(i).getName());
            assertEquals(wfList.get(i).getDescription(), readWorkflowList.get(i).getDescription());
        }


    }

    /**
     * Workflow list All - Robustness case - No workflow stored
     *
     * @throws Exception if test fails
     */
    @Test
    public void listAll_404() throws Exception {

        // PREPARE THE DATABASE
        // No data needed in database

        // PREPARE THE TEST
        // No preparation needed

        // DO THE TEST
        Response response = callAPI(VERB.GET, "/wf/", null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(204, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);
    }

    /**
     * Workflow get - Nominal case
     *
     * @throws Exception if test fails
     */
    @Test
    public void getWorkflow_200() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        Workflow wf = addWfToDb(1);

        // PREPARE THE TEST
        // Nothing to do

        // DO THE TEST
        Response response = callAPI(VERB.GET, "/wf/" + wf.getId().toString(), null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(200, status);

        Workflow readWorkflow = response.readEntity(new GenericType<Workflow>() {
        });
        assertEquals(wf.getId(), readWorkflow.getId());
        assertEquals(wf.getName(), readWorkflow.getName());
        assertEquals(wf.getDescription(), readWorkflow.getDescription());
        assertEquals(wf.getRaw(), readWorkflow.getRaw());
    }

    /**
     * Workflow get - Robustness case - Not found
     * There is no workflow matching this Id
     *
     * @throws Exception if test fails
     */
    @Test
    public void getWorkflow_404() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        addWfToDb(2);
        Integer id = addWfToDb(3).getId();

        // PREPARE THE TEST
        // Set id to an unknown one
        Integer idToRequest = id + 1;

        // DO THE TEST
        Response response = callAPI(VERB.GET, "/wf/" + idToRequest.toString(), null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(404, status);

        String body = response.readEntity(String.class);
        assertEquals("Searching workflow from id=" + idToRequest + ": no resource found, but should exist.", body);
    }

    /**
     * Workflow get - Robustness case - Bad Request
     * The id is badly formatted
     *
     * @throws Exception if test fails
     */
    @Test
    public void getWorkflow_400() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        addWfToDb(2);
        addWfToDb(3);

        // PREPARE THE TEST
        String badId = "bad_id";

        // DO THE TEST
        Response response = callAPI(VERB.GET, "/wf/" + badId, null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(400, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);
    }

    /**
     * Workflow update - Nominal case
     *
     * @throws Exception if test fails
     */
    @Test
    public void updateWorkflow_200() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        Integer id = addWfToDb(2).getId();
        addWfToDb(3);

        // PREPARE THE TEST
        Workflow wf = new Workflow();
        wf.setName("New My_Workflow_1");
        wf.setDescription("New Description of my new workflow");
        wf.setRaw("New Workflow new content");

        // DO THE TEST
        Response response = callAPI(VERB.PUT, "/wf/" + id.toString(), wf);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(200, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);
    }

    /**
     * Workflow update - Robustness case - Bad Request
     * The id is badly formatted
     *
     * @throws Exception if test fails
     */
    @Test
    public void updateWorkflow_400() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        addWfToDb(2);
        addWfToDb(3);

        // PREPARE THE TEST
        String badId = "bad_id";
        Workflow wf = new Workflow();
        wf.setName("New My_Workflow_2");
        wf.setDescription("New Description of my new workflow");
        wf.setRaw("New Workflow new content");

        // DO THE TEST
        Response response = callAPI(VERB.PUT, "/wf/" + badId, wf);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(400, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);
    }

    /**
     * Workflow update - Robustness case - Conflict
     * The workflow name already exists
     *
     * @throws Exception if test fails
     */
    @Test
    public void updateWorkflow_409() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        Workflow wf1 = addWfToDb(1);
        Workflow wf2 = addWfToDb(2);

        // PREPARE THE TEST
        wf2.setName(wf1.getName()); // this name already exists in database
        wf2.setDescription("New Description of my workflow");
        wf2.setRaw("New content of my workflow");

        // DO THE TEST
        Response response = callAPI(VERB.PUT, "/wf/" + wf2.getId(), wf2);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(409, status);

        String body = response.readEntity(String.class);
        assertEquals("Workflow " + wf2.getName() + " already exists", body);
    }

    /**
     * Workflow update - Robustness case - Not found
     * There is no workflow matching this Id
     *
     * @throws Exception if test fails
     */
    @Test
    public void updateWorkflow_404() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        addWfToDb(2);
        Integer id = addWfToDb(3).getId();

        // PREPARE THE TEST
        Integer unknownId = id + 1;

        Workflow wf = new Workflow();
        wf.setName("New My_Workflow_3");
        wf.setDescription("New Description of my workflow");
        wf.setRaw("New Workflow content");

        // DO THE TEST
        Response response = callAPI(VERB.PUT, "/wf/" + unknownId.toString(), wf);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(404, status);

        String body = response.readEntity(String.class);
        assertEquals("No match for Workflow with id:" + unknownId.toString(), body);
    }

    /**
     * Workflow update all - Robustness Case - Not implemented
     *
     * @throws Exception if test fails
     */
    @Test
    public void updateAll_501() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        addWfToDb(2);
        addWfToDb(3);

        Workflow wf = new Workflow();

        // PREPARE THE TEST
        // Nothing to do

        // DO THE TEST
        Response response = callAPI(VERB.PUT, "/wf/", wf);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(501, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);

    }

    /**
     * Workflow deletion - Nominal case
     *
     * @throws Exception if test fails
     */
    @Test
    public void removeWorkflow_204() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        Integer id = addWfToDb(2).getId();
        addWfToDb(3);

        // PREPARE THE TEST
        // Nothing to do

        // DO THE TEST
        Response response = callAPI(VERB.DELETE, "/wf/" + id.toString(), null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(204, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);
    }

    /**
     * Workflow deletion - Robustness case - Bad Request
     * The id is badly formatted
     *
     * @throws Exception if test fails
     */
    @Test
    public void removeWorkflow_400() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        addWfToDb(2);
        addWfToDb(3);

        // PREPARE THE TEST
        String badId = "bad_id";

        // DO THE TEST
        Response response = callAPI(VERB.DELETE, "/wf/" + badId, null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(400, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);
    }

    /**
     * Workflow get - Robustness case - Not found
     * There is no workflow matching this Id
     *
     * @throws Exception if test fails
     */
    @Test
    public void removeWorkflow_404() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        addWfToDb(2);
        Integer id = addWfToDb(3).getId();

        // PREPARE THE TEST
        Integer unknownId = id + 1;

        // DO THE TEST
        Response response = callAPI(VERB.DELETE, "/wf/" + unknownId.toString(), null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(404, status);

        String body = response.readEntity(String.class);
        assertEquals("No workflow exists with Id:" + unknownId.toString(), body);
    }

    /**
     * All Workflow deletion - Nominal case
     *
     * @throws Exception if test fails
     */
    @Test
    public void removeAll_204() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        addWfToDb(2);
        addWfToDb(3);

        // PREPARE THE TEST
        // Nothing to do

        // DO THE TEST
        Response response = callAPI(VERB.DELETE, "/wf/", null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(204, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);
    }

    /**
     * All Workflow deletion - Robustness case - Not Found
     * There was no workflow stored
     *
     * @throws Exception if test fails
     */
    @Test
    public void removeAll_204_empty() throws Exception {

        // PREPARE THE DATABASE
        // Database is empty

        // PREPARE THE TEST
        // Nothing to do

        // DO THE TEST
        Response response = callAPI(VERB.DELETE, "/wf/", null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(204, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);
    }

}
