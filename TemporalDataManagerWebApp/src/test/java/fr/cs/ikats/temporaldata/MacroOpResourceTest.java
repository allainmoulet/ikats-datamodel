package fr.cs.ikats.temporaldata;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.datamanager.client.RequestSender;
import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.workflow.Workflow;
import fr.cs.ikats.workflow.WorkflowFacade;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class MacroOpResourceTest extends AbstractRequestTest {
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
     * @param mo   Macro operator information to provide
     * @return the HTTP response
     * @throws IkatsWebClientException if any exception occurs
     */
    private static Response callAPI(VERB verb, String url, Workflow mo) throws Exception {

        Response result;
        Entity<Workflow> wfEntity = Entity.entity(mo, MediaType.APPLICATION_JSON_TYPE);
        switch (verb) {
            case POST:
                result = RequestSender.sendPOSTRequest(getAPIURL() + url, wfEntity);
                break;
            case GET:
                result = RequestSender.sendGETRequest(getAPIURL() + url, null);
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

    /**
     * Test utils to add easily a new macro operator in database
     *
     * @param number Test identifier for the macro operator
     * @return the added macro operator
     * @throws IkatsDaoException if something fails
     */
    private Workflow addMOToDb(Integer number) throws IkatsDaoException {

        Workflow mo = new Workflow();

        mo.setName("MacroOp_" + number.toString());
        mo.setDescription("Description about MacroOp_" + number.toString());
        mo.setMacroOp(true);
        mo.setRaw("Raw content " + number.toString());

        Facade.persist(mo);

        return mo;
    }

    @BeforeClass
    public static void setUpBeforClass() {
        AbstractRequestTest.setUpBeforClass(MacroOpResourceTest.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        AbstractRequestTest.tearDownAfterClass(MacroOpResourceTest.class.getSimpleName());
    }

    /**
     * At the beginning of every Unit test, clear the database
     */
    @Before
    public void setUp() throws IkatsDaoException {
        Facade.removeAllMacroOp();
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
        Workflow mo = new Workflow();
        mo.setName("My_Macro_Operator");
        mo.setDescription("Description of my new macro operator");
        mo.setRaw("Macro operator content");

        // DO THE TEST
        Response response = callAPI(VERB.POST, "/mo/", mo);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(201, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);

        URI headerLocation = response.getLocation();
        Pattern pattern = Pattern.compile(".*/mo/([0-9]+)");
        Matcher matcher = pattern.matcher(headerLocation.toString());
        assertEquals(true, matcher.matches());
        String expectedId = matcher.group(1);
        assertEquals(getAPIURL() + "/mo/" + expectedId, headerLocation.toString());

        List<Workflow> macroOpList = Facade.listAllMacroOp();
        assertEquals(1, macroOpList.size());

        Response response2 = callAPI(VERB.GET, "/mo/" + expectedId, mo);
        assertEquals(200, response2.getStatus());


    }

    /**
     * Macro operator creation - Nominal case - Database contains Macro operator
     *
     * @throws Exception if test fails
     */
    @Test
    public void create_databaseFilled_201() throws Exception {

        // PREPARE THE DATABASE
        addMOToDb(1);

        // PREPARE THE TEST
        Workflow mo = new Workflow();
        mo.setName("My_Macro_Operator");
        mo.setDescription("Description of my new macro operator");
        mo.setRaw("Macro operator content");

        // DO THE TEST
        Response response = callAPI(VERB.POST, "/mo/", mo);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(201, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);

        URI headerLocation = response.getLocation();
        Pattern pattern = Pattern.compile(".*/mo/([0-9]+)");
        Matcher matcher = pattern.matcher(headerLocation.toString());
        assertEquals(true, matcher.matches());
        String expectedId = matcher.group(1);
        assertEquals(getAPIURL() + "/mo/" + expectedId, headerLocation.toString());

        List<Workflow> macroOpList = Facade.listAllMacroOp();
        assertEquals(2, macroOpList.size());

        Response response2 = callAPI(VERB.GET, "/mo/" + expectedId, mo);
        assertEquals(200, response2.getStatus());

    }

    /**
     * Macro operator creation - Robustness case - Bad Request
     * Empty Macro operator information
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
        Response response = callAPI(VERB.POST, "/mo/", null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(400, status);

        String body = response.readEntity(String.class);
        assertEquals("Wrong inputs", body);

    }

    /**
     * Macro operator creation - Robustness case - Method not allowed
     * Creation are not authorized on REST "resources" (only allowed on "collections")
     *
     * @throws Exception if test fails
     */
    @Test
    public void create_405() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addMOToDb(1);
        addMOToDb(2);
        Integer id = addMOToDb(3).getId();

        // PREPARE THE TEST
        // Change the name
        Workflow mo = new Workflow();
        mo.setName("Different name");
        mo.setDescription("Different description");
        mo.setRaw("Different raw");


        // DO THE TEST
        Response response = callAPI(VERB.POST, "/mo/" + id.toString(), mo);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(405, status);

        String body = response.readEntity(String.class);
        assertEquals("HTTP 405 Method Not Allowed", body);
    }

    /**
     * Macro operator list All - Nominal case
     *
     * @throws Exception if test fails
     */
    @Test
    public void listAll_200() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        List<Workflow> wfList = new ArrayList<>();
        wfList.add(addMOToDb(1));
        wfList.add(addMOToDb(2));
        wfList.add(addMOToDb(3));

        // PREPARE THE TEST
        // Fill in the workflow db

        // DO THE TEST
        Response response = callAPI(VERB.GET, "/mo/", null);

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
            assertEquals(null, readWorkflowList.get(i).getRaw());
        }


    }

    /**
     * Macro operator list All - Nominal case - with full content
     *
     * @throws Exception if test fails
     */
    @Test
    public void listAll_full_200() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        List<Workflow> wfList = new ArrayList<>();
        wfList.add(addMOToDb(1));
        wfList.add(addMOToDb(2));
        wfList.add(addMOToDb(3));

        // PREPARE THE TEST
        // Fill in the workflow db

        // DO THE TEST
        Response response = callAPI(VERB.GET, "/mo/?full=true", null);

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
            assertEquals(wfList.get(i).getRaw(), readWorkflowList.get(i).getRaw());
        }

    }

    /**
     * Macro operator list All - Robustness case - No Macro operator stored
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
        Response response = callAPI(VERB.GET, "/mo/", null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(204, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);
    }

    /**
     * Macro operator get - Nominal case
     *
     * @throws Exception if test fails
     */
    @Test
    public void getWorkflow_200() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db with a new macro operator
        Workflow mo = addMOToDb(1);

        // PREPARE THE TEST
        // Nothing to do

        // DO THE TEST
        Response response = callAPI(VERB.GET, "/mo/" + mo.getId().toString(), null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(200, status);

        Workflow readWorkflow = response.readEntity(new GenericType<Workflow>() {
        });
        assertEquals(mo.getId(), readWorkflow.getId());
        assertEquals(mo.getName(), readWorkflow.getName());
        assertEquals(mo.getDescription(), readWorkflow.getDescription());
        assertEquals(mo.getRaw(), readWorkflow.getRaw());
    }

    /**
     * Macro operator get - Robustness case - Not found
     * There is no Macro operator matching this Id
     *
     * @throws Exception if test fails
     */
    @Test
    public void getWorkflow_404() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addMOToDb(1);
        addMOToDb(2);
        Integer id = addMOToDb(3).getId();

        // PREPARE THE TEST
        // Set id to an unknown one
        Integer idToRequest = id + 1;

        // DO THE TEST
        Response response = callAPI(VERB.GET, "/mo/" + idToRequest.toString(), null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(404, status);

        String body = response.readEntity(String.class);
        assertEquals("Searching workflow from id=" + idToRequest + ": no resource found, but should exist.", body);
    }

    /**
     * Macro operator get - Robustness case - Bad Request
     * The id is badly formatted
     *
     * @throws Exception if test fails
     */
    @Test
    public void getWorkflow_400() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addMOToDb(1);
        addMOToDb(2);
        addMOToDb(3);

        // PREPARE THE TEST
        String badId = "bad_id";

        // DO THE TEST
        Response response = callAPI(VERB.GET, "/mo/" + badId, null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(400, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);
    }

    /**
     * Macro operator update - Nominal case
     *
     * @throws Exception if test fails
     */
    @Test
    public void updateWorkflow_200() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addMOToDb(1);
        Integer id = addMOToDb(2).getId();
        addMOToDb(3);

        // PREPARE THE TEST
        Workflow mo = new Workflow();
        mo.setName("New My_Workflow");
        mo.setDescription("New Description of my new workflow");
        mo.setRaw("New Workflow new content");

        // DO THE TEST
        Response response = callAPI(VERB.PUT, "/mo/" + id.toString(), mo);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(200, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);
    }

    /**
     * Macro operator update - Robustness case - Bad Request
     * The id is badly formatted
     *
     * @throws Exception if test fails
     */
    @Test
    public void updateWorkflow_400() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addMOToDb(1);
        addMOToDb(2);
        addMOToDb(3);

        // PREPARE THE TEST
        String badId = "bad_id";
        Workflow mo = new Workflow();
        mo.setName("New My_Workflow");
        mo.setDescription("New Description of my new workflow");
        mo.setRaw("New Workflow new content");

        // DO THE TEST
        Response response = callAPI(VERB.PUT, "/mo/" + badId, mo);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(400, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);
    }

    /**
     * Macro operator update - Robustness case - Not found
     * There is no Macro operator matching this Id
     *
     * @throws Exception if test fails
     */
    @Test
    public void updateWorkflow_404() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addMOToDb(1);
        addMOToDb(2);
        Integer id = addMOToDb(3).getId();

        // PREPARE THE TEST
        Integer unknownId = id + 1;

        Workflow wf = new Workflow();
        wf.setName("New My_Workflow");
        wf.setDescription("New Description of my workflow");
        wf.setRaw("New Workflow content");

        // DO THE TEST
        Response response = callAPI(VERB.PUT, "/mo/" + unknownId.toString(), wf);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(404, status);

        String body = response.readEntity(String.class);
        assertEquals("No match for Workflow with id:" + unknownId.toString(), body);
    }

    /**
     * Macro operator update all - Robustness Case - Not implemented
     *
     * @throws Exception if test fails
     */
    @Test
    public void updateAll_501() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addMOToDb(1);
        addMOToDb(2);
        addMOToDb(3);

        Workflow mo = new Workflow();

        // PREPARE THE TEST
        // Nothing to do

        // DO THE TEST
        Response response = callAPI(VERB.PUT, "/mo/", mo);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(501, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);

    }

    /**
     * Macro operator deletion - Nominal case
     *
     * @throws Exception if test fails
     */
    @Test
    public void removeWorkflow_204() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addMOToDb(1);
        Integer id = addMOToDb(2).getId();
        addMOToDb(3);

        // PREPARE THE TEST
        // Nothing to do

        // DO THE TEST
        Response response = callAPI(VERB.DELETE, "/mo/" + id.toString(), null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(204, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);

        response = callAPI(VERB.GET, "/mo/", null);
        List<Workflow> readMacroOpList = response.readEntity(new GenericType<List<Workflow>>() {
        });
        assertEquals(2,readMacroOpList.size());    }

    /**
     * Macro operator deletion - Robustness case - Bad Request
     * The id is badly formatted
     *
     * @throws Exception if test fails
     */
    @Test
    public void removeWorkflow_400() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addMOToDb(1);
        addMOToDb(2);
        addMOToDb(3);

        // PREPARE THE TEST
        String badId = "bad_id";

        // DO THE TEST
        Response response = callAPI(VERB.DELETE, "/mo/" + badId, null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(400, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);
    }

    /**
     * Macro operator get - Robustness case - Not found
     * There is no Macro operator matching this Id
     *
     * @throws Exception if test fails
     */
    @Test
    public void removeWorkflow_404() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addMOToDb(1);
        addMOToDb(2);
        Integer id = addMOToDb(3).getId();

        // PREPARE THE TEST
        Integer unknownId = id + 1;

        // DO THE TEST
        Response response = callAPI(VERB.DELETE, "/mo/" + unknownId.toString(), null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(404, status);

        String body = response.readEntity(String.class);
        assertEquals("No workflow exists with Id:" + unknownId.toString(), body);
    }

    /**
     * All Macro operator deletion - Nominal case
     *
     * @throws Exception if test fails
     */
    @Test
    public void removeAll_204() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addMOToDb(1);
        addMOToDb(2);
        addMOToDb(3);

        // PREPARE THE TEST
        // Nothing to do

        // DO THE TEST
        Response response = callAPI(VERB.DELETE, "/mo/", null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(204, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);
    }

    /**
     * All Macro operator deletion - Nominal case - with Workflow present in database
     *
     * @throws Exception if test fails
     */
    @Test
    public void removeAll_with_workflow_204() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(0);
        addMOToDb(1);
        addMOToDb(2);
        addMOToDb(3);

        // PREPARE THE TEST
        // Nothing to do

        // DO THE TEST
        Response response = callAPI(VERB.DELETE, "/mo/", null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(204, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);

        response = callAPI(VERB.GET, "/wf/", null);
        status = response.getStatus();
        assertEquals(200, status);

        List<Workflow> readWorkflowList = response.readEntity(new GenericType<List<Workflow>>() {
        });
        assertEquals(1,readWorkflowList.size());
    }

    /**
     * All Macro operator deletion - Robustness case - Not Found
     * There was no Macro operator stored
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
        Response response = callAPI(VERB.DELETE, "/mo/", null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(204, status);

        String body = response.readEntity(String.class);
        assertEquals("", body);
    }

}