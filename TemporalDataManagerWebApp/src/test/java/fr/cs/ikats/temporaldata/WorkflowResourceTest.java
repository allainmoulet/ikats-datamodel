package fr.cs.ikats.temporaldata;

import fr.cs.ikats.common.dao.DataBaseDAO;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.datamanager.client.RequestSender;
import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.workflow.Workflow;
import fr.cs.ikats.workflow.WorkflowFacade;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;

/**
 * Created by ftortora on 13/09/16.
 */
public class WorkflowResourceTest extends AbstractRequestTest {
    private WorkflowFacade Facade = new WorkflowFacade();
    private static Logger LOGGER = Logger.getLogger(DataBaseDAO.class);

    /**
     * Standard API caller used only in this test
     *
     * @param verb Can be GET,POST,PUT,DELETE
     * @param url  complete url calling API
     * @param wf   Workflow information to provide
     * @return the HTTP response
     * @throws IkatsWebClientException
     */
    private static Response callAPI(String verb, String url, Workflow wf) throws Exception {

        Response result;
        Entity<Workflow> wfEntity = Entity.entity(wf, MediaType.APPLICATION_JSON_TYPE);
        switch (verb) {
            case "POST":
                result = RequestSender.sendPOSTRequest(getAPIURL() + url, wfEntity);
                break;
            case "GET":
                result = RequestSender.sendGETRequest(getAPIURL() + url, null);
                break;
            case "PUT":
                result = RequestSender.sendPUTRequest(getAPIURL() + url, wfEntity);
                break;
            case "DELETE":
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
     * @return
     * @throws IkatsDaoException
     */
    private Integer addWfToDb(Integer number) throws IkatsDaoException {

        Workflow wf = new Workflow();

        wf.setName("Workflow_" + number.toString());
        wf.setDescription("Description about Workflow_" + number.toString());
        wf.setRaw("Raw content " + number.toString());

        return Facade.persist(wf);
    }

    @BeforeClass
    public static void setUpBeforClass() {
        AbstractRequestTest.setUpBeforClass(QueryRequestTest.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        AbstractRequestTest.tearDownAfterClass(QueryRequestTest.class.getSimpleName());
    }

    /**
     * At the beginning of every Unit test, clear the database
     */
    @Before
    public void setUp() throws IkatsDaoException {
        Facade.removeAll();
    }

    /**
     * Workflow creation - Nominal case
     *
     * @throws Exception
     */
    @Test
    public void create_201() throws Exception {

        // PREPARE THE DATABASE
        // No data needed in database       

        // PREPARE THE TEST
        Workflow wf = new Workflow();
        wf.setName("My_Workflow");
        wf.setDescription("Description of my new workflow");
        wf.setRaw("Workflow content");

        // DO THE TEST
        Response response = callAPI("POST", "/wf/", wf);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(201, status);
    }

    /**
     * Workflow creation - Robustness case - Bad Request
     * Empty workflow information
     *
     * @throws Exception
     */
    @Test
    public void create_400() throws Exception {

        // PREPARE THE DATABASE
        // No data needed in database

        // PREPARE THE TEST
        // No preparation needed

        // DO THE TEST
        Response response = callAPI("POST", "/wf/", null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(400, status);
    }

    /**
     * Workflow creation - Robustness case - Method not allowed
     * Creation are not authorized on REST "resources" (only allowed on "collections")
     *
     * @throws Exception
     */
    @Test
    public void create_405() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        addWfToDb(2);
        Integer id = addWfToDb(3);

        // PREPARE THE TEST
        // Change the name
        Workflow wf = new Workflow();
        wf.setName("Different name");
        wf.setDescription("Different description");
        wf.setRaw("Different raw");


        // DO THE TEST
        Response response = callAPI("POST", "/wf/" + id.toString(), wf);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(405, status);
    }

    /**
     * Workflow list All - Nominal case
     *
     * @throws Exception
     */
    @Test
    public void listAll_200() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        Integer id1 = addWfToDb(1);
        Integer id2 = addWfToDb(2);
        Integer id3 = addWfToDb(3);

        // PREPARE THE TEST
        // Fill in the workflow db

        // DO THE TEST
        Response response = callAPI("GET", "/wf/", null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(200, status);
    }

    /**
     * Workflow list All - Robustness case - No workflow stored
     *
     * @throws Exception
     */
    @Test
    public void listAll_404() throws Exception {

        // PREPARE THE DATABASE
        // No data needed in database

        // PREPARE THE TEST
        // No preparation needed

        // DO THE TEST
        Response response = callAPI("GET", "/wf/", null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(204, status);
    }

    /**
     * Workflow get - Nominal case
     *
     * @throws Exception
     */
    @Test
    public void getWorkflow_200() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        Integer id = addWfToDb(1);

        // PREPARE THE TEST
        // Nothing to do

        // DO THE TEST
        Response response = callAPI("GET", "/wf/" + id.toString(), null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(200, status);
    }

    /**
     * Workflow get - Robustness case - Not found
     * There is no workflow matching this Id
     *
     * @throws Exception
     */
    @Test
    public void getWorkflow_404() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        addWfToDb(2);
        Integer id = addWfToDb(3);

        // PREPARE THE TEST
        // Set id to an unknown one
        Integer idToRequest = id + 1;

        // DO THE TEST
        Response response = callAPI("GET", "/wf/" + idToRequest.toString(), null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(404, status);
    }

    /**
     * Workflow get - Robustness case - Bad Request
     * The id is badly formatted
     *
     * @throws Exception
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
        Response response = callAPI("GET", "/wf/" + badId, null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(400, status);
    }

    /**
     * Workflow update - Nominal case
     *
     * @throws Exception
     */
    @Test
    public void updateWorkflow_200() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        Integer id = addWfToDb(2);
        addWfToDb(3);

        // PREPARE THE TEST
        Workflow wf = new Workflow();
        wf.setId(id);
        wf.setName("New My_Workflow");
        wf.setDescription("New Description of my new workflow");
        wf.setRaw("New Workflow new content");

        // DO THE TEST
        Response response = callAPI("PUT", "/wf/" + id.toString(), wf);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(200, status);
    }

    /**
     * Workflow update - Robustness case - Bad Request
     * The id is badly formatted
     *
     * @throws Exception
     */
    @Test
    public void updateWorkflow_400() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        Integer id = addWfToDb(2);
        addWfToDb(3);

        // PREPARE THE TEST
        String badId = "bad_id";
        Workflow wf = new Workflow();
        wf.setId(id);
        wf.setName("New My_Workflow");
        wf.setDescription("New Description of my new workflow");
        wf.setRaw("New Workflow new content");

        // DO THE TEST
        Response response = callAPI("PUT", "/wf/" + badId, wf);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(400, status);
    }

    /**
     * Workflow update - Robustness case - Not found
     * There is no workflow matching this Id
     *
     * @throws Exception
     */
    @Test
    public void updateWorkflow_404() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        addWfToDb(2);
        Integer id = addWfToDb(3);

        // PREPARE THE TEST
        Integer unknownId = id + 1;

        Workflow wf = new Workflow();
        wf.setId(unknownId);
        wf.setName("New My_Workflow");
        wf.setDescription("New Description of my workflow");
        wf.setRaw("New Workflow content");

        // DO THE TEST
        Response response = callAPI("PUT", "/wf/" + unknownId.toString(), wf);

        //TODO debug got 200

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(404, status);
    }

    /**
     * Workflow update all - Robustness Case - Not implemented
     *
     * @throws Exception
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
        Response response = callAPI("PUT", "/wf/", wf);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(501, status);
    }

    /**
     * Workflow deletion - Nominal case
     *
     * @throws Exception
     */
    @Test
    public void removeWorkflow_204() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        Integer id = addWfToDb(2);
        addWfToDb(3);

        // PREPARE THE TEST
        // Nothing to do

        // DO THE TEST
        Response response = callAPI("DELETE", "/wf/" + id.toString(), null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(204, status);
    }

    /**
     * Workflow deletion - Robustness case - Bad Request
     * The id is badly formatted
     *
     * @throws Exception
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
        Response response = callAPI("DELETE", "/wf/" + badId, null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(400, status);
    }

    /**
     * Workflow get - Robustness case - Not found
     * There is no workflow matching this Id
     *
     * @throws Exception
     */
    @Test
    public void removeWorkflow_404() throws Exception {

        // PREPARE THE DATABASE
        // Fill in the workflow db
        addWfToDb(1);
        addWfToDb(2);
        Integer id = addWfToDb(3);

        // PREPARE THE TEST
        Integer unknownId = id + 1;

        // DO THE TEST
        Response response = callAPI("DELETE", "/wf/" + unknownId.toString(), null);

        //TODO debug got 400

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(404, status);
    }

    /**
     * All Workflow deletion - Nominal case
     *
     * @throws Exception
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
        Response response = callAPI("DELETE", "/wf/", null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(204, status);
    }

    /**
     * All Workflow deletion - Robustness case - Not Found
     * There was no workflow stored
     *
     * @throws Exception
     */
    @Test
    public void removeAll_204_empty() throws Exception {

        // PREPARE THE DATABASE
        // Database is empty

        // PREPARE THE TEST
        // Nothing to do

        // DO THE TEST
        Response response = callAPI("DELETE", "/wf/", null);

        // CHECK RESULTS
        int status = response.getStatus();
        assertEquals(204, status);
    }

}