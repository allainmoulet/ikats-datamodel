package fr.cs.ikats.temporaldata;

import fr.cs.ikats.datamanager.client.RequestSender;
import fr.cs.ikats.workflow.Workflow;
import org.junit.AfterClass;
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


    @BeforeClass
    public static void setUpBeforClass() {
        AbstractRequestTest.setUpBeforClass(QueryRequestTest.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        AbstractRequestTest.tearDownAfterClass(QueryRequestTest.class.getSimpleName());
    }

    @Test
    public void create_201() throws Exception {
        Workflow wf = new Workflow();

        wf.setName("My_Workflow");
        wf.setDescription("Description of my new workflow");
        wf.setRaw("Workflow content");

        Entity<Workflow> wfEntity = Entity.entity(wf, MediaType.APPLICATION_JSON_TYPE);

        String ROOT_URL = getAPIURL() + "/wf/";
        Response response = RequestSender.sendPOSTRequest(ROOT_URL, wfEntity);
        int status = response.getStatus();
        assertEquals(201, status);
    }

    @Test
    public void create_400() throws Exception {

        String ROOT_URL = getAPIURL() + "/wf/";
        Response response = RequestSender.sendPOSTRequest(ROOT_URL, null);
        int status = response.getStatus();
        assertEquals(400, status);
    }

    @Test
    public void listAll_200() throws Exception {

        String ROOT_URL = getAPIURL() + "/wf/";
        Response response = RequestSender.sendGETRequest(ROOT_URL, null);
        int status = response.getStatus();
        assertEquals(200, status);
    }

    @Test
    public void getWorkflow_404() throws Exception {

        String ROOT_URL = getAPIURL() + "/wf/0";
        Response response = RequestSender.sendGETRequest(ROOT_URL, null);
        int status = response.getStatus();
        assertEquals(404, status);
    }

    @Test
    public void getWorkflow_400() throws Exception {
        String ROOT_URL = getAPIURL() + "/wf/bad_id";
        Response response = RequestSender.sendGETRequest(ROOT_URL, null);
        int status = response.getStatus();
        assertEquals(400, status);
    }

    @Test
    public void updateWorkflow_200() throws Exception {
        String ROOT_URL = getAPIURL() + "/wf/0";
        Response response = RequestSender.sendPUTRequest(ROOT_URL, null);
        int status = response.getStatus();
        assertEquals(200, status);
    }

    @Test
    public void updateWorkflow_400() throws Exception {
        String ROOT_URL = getAPIURL() + "/wf/bad_id";
        Response response = RequestSender.sendPUTRequest(ROOT_URL, null);
        int status = response.getStatus();
        assertEquals(400, status);
    }

    @Test
    public void updateWorkflow_404() throws Exception {
        String ROOT_URL = getAPIURL() + "/wf/99";
        Response response = RequestSender.sendPUTRequest(ROOT_URL, null);
        int status = response.getStatus();
        assertEquals(404, status);
    }

    @Test
    public void updateAll_501() throws Exception {
        String ROOT_URL = getAPIURL() + "/wf/";
        Response response = RequestSender.sendPUTRequest(ROOT_URL, null);
        int status = response.getStatus();
        assertEquals(501, status);
    }

    @Test
    public void removeWorkflow_204() throws Exception {
        String ROOT_URL = getAPIURL() + "/wf/0";
        Response response = RequestSender.sendDELETERequest(ROOT_URL);
        int status = response.getStatus();
        assertEquals(204, status);
    }

    @Test
    public void removeWorkflow_400() throws Exception {
        String ROOT_URL = getAPIURL() + "/wf/bad_id";
        Response response = RequestSender.sendDELETERequest(ROOT_URL);
        int status = response.getStatus();
        assertEquals(400, status);
    }

    @Test
    public void removeWorkflow_404() throws Exception {
        String ROOT_URL = getAPIURL() + "/wf/99";
        Response response = RequestSender.sendDELETERequest(ROOT_URL);
        int status = response.getStatus();
        assertEquals(404, status);
    }

    @Test
    public void removeAll_204() throws Exception {
        String ROOT_URL = getAPIURL() + "/wf/";
        Response response = RequestSender.sendDELETERequest(ROOT_URL);
        int status = response.getStatus();
        assertEquals(204, status);
    }

}