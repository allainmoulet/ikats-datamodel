package fr.cs.ikats.temporaldata.resource;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.workflow.Workflow;
import fr.cs.ikats.workflow.WorkflowFacade;
import org.apache.log4j.Logger;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.util.List;

/**
 * This class hosts all the operations on Workflow
 */
@Path("wf")
public class WorkflowResource extends AbstractResource {

    private static Logger logger = Logger.getLogger(MetaDataResource.class);
    private WorkflowFacade Facade = new WorkflowFacade();

    /**
     * Default constructor
     */
    public WorkflowResource() {
        super();
    }

    /**
     * create a new Workflow
     *
     * @param wf      Workflow to provide
     * @param uriInfo the uri info
     * @return the id of the created workflow
     * @throws IkatsDaoException if any DAO exception occurs
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response create(
            Workflow wf,
            @Context UriInfo uriInfo
    ) throws IkatsDaoException {

        try {
            wf.setMacroOp(false);
        } catch (NullPointerException e) {
            throw new IkatsDaoException("Wrong inputs");
        }

        Integer wfId = Facade.persist(wf);

        // Return the location header
        UriBuilder uri = uriInfo.getAbsolutePathBuilder();
        uri.path(wfId.toString());

        return Response.created(uri.build()).build();
    }

    /**
     * Get the list of all workflow summary (raw content is not provided unless full is set to true)
     *
     * @param full to indicate if the raw content shall be included in response
     * @return the workflow
     * @throws IkatsDaoException if any DAO exception occurs
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response listAll(
            @QueryParam("full") @DefaultValue("false") Boolean full
    ) throws IkatsDaoException {

        List<Workflow> result = Facade.listAllWorkflows();
        if (!full) {
            // Remove Raw content from data to reduce the payload
            result.forEach(wf -> wf.setRaw(null));
        }
        // Internal flags must not be provided
        result.forEach(wf -> wf.setMacroOp(null));

        Response.StatusType resultStatus = Response.Status.OK;
        if (result.size() == 0) {
            resultStatus = Response.Status.NO_CONTENT;
        }

        return Response.status(resultStatus).entity(result).build();

    }

    /**
     * Get the content of a workflow by providing its id
     *
     * @param id id of the workflow to read
     * @return the workflow
     * @throws IkatsDaoException if any DAO exception occurs
     */
    @GET
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWorkflow(
            @PathParam("id") Integer id
    ) throws IkatsDaoException {

        Workflow wf = Facade.getById(id);

        // Internal flags must not be provided
        wf.setMacroOp(null);

        return Response.status(Response.Status.OK).entity(wf).build();
    }

    /**
     * Update all workflow at once
     *
     * @return HTTP response
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateWorkflow() {
        return Response.status(Response.Status.NOT_IMPLEMENTED).build();
    }

    /**
     * Update a workflow identified by its Id
     *
     * @param wf      New content for the workflow
     * @param uriInfo the uri info
     * @param id      id of the workflow to update
     * @return HTTP response
     * @throws IkatsDaoException if any DAO exception occurs
     */
    @PUT
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateWorkflow(
            Workflow wf,
            @Context UriInfo uriInfo,
            @PathParam("id") int id
    ) throws IkatsDaoException, IkatsWebClientException {

        if (wf.getId() != null && id != wf.getId()) {
            throw new IkatsWebClientException("Mismatch in request with Id between URI and body part");
        }
        wf.setId(id);
        wf.setMacroOp(false);

        boolean result = Facade.update(wf);

        if (result) {
            return Response.status(Response.Status.OK).build();
        } else {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Delete a workflow identified by its id
     *
     * @param id id of the workflow to delete
     * @return HTTP response
     * @throws IkatsDaoException if any DAO exception occurs
     */
    @DELETE
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeWorkflow(
            @PathParam("id") Integer id
    ) throws IkatsDaoException {
        Facade.removeById(id);
        return Response.status(Response.Status.NO_CONTENT).build();
    }

    /**
     * Delete all workflow
     *
     * @return HTTP response
     * @throws IkatsDaoException if any DAO exception occurs
     */
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeAll() throws IkatsDaoException {

        Facade.removeAllWorkflows();

        return Response.status(Response.Status.NO_CONTENT).build();
    }
}