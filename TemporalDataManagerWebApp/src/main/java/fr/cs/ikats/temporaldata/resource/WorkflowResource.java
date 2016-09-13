package fr.cs.ikats.temporaldata.resource;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.workflow.Workflow;
import fr.cs.ikats.workflow.WorkflowFacade;
import org.apache.log4j.Logger;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.util.ArrayList;
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
     * Create a new Workflow
     *
     * @param wf      Workflow to provide
     * @param uriInfo the uri info
     * @return the id of the created workflow
     * @throws IkatsDaoConflictException if the id of the workflow already exists
     * @throws IkatsDaoException         if any DAO exception occurs
     */
    @POST
    @Path("/")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response Create(
            Workflow wf,
            @Context UriInfo uriInfo
    ) throws IkatsDaoConflictException, IkatsDaoException {

        if (wf.getId() != null) {
            throw new IkatsDaoException("Workflow should not have an Id");
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
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Response ListAll(
            @QueryParam("full") @DefaultValue("false") Boolean full
    ) throws IkatsDaoException {

        List<Workflow> result = new ArrayList<Workflow>();
        List<Workflow> fullResults = Facade.listAll();
        if (full) {
            result = fullResults;
        } else {
            // Filter results to not provide raw content
            for (Workflow workflowItem : fullResults) {
                Workflow outputWf = new Workflow();
                outputWf.setName(workflowItem.getName());
                outputWf.setDescription(workflowItem.getDescription());
                result.add(outputWf);
            }
        }

        return Response.status(Response.Status.OK).entity(result).build();

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

        return Response.status(Response.Status.OK).entity(wf).build();
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
            @PathParam("id") Integer id
    ) throws IkatsDaoException {

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
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeAll() throws IkatsDaoException {

        Facade.removeAll();

        return Response.status(Response.Status.NO_CONTENT).build();
    }

}
