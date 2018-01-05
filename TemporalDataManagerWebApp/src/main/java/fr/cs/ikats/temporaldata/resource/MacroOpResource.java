/**
 * LICENSE:
 * --------
 * Copyright 2017 CS SYSTEMES D'INFORMATION
 *
 * Licensed to CS SYSTEMES D'INFORMATION under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. CS licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * @author Fabien TORTORA <fabien.tortora@c-s.fr>
 * @author Pierre BONHOURE <pierre.bonhoure@c-s.fr>
 */

package fr.cs.ikats.temporaldata.resource;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.workflow.Workflow;
import fr.cs.ikats.workflow.WorkflowFacade;

/**
 * This class hosts all the operations on Workflow
 */
@Path("mo")
public class MacroOpResource extends AbstractResource {

    private static Logger logger = Logger.getLogger(MetaDataResource.class);
    private WorkflowFacade Facade = new WorkflowFacade();

    /**
     * Default constructor
     */
    public MacroOpResource() {
        super();
    }

    /**
     * create a new Macro Operator
     *
     * @param wf      Macro Operator to provide
     * @param uriInfo the uri info
     * @return the id of the created Macro Operator
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
            wf.setMacroOp(true);
        } catch (NullPointerException e) {
            throw new IkatsDaoException("Wrong inputs", e);
        }

        Integer wfId = Facade.persist(wf);

        // Return the location header
        UriBuilder uri = uriInfo.getAbsolutePathBuilder();
        uri.path(wfId.toString());

        return Response.created(uri.build()).build();
    }

    /**
     * Get the list of all Macro Operator summary (raw content is not provided unless full is set to true)
     *
     * @param full to indicate if the raw content shall be included in response
     * @return the Macro Operator
     * @throws IkatsDaoException if any DAO exception occurs
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response listAll(
            @QueryParam("full") @DefaultValue("false") Boolean full
    ) throws IkatsDaoException {

        List<Workflow> result = Facade.listAllMacroOp();
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
     * Get the content of a Macro Operator by providing its id
     *
     * @param id id of the Macro Operator to read
     * @return the Macro Operator
     * @throws IkatsDaoException if any DAO exception occurs
     */
    @GET
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getMacroOp(
            @PathParam("id") Integer id
    ) throws IkatsDaoException {

        Workflow wf = Facade.getById(id);

        // Internal flags must not be provided
        wf.setMacroOp(null);

        return Response.status(Response.Status.OK).entity(wf).build();
    }

    /**
     * Update all Macro Operator at once
     *
     * @return HTTP response
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateMacroOps() {
        return Response.status(Response.Status.NOT_IMPLEMENTED).build();
    }

    /**
     * Update a Macro Operator identified by its Id
     *
     * @param wf      New content for the Macro Operator
     * @param uriInfo the uri info
     * @param id      id of the workflow to update
     * @return HTTP response
     * @throws IkatsDaoException if any DAO exception occurs
     */
    @PUT
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateMacroOp(
            Workflow wf,
            @Context UriInfo uriInfo,
            @PathParam("id") int id
    ) throws IkatsDaoException, IkatsWebClientException {

        if (wf.getId() != null && id != wf.getId()) {
            throw new IkatsWebClientException("Mismatch in request with Id between URI and body part");
        }
        wf.setId(id);
        wf.setMacroOp(true);

        boolean result = Facade.update(wf);

        if (result) {
            return Response.status(Response.Status.OK).build();
        } else {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Delete a Macro Operator identified by its id
     *
     * @param id id of the Macro Operator to delete
     * @return HTTP response
     * @throws IkatsDaoException if any DAO exception occurs
     */
    @DELETE
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeMacroOp(
            @PathParam("id") Integer id
    ) throws IkatsDaoException {
        Facade.removeById(id);
        return Response.status(Response.Status.NO_CONTENT).build();
    }

    /**
     * Delete all Macro Operators
     *
     * @return HTTP response
     * @throws IkatsDaoException if any DAO exception occurs
     */
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeAll() throws IkatsDaoException {

        Facade.removeAllMacroOp();

        return Response.status(Response.Status.NO_CONTENT).build();
    }
}

