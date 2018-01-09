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
 * @author Fabien TORAL <fabien.toral@c-s.fr>
 * @author Fabien TORTORA <fabien.tortora@c-s.fr>
 */

package fr.cs.ikats.temporaldata.resource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingResource;
import fr.cs.ikats.datamanager.client.opentsdb.ApiResponse;
import fr.cs.ikats.datamanager.client.opentsdb.ImportResult;
import fr.cs.ikats.lang.CollectionUtils;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.temporaldata.application.ApplicationLabels;
import fr.cs.ikats.temporaldata.business.FilterFunctionalIdentifiers;
import fr.cs.ikats.temporaldata.business.TsuidListInfo;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.IkatsJsonException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundJsonException;
import fr.cs.ikats.temporaldata.utils.Chronometer;

/**
 * this class hosts all the operations on metadata : import and list <br/>
 * Added IkatsDaoException management handling the MetaData
 * resources: see IkatsDaoException subclasses and IkatsDaoExceptionHandler.
 */
@Path("metadata")
public class MetaDataResource extends AbstractResource {

    private static Logger logger = Logger.getLogger(MetaDataResource.class);

    /**
     * default constructor
     */
    public MetaDataResource() {
        super();
    }

    /**
     * Import a metadata for a tsuid.
     *
     * @param tsuid   , the identifier of the TS in the TimeSeries storage system
     * @param name    name of the metadata
     * @param value   value of the metadata.
     * @param dtype   data type of the metadata.
     * @param details if true, message is returned to the client, otherwise, only
     *                the id
     * @return the internal identifier.
     * @throws IkatsDaoConflictException dao conflict error with one MetaData in database
     * @throws IkatsDaoException         another dao error.
     */
    @POST
    @Path("/import/{tsuid}/{name}/{value}")
    public String importMetaData(@PathParam("tsuid") String tsuid, @PathParam("name") String name, @PathParam("value") String value,
                                 @QueryParam("dtype") @DefaultValue("string") String dtype, @QueryParam("details") @DefaultValue("false") Boolean details)
            throws IkatsDaoConflictException, IkatsDaoException {

        // since [#142998] result ought to be good or exception is raised
        Integer id = metadataManager.persistMetaData(tsuid, name, value, dtype);

        String streResult = null;
        if (id != null) {
            streResult = Integer.toString(id);
        }

        if (details) {
            streResult = ApplicationLabels.getInstance().getLabel("metadata.import.details", id);
        }
        return streResult;
    }

    /**
     * import a metadata for a tsuid.
     *
     * @param tsuid , the identifier of the TS in the TimeSeries storage system
     * @param name  name of the metadata
     * @param value value of the metadata.
     * @return the internal identifier.
     * @throws ResourceNotFoundException if nothing updated
     * @throws IkatsDaoConflictException update error raised on conflict with another MetaData
     * @throws IkatsDaoException         another error from DAO
     */
    @PUT
    @Path("/{tsuid}/{name}/{value}")
    public String updateMetaData(@PathParam("tsuid") String tsuid, @PathParam("name") String name, @PathParam("value") String value)
            throws ResourceNotFoundException, IkatsDaoConflictException, IkatsDaoException {
        try {
            // result ought to be good or exception is raised
            Integer result = metadataManager.updateMetaData(tsuid, name, value);

            String streResult = result.toString();
            return streResult;
        } catch (IkatsDaoMissingResource e) {
            throw new ResourceNotFoundException(tsuid, e);
        }
    }

    /**
     * delete a all metadata + functional identifier for a tsuid.
     *
     * @param tsuid , the identifier of the TS in the TimeSeries storage system
     * @return an ImportResult with tsuid, number of success and summary set
     * @throws IkatsDaoException error raised by DAO layer
     */
    @DELETE
    @Path("/{tsuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse delete(@PathParam("tsuid") String tsuid) throws IkatsDaoException {
        int mdremoved = metadataManager.deleteMetaData(tsuid);
        ImportResult result = new ImportResult();
        result.setTsuid(tsuid);
        result.setNumberOfSuccess(mdremoved);
        result.setSummary(mdremoved + " metadata removed for tsuid " + tsuid);
        return result;
    }

    /**
     * delete a named metadata for a tsuid.
     *
     * @param tsuid , the identifier of the TS in the TimeSeries storage system
     * @param name  the name ot metadata
     * @return an ImportResult with tsuid, number of success and summary set
     * @throws IkatsDaoException error raised by DAO layer.
     */
    @DELETE
    @Path("/{tsuid}/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse delete(@PathParam("tsuid") String tsuid, @PathParam("name") String name) throws IkatsDaoException {
        int mdremoved = metadataManager.deleteMetaData(tsuid, name);
        ImportResult result = new ImportResult();
        result.setTsuid(tsuid);
        result.setNumberOfSuccess(mdremoved);
        result.setSummary(mdremoved + " metadata removed for tsuid " + tsuid);
        return result;
    }

    /**
     * import a CSV file from inputStream read from multipart form.
     *
     * @param fileis          input stream
     * @param fileDisposition file information
     * @param details         indicates if response must contains details (human readable)
     *                        on the operation
     * @param update          if true, already existing metadata is updated otherwise no
     *                        metadata is imported if one of them already exists
     * @return an operation report with details or not.
     * @throws IkatsDaoException error raised by DAO layer.
     * @throws IkatsException    another error raised executing the service
     */
    @POST
    @Path("/import/file")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.TEXT_PLAIN)
    public String importMetaDataFile(@FormDataParam("file") InputStream fileis, @FormDataParam("file") FormDataContentDisposition fileDisposition,
                                     @QueryParam("details") @DefaultValue("false") Boolean details, @QueryParam("update") @DefaultValue("false") Boolean update)
            throws IkatsDaoException, IkatsException {
        Chronometer chrono = new Chronometer(fileDisposition.getFileName(), true);

        List<Integer> result = null;

        result = metadataManager.persistMetaData(fileis, update);

        String streResult = Integer.toString(result.size());
        if (details) {
            streResult = ApplicationLabels.getInstance().getLabel("metadata.import.csv.details", streResult);
        }
        chrono.stop(logger);
        return streResult;
    }

    /**
     * get a csv representation of the metadata for a given tsuid. tsuid can be
     * a "*" to return all metadata, one String value, or a comma separated list
     * of identifiers
     *
     * @param tsuids the requested tsuid, a comma separated list of values,
     * @return a String : csv file with on metadata per line.
     * @throws IkatsDaoMissingResource dao error raised in case of unmatched TSUID
     * @throws IkatsDaoException       other error raised by DAO layer
     */
    @GET
    @Path("/list")
    @Produces({"application/ms-excel"})
    public Response getMetaData(@QueryParam("tsuid") String tsuids) throws IkatsDaoMissingResource, IkatsDaoException {

        // Handle multiple tsuids separated by ','
        List<String> tsuidslist = new ArrayList<String>(Arrays.asList(tsuids.split(",")));
        String filename;

        Response response;
        try {
            String csvStr = metadataManager.getListAsCSV(tsuidslist);

            if (tsuids.equals("*")) {
                logger.info("listing all metadata in database");
                filename = "metadata.csv";
            } else {
                logger.info("listing metadata for " + tsuids);
                if (tsuidslist.size() > 1) {
                    filename = "metadata.csv";
                } else {
                    filename = tsuids + "_metadata.csv";
                }
            }
            response = Response.ok(getOut(csvStr)).header("Content-Disposition", "attachment;filename=" + filename).build();
        } catch (IkatsDaoMissingResource e) {
            // just to add a message in the logger ...
            logger.error("Unable to find tsuid " + tsuids + " in database");
            throw e; // ... response encoded by IkatsDaoExceptionHandler
        }
        return response;
    }

    /**
     * get a csv representation of the metadata for a given tsuid. tsuid can be
     * a "*" to return all metadata, one String value, or a comma separated list
     * of identifiers
     *
     * @param tsuids the requested tsuid, a comma separated list of values,
     * @return a String : csv file with on metadata per line.
     * @throws ResourceNotFoundException if at least one metadata was not found: mismatched tsuid in
     *                                   DAO layer
     * @throws IkatsDaoException         another error raised by DAO layer
     */
    @GET
    @Path("/list/json")
    @Produces(MediaType.APPLICATION_JSON)
    public List<MetaData> getJsonMetaData(@QueryParam("tsuid") String tsuids) throws ResourceNotFoundException, IkatsDaoException {

        // Handle multiple tsuids separated by ','
        List<String> tsuidslist = new ArrayList<>();
        List<MetaData> result;
        try {
            tsuidslist = new ArrayList<>(Arrays.asList(tsuids.split(",")));
            result = metadataManager.getList(tsuidslist);
        } catch (IkatsDaoMissingResource e) {
            // ResourceNotFoundException is equivalent to
            // IkatsDaoMissingRessource
            throw new ResourceNotFoundException(tsuids, e);
        } catch (IkatsDaoException otherE) {
            throw new IkatsDaoException("Getting MetaData list from TSUID list=" + tsuids, otherE);
        }
        return result;
    }

    @POST
    @Path("/list/json")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<MetaData> getJsonMetaDataWithPostQuery(TsuidListInfo tsuids) throws ResourceNotFoundException, IkatsDaoException {

        List<MetaData> result;
        try {

            List<String> tsuidslist = tsuids.getTsuids();
            result = metadataManager.getList(tsuidslist);
        } catch (IkatsDaoMissingResource e) {
            // ResourceNotFoundException is equivalent to
            // IkatsDaoMissingRessource
            throw new ResourceNotFoundException(tsuids.toString(), e);
        } catch (IkatsDaoException otherE) {
            throw new IkatsDaoException("Getting MetaData list from TSUID list=" + tsuids, otherE);
        }
        return result;
    }

    /**
     * get a json representation of the metadata types
     *
     * @return a String : json file with one {metadata_name:metadata_type} per line.
     * @throws IkatsDaoException         error raised by DAO layer
     */
    @GET
    @Path("/types")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> getJsonMetaDataTypes() throws IkatsDaoException {
        return metadataManager.getListTypes();
    }

    /**
     * return a new Instance of OutputStreaming, used for streaming out the csv
     * file
     *
     * @param excelBytes the bytes to write
     * @return
     */
    private StreamingOutput getOut(final String excelBytes) {
        return new StreamingOutput() {
            @Override
            public void write(OutputStream out) throws IOException, WebApplicationException {
                out.write(excelBytes.getBytes());
            }
        };
    }

    /**
     * import a FunctionalIdentifier for a tsuid.
     *
     * @param tsuid  , the identifier of the TS in the TimeSeries storage system
     * @param funcId the functional identifier to store
     * @return an ImportResult
     * @throws InvalidValueException if funcId is not valid
     */
    @POST
    @Path("/funcId/{tsuid}/{funcId}")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse importFunctionalIdentifier(@PathParam("tsuid") String tsuid, @PathParam("funcId") String funcId)
            throws InvalidValueException, IkatsDaoException {
        temporalDataManager.validateFuncId(funcId);
        int added = metadataManager.persistFunctionalIdentifier(tsuid, funcId);
        ImportResult result = new ImportResult();
        result.setTsuid(tsuid);
        result.setNumberOfSuccess(added);
        result.setSummary(funcId + " added for tsuid " + tsuid);
        return result;
    }

    /**
     * delete a FunctionalIdentifier for a tsuid.
     *
     * @param tsuid , the identifier of the TS in the TimeSeries storage system
     * @return an ImportResult with tsuid, number of success and summary set
     * @throws IkatsDaoException
     */
    @DELETE
    @Path("/funcId/{tsuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse deleteFunctionalIdentifier(@PathParam("tsuid") String tsuid) throws IkatsDaoException {
        int added = metadataManager.deleteFunctionalIdentifier(tsuid);
        ImportResult result = new ImportResult();
        result.setTsuid(tsuid);
        result.setNumberOfSuccess(added);
        result.setSummary("functional identifier removed for tsuid " + tsuid);
        return result;
    }

    /**
     * Read the FunctionalIdentifier resource associated to the provided tsuid
     * value
     *
     * @param tsuid ts identifier
     * @return matching
     * @throws ResourceNotFoundJsonException if no internalIdentifier found.
     */
    @GET
    @Path("/funcId/{tsuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public FunctionalIdentifier getFunctionalIdentifier(@PathParam("tsuid") String tsuid) throws IkatsDaoException,
            ResourceNotFoundJsonException {

        FunctionalIdentifier internalIdentifiers = metadataManager.getFunctionalIdentifierByTsuid(tsuid);
        if (internalIdentifiers == null) {
            throw new ResourceNotFoundJsonException("Missing functional ID for TSUID: " + tsuid);
        }
        return internalIdentifiers;
    }

    /**
     * Read the list of resources FunctionalIdentifier matched by criteria
     * passed by form: tsuids or funcIds Note: consumes Form data
     *
     * @param tsuids  the criteria list of tsuids: when search is from tsuids
     * @param funcIds the criteria list of funcIds: when search is from funcIds
     * @return the filtered list of resources FunctionalIdentifier
     * @throws ResourceNotFoundJsonException if result is null or empty: not found
     * @throws IkatsJsonException            error when search is incorrectly defined by client: bad
     *                                       request
     */
    @POST
    @Path("/funcId")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_JSON)
    public List<FunctionalIdentifier> searchFunctionalIdentifiers(@FormParam("tsuids") List<String> tsuids,
                                                                  @FormParam("funcIds") List<String> funcIds) throws IkatsDaoException, IkatsJsonException {

        FilterFunctionalIdentifiers filter = new FilterFunctionalIdentifiers();
        filter.setTsuids(tsuids);
        filter.setFuncIds(funcIds);
        return searchTsIds(filter);
    }

    /**
     * Read the list of resources FunctionalIdentifier matched by the filter.
     * Note: consumes JSON
     *
     * @param filter the filter defined by the json consumed. Note: see
     *               FilterFunctionalIdentifiers class which is a mapping of
     *               expected JSON content.
     * @return the filtered list of resources FunctionalIdentifier
     * @throws ResourceNotFoundJsonException if result is null or empty: not found
     * @throws IkatsJsonException            error when search is incorrectly defined by client: bad
     *                                       request
     */
    @POST
    @Path("/funcId")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<FunctionalIdentifier> searchFunctionalIdentifiersJson(FilterFunctionalIdentifiers filter)
            throws IkatsDaoException, IkatsJsonException {
        return searchTsIds(filter);
    }

    @GET
    @Path("/funcId")
    @Produces(MediaType.APPLICATION_JSON)
    public List<FunctionalIdentifier> searchFunctionalIdentifiersJson() throws IkatsDaoException {
        return metadataManager.getAllFunctionalIdentifiers();
    }


    /**
     * implementation searching a FunctionalIdentifier list
     *
     * @param filter defined for searching records FunctionalIdentifier
     * @return response computed
     * @throws ResourceNotFoundJsonException if result is null or empty: not found
     * @throws IkatsJsonException            error when search is incorrectly defined by client: bad
     *                                       request
     */
    private List<FunctionalIdentifier> searchTsIds(FilterFunctionalIdentifiers filter) throws IkatsDaoException, IkatsJsonException {
        List<FunctionalIdentifier> res = null;
        if ((!CollectionUtils.isNullOrEmpy(filter.getTsuids())) && (!CollectionUtils.isNullOrEmpy(filter.getFuncIds()))) {
            throw new IkatsJsonException("Forbidden: both funcIds and tsuids filters are defined: " + filter.toString());
        } else if (!CollectionUtils.isNullOrEmpy(filter.getTsuids())) {
            res = metadataManager.getFunctionalIdentifierByTsuidList(filter.getTsuids());

        } else if (!CollectionUtils.isNullOrEmpy(filter.getFuncIds())) {
            res = metadataManager.getFunctionalIdentifierByFuncIdList(filter.getFuncIds());
        } else {
            // Return all Functional ids
            res = metadataManager.getAllFunctionalIdentifiers();
        }

        if ((res == null) || res.isEmpty()) {
            throw new ResourceNotFoundJsonException("Empty result, searching (tsuid, functionalId) pairs");
        }
        return res;
    }
}

