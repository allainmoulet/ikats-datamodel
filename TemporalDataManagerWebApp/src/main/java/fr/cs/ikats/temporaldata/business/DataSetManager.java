/**
 * $Id$
 */
package fr.cs.ikats.temporaldata.business;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import org.apache.log4j.Logger;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoInvalidValueException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import fr.cs.ikats.metadata.MetaDataFacade;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.temporaldata.application.TemporalDataApplication;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.resource.TimeSerieResource;
import fr.cs.ikats.ts.dataset.DataSetFacade;
import fr.cs.ikats.ts.dataset.model.DataSet;
import fr.cs.ikats.ts.dataset.model.LinkDatasetTimeSeries;

/**
 * manage datasets
 */
public class DataSetManager {

    /**
     * the LOGGER instance for this class
     */
    private static final Logger LOGGER = Logger.getLogger(DataSetManager.class);

    /**
     * default constructor.
     */
    public DataSetManager() {

    }

    /**
     * private method to get the DataSetFacade from Spring context.
     * 
     * @return
     */
    public DataSetFacade getDataSetFacade() {
        return TemporalDataApplication.getApplicationConfiguration().getSpringContext().getBean(DataSetFacade.class);
    }

    /**
     * getter on new TimeSerieResource.
     */
    public TimeSerieResource getTimeSerieResource() {
		return new TimeSerieResource();
	}

	private MetaDataFacade getMetaDataFacade() {
        return TemporalDataApplication.getApplicationConfiguration().getSpringContext().getBean(MetaDataFacade.class);
    }

    /**
     * create dataset from low level values <br/>
     * Beware: precondition: the tsuids and functional identifiers must have
     * been created by importing ts
     * 
     * @param dataSetId
     *            the dataset id
     * @param description
     *            the description
     * @param tsuids
     *            the tsuids
     * 
     * @return the list of the internal id of the inserted metadata
     * @throws IkatsDaoException
     *             error saving the dataset
     */
    public String persistDataSet(String dataSetId, String description, List<String> tsuids) throws IkatsDaoException {
        return getDataSetFacade().persistDataSet(dataSetId, description, tsuids);
    }

    /**
     * create dataset from list of FunctionalIdentifier <br/>
     * Beware: precondition: the tsuids and functional identifiers must have
     * been created by importing ts
     * 
     * @param dataSetId
     *            the dataset id
     * @param description
     *            the description
     * @param funcIdentifiers
     *            the funcIdentifiers
     * 
     * @return the list of the internal id of the inserted metadata
     */
    public String persistDataSetFromEntity(String name, String description, List<FunctionalIdentifier> funcIdentifiers) throws IkatsDaoException {
        return getDataSetFacade().persistDataSetFromEntity(name, description, funcIdentifiers);
    }

    /**
     * return the list of links from one dataset to its associated timeseries (0, or more).
     * 
     * @param dataSetId
     *            the identifier
     * @return a list of LinkDatasetTimeSeries, null if dataset is not found.
     */
    public List<LinkDatasetTimeSeries> getDataSetContent(String dataSetId) throws IkatsDaoMissingRessource, IkatsDaoException {
        List<LinkDatasetTimeSeries> result = null;
        DataSet dataset = getDataSetFacade().getDataSet(dataSetId);
        if (dataset != null) {
            result = dataset.getLinksToTimeSeries();
        }
        return result;

    }

    /**
     * return the dataset
     * 
     * @param datasetName
     *            the identifier
     * @return null if dataset is not found.
     */
    public DataSet getDataSet(String datasetName) throws IkatsDaoMissingRessource, IkatsDaoException {
        DataSet dataset = getDataSetFacade().getDataSet(datasetName);
        return dataset;
    }
    /**
     * Return the dataset summary
     *
     * @param datasetName
     *            the identifier
     * @return null if dataset is not found.
     */
    public DataSet getDataSetSummary(String datasetName)
            throws IkatsDaoMissingRessource, IkatsDaoException {
        DataSet dataset = getDataSetFacade().getDataSetSummary(datasetName);
        return dataset;
    }

    /**
     * remove the dataset with identifier datasetId
     * 
     * @param datasetId
     *            the dataset to remove
     * @param deep
     *            boolean flag, optional (default false): true activates the
     *            deletion of the timeseries linked to the dataset and their associated metadata
     * @throws IkatsDaoException
     */
    public Status removeDataSet(String datasetId, Boolean deep) throws IkatsDaoMissingRessource, IkatsDaoException {

        String context = "Removing dataset=" + datasetId + " : ";
        List<String> tsuidsToRemove = new ArrayList<String>();
        List<IkatsDaoException> tsRemoveError = new ArrayList<IkatsDaoException>();
        if (Boolean.valueOf(deep)) {

            // 1: evaluate tsuidsToRemove
            // -------------------------------------------
            context = "Removing dataset=" + datasetId + "(mode=DEEP) : ";
            LOGGER.info(context + "step : evaluate time series that can be removed ");

            List<String> tsNotRemovedWarnings = new ArrayList<String>();

            DataSet dataSet = getDataSet(datasetId);
            if (dataSet == null) {
                return Status.NOT_FOUND;
            }

            for (LinkDatasetTimeSeries linkDatasetTs : dataSet.getLinksToTimeSeries()) {
                String tsuid = linkDatasetTs.getFuncIdentifier().getTsuid();

                List<String> inDataSetNames = getContainers(tsuid);
                if (inDataSetNames == null) {
                    throw new IkatsDaoConflictException("No dataset found for timeseries :" + tsuid);
                }
                else if ((inDataSetNames.size() == 1) && inDataSetNames.get(0).equals(datasetId)) {

                    tsuidsToRemove.add(tsuid);
                }
                else {
                    // add an error : TS is in another dataset
                    StringBuilder builder = new StringBuilder();
                    for (String datasetName : inDataSetNames) {

                        if (!datasetName.equals(datasetId)) {
                            builder.append(datasetName).append(",");
                        }
                    }

                    tsNotRemovedWarnings.add("TS with " + tsuid + " is included in " + (inDataSetNames.size() - 1) + " others datasets ("
                            + builder.toString() + "). It cannot be removed");
                }
            }
            if (!tsNotRemovedWarnings.isEmpty()) {
                LOGGER.warn(context + "Some of the timeseries were not deleted because they are attached to another dataset than " + datasetId + " : ");
                for (String error : tsNotRemovedWarnings) {
                    LOGGER.warn("- delete cancelled: " + tsNotRemovedWarnings);
                }
            }

            // 2: remove data + metadata for TS having only this dataset as parent
            // --------------------------------------------------------------------
            LOGGER.info(context + "step : remove linked time series and their metadata");
            TimeSerieResource timeS = getTimeSerieResource();

            for (String tsuid : tsuidsToRemove) {
                // only one dataset includes this tsuid, so remove all data
                // about the TS.
                try {
                    // firstly remove link before ds and ts to avoid constraint
                    // problem in db
                    getDataSetFacade().removeTSFromDataSet(tsuid, datasetId);
                    // then remove timeseries data + metadata from db
                    timeS.removeTimeSeries(tsuid);
                }
                catch (ResourceNotFoundException e) {
                    LOGGER.error("- failed to remove tsuid " + tsuid + " : does not exist in DB");
                }
                catch (IkatsDaoException e) {
                    LOGGER.error("- failed to remove associated metadata for tsuid=" + tsuid);
                    tsRemoveError.add(new IkatsDaoException("failed to remove associated metadata for tsuid " + tsuid, e));
                }
                catch (Throwable e) {
                    LOGGER.error("- failed to remove tsuid=" + tsuid);
                    tsRemoveError.add(new IkatsDaoException("failed to delete TS for tsuid " + tsuid, e));
                }
            }

            if (!tsRemoveError.isEmpty()) {
                LOGGER.error(context
                        + " Detailed report: errors raised while cleaning TS belonging to removed dataset (see summary in preceeding lines):");
                for (IkatsDaoException error : tsRemoveError) {
                    LOGGER.error("- caught error: " + context + error.getMessage(), error.getCause());
                }

                // => throw runtime exception in order to skip the deletion of dataset itself
                // execution
                WebApplicationException serverError = new WebApplicationException(
                        context + "delete dataset (deep=true) error: at least one error deleting the TS data : see admin logs."
                                + " => dataset not fully deleted (only deleted TS data are removed from dataset)");
                LOGGER.error(serverError.getMessage());
                throw serverError; // handled by ApplicationExceptionHandler
            }

        }
        // Finally remove dataset links
        // => avoid constraint error.
        // --------------------------------------------------------------------
        LOGGER.info(context + "step : remove dataset from database ");
        getDataSetFacade().removeDataSet(datasetId);
        
        LOGGER.info(context + "... successfully ended");

        return Status.NO_CONTENT;
    }

	/**
     * get all the dataSet summary : Name and Description only.
     * 
     * @return a list of dataset, empty if nothin matches
     */
    public List<DataSet> getAllDataSetSummary() throws IkatsDaoException {
        return getDataSetFacade().getAllDataSetSummary();

    }

    /**
     * list all the dataset which include the TS
     * 
     * @param tsuid
     *            the ts to test
     * @return null list if no dataset contains the tsuid
     */
    public List<String> getContainers(String tsuid) throws IkatsDaoException {
        return getDataSetFacade().getDataSetNamesForTsuid(tsuid);
    }

    /**
     * delete the link betweend tsuid and datasetName
     * 
     * @param tsuid
     *            the tsuid to detach from dataset
     * @param datasetName
     *            the dataset
     */
    public void removeTSFromDataSet(String tsuid, String datasetName) throws IkatsDaoException {
        getDataSetFacade().removeTSFromDataSet(tsuid, datasetName);
    }

    /**
     * update the dataset, set the new description, and add the non already
     * contained ts into the dataset.
     *
     * @param datasetName
     *            the name of the dataset to update
     * @param description
     *            the new description
     * @param tsuidList
     *            a list of tsuid to add to dataset, can be null
     * @param updateMode
     *            use "replace" - the default value- when the updated dataset
     *            will exactly contains the TS defined by tsuidList: content is
     *            explicitely defined or use "append" when the updated dataset
     *            is appended with the TS defined by tsuidList: its previous
     *            content is preserved
     * @return the number of TS added while updating
     */
    public int updateDataSet(String datasetName, String description, List<String> tsuidList, String updateMode)
            throws IkatsDaoInvalidValueException, IkatsDaoMissingRessource, IkatsDaoException {
        if ("replace".equalsIgnoreCase(updateMode)) {
            return getDataSetFacade().updateDataSet(datasetName, description, tsuidList);
        }
        else if ("append".equalsIgnoreCase(updateMode)) {
            return getDataSetFacade().updateInAppendMode(datasetName, description, tsuidList);
        }
        else {
            throw new IkatsDaoInvalidValueException(
                    "Unexpected parameter for updateDataSet: updateMode: expected values are  \"replace\" or \"append\", value=" + updateMode);
        }
    }
}
