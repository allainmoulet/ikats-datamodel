package fr.cs.ikats.temporaldata.business;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoInvalidValueException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import fr.cs.ikats.common.expr.Atom;
import fr.cs.ikats.common.expr.Expression;
import fr.cs.ikats.common.expr.Expression.ConnectorExpression;
import fr.cs.ikats.common.expr.Group;
import fr.cs.ikats.common.expr.SingleValueComparator;
import fr.cs.ikats.metadata.MetaDataFacade;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.metadata.model.MetaData.MetaType;
import fr.cs.ikats.metadata.model.MetadataCriterion;
import fr.cs.ikats.temporaldata.application.TemporalDataApplication;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;

/**
 * MetaData Business Manager singleton.
 */

public class MetaDataManager {

    private static final Logger logger = Logger.getLogger(MetaDataManager.class);

    /**
     * private method to get the MetaDataFacade from Spring context.
     *
     * @return
     */
    private MetaDataFacade getMetaDataFacade() {
        return TemporalDataApplication.getApplicationConfiguration().getSpringContext().getBean(MetaDataFacade.class);
    }

    /**
     * Create the MetaData for a given tsuid, name and value.
     *
     * @param tsuid the tsuid of the metadata
     * @param name  name of metadata
     * @param value value of metadata
     * @return the list of the internal id of the inserted metadata
     * @throws IkatsDaoConflictException     create error raised on conflict with another resource
     * @throws IkatsDaoInvalidValueException invalid content
     * @throws IkatsDaoException             another error from DAO
     */
    public Integer persistMetaData(String tsuid, String name, String value)
            throws IkatsDaoConflictException, IkatsDaoInvalidValueException, IkatsDaoException {
        return getMetaDataFacade().persistMetaData(tsuid, name, value);
    }

    /**
     * persist meta data for a given tsuid, name, values and dtype.
     *
     * @param tsuid the tsuid of the metadata
     * @param name  name of metadata
     * @param value value of metadata
     * @param dtype data type of metadata
     * @return the list of the internal id of the inserted metadata
     * @throws IkatsDaoConflictException     error raised by conflict with another row in database
     * @throws IkatsDaoInvalidValueException error raised for invalid dtype value
     * @throws IkatsDaoException             another error
     */
    public Integer persistMetaData(String tsuid, String name, String value, String dtype)
            throws IkatsDaoConflictException, IkatsDaoInvalidValueException, IkatsDaoException {
        return getMetaDataFacade().persistMetaData(tsuid, name, value, dtype);
    }

    /**
     * persist meta data for a given tsuid, name, values and dtype.
     *
     * @param tsuid the tsuid of the metadata
     * @param name  name of metadata
     * @param value value of metadata
     * @param dtype enumerate type of metadata
     * @return the list of the internal id of the inserted metadata
     * @throws IkatsDaoConflictException error raised by conflict with another row in database
     * @throws IkatsDaoException         another error
     */
    public Integer persistMetaData(String tsuid, String name, String value, MetaType dtype) throws IkatsDaoConflictException, IkatsDaoException {
        return getMetaDataFacade().persistMetaData(tsuid, name, value, dtype);
    }

    /**
     * Update meta data for a given tsuid, name with value.
     *
     * @param tsuid the tsuid of the metadata
     * @param name  name of metadata
     * @param value value of metadata to update
     * @return the id of the updated metadata or null if no update has been
     * performed
     * @throws IkatsDaoConflictException update error raised on conflict with another resource
     * @throws IkatsDaoMissingRessource  update error raised when a MetaData is missing
     * @throws IkatsDaoException         another error from DAO
     */
    public Integer updateMetaData(String tsuid, String name, String value)
            throws IkatsDaoConflictException, IkatsDaoMissingRessource, IkatsDaoException {
        return getMetaDataFacade().updateMetaData(tsuid, name, value);
    }

    /**
     * Retrieve the MetaData matching the criteria tsuid+name
     *
     * @param tsuid the tsuid criterion value
     * @param name  the name criterion value
     * @return MetaData
     * @throws IkatsDaoMissingRessource  error raised when no metadata is matching the tsuid+name
     *                                   criteria
     * @throws IkatsDaoConflictException error raised when multiple metadata are found
     * @throws IkatsDaoException         any other exceptions
     */
    public MetaData getMetaData(String tsuid, String name) throws IkatsDaoConflictException, IkatsDaoMissingRessource, IkatsDaoException {

        MetaData metadata = getMetaDataFacade().getMetaData(tsuid, name);

        return metadata;
    }

    /**
     * persist all meta data read in a csv file.
     *
     * @param fileis the fileInputStream
     * @param update if true, already existing metadata is updated otherwise no
     *               metadata is persisted if one of them already exists
     * @return a list of internal identifiers
     * @throws IkatsException    any other error
     * @since [#142998] Handling IkatsDaoException: keep all the troubles
     */
    public List<Integer> persistMetaData(InputStream fileis, Boolean update) throws IkatsException {

        List<Integer> results = null;
        List<MetaData> mdataListFromCSV;

        try {
            // raises Exception
            mdataListFromCSV = getMetaDataFacade().getMetaDataFromCSV(fileis);

            // raises IkatsDaoException
            results = getMetaDataFacade().persist(mdataListFromCSV, update);
        } catch (IkatsDaoException daoException) {
            // Dealing with the first error ...

            IkatsException contextError = new IkatsException(
                    "persistMetaData() : import has globally failed with one transaction at DAO level: see server logs.", daoException);

            logger.error(contextError);
            throw contextError;

        } catch (Exception csvParsingError) {
            // Dealing with unexpected errors like: IOException ...
            IkatsException contextError = new IkatsException(
                    "persistMetaData() : bad CSV format detected before opening the transaction: please correct CSV data at pointed lines in the detailed logs.",
                    csvParsingError);

            logger.error(contextError);
            throw contextError;
        }

        return results;
    }

    /**
     * get a synthetic metadata CSV representation for the list of tsduids in
     * param
     *
     * @param tsuids : list of tsuid
     * @return a csv formatted string.
     * @throws IkatsDaoMissingRessource error raised when no matching resource is found, for a tsuid
     *                                  different from '*'
     * @throws IkatsDaoException        any error raised by DAO layer.
     */
    public String getListAsCSV(List<String> tsuids) throws IkatsDaoMissingRessource, IkatsDaoException {
        MetaDataFacade facade = getMetaDataFacade();
        String separator = ";";
        Map<String, Map<String, String>> exportedMap = new HashMap<>();
        Set<String> metadataNameList = new HashSet<>();

        // Parse all tsuids and retrieve all associated metadata
        for (String tsuid : tsuids) {
            Map<String, String> metadata = new HashMap<>();
            logger.info("Reading MetaData list for TSUID=" + tsuid);
            List<MetaData> metaListForTs = facade.getMetaDataForTS(tsuid);
            for (MetaData meta : metaListForTs) {
                metadata.put(meta.getName(), meta.getValue());
                // construct metadata names list
                metadataNameList.add(meta.getName());
            }
            FunctionalIdentifier funcId = getFunctionalIdentifierByTsuid(tsuid);

            if (funcId == null) {
                funcId = new FunctionalIdentifier(tsuid, "NO_FUNC_ID_" + tsuid);
            }
            exportedMap.put(funcId.getFuncId(), metadata);
        }

        StringBuilder sb = new StringBuilder();
        for (String metadataName : metadataNameList) {
            sb.append(separator);
            sb.append(metadataName);
        }
        for (String key : exportedMap.keySet()) {
            sb.append("\n");
            sb.append(key);
            for (String meta : metadataNameList) {
                sb.append(separator);
                if (exportedMap.get(key).containsKey(meta)) {
                    sb.append(exportedMap.get(key).get(meta));
                }
            }
        }
        return sb.toString();
    }

    /**
     * get a list of metadata for the list of tsuids in param
     *
     * @param tsuids : list of tsuid
     * @return a csv formatted string.
     * @throws IkatsDaoMissingRessource error raised when no matching resource is found, for a tsuid
     *                                  different from '*'
     * @throws IkatsDaoException        any error raised by DAO layer.
     */
    public List<MetaData> getList(List<String> tsuids) throws IkatsDaoMissingRessource, IkatsDaoException {
        MetaDataFacade facade = getMetaDataFacade();

        // List of metadata returned
        List<MetaData> result = new ArrayList<MetaData>();

        // Parse all tsuids and concat to the result
        for (String tsuid : tsuids) {
            result.addAll(facade.getMetaDataForTS(tsuid.trim()));
        }
        return result;
    }

    /**
     * get a list of metadata {name:type} for the list of tsuids in param
     *
     * @return a json formatted string.
     * @throws IkatsDaoMissingRessource error raised when no matching resource is found, for a tsuid
     *                                  different from '*'
     * @throws IkatsDaoException        any error raised by DAO layer.
     */
    public Map<String, String> getListTypes() throws IkatsDaoMissingRessource, IkatsDaoException {
        MetaDataFacade facade = getMetaDataFacade();
        return facade.getMetaDataTypes();
    }

    /**
     * get a list of metadata for a given tsuid
     *
     * @param tsuid : tsuid
     * @return a csv formatted string.
     * @throws IkatsDaoMissingRessource error raised when no matching resource is found, for a tsuid
     *                                  different from '*'
     * @throws IkatsDaoException        any error raised by DAO layer.
     */
    public Object getList(String tsuid) throws IkatsDaoMissingRessource, IkatsDaoException {
        MetaDataFacade facade = getMetaDataFacade();
        return facade.getMetaDataForTS(tsuid);
    }

    /**
     * get a CSV representation for a tsuid in param
     *
     * @param tsuid the TS requested
     * @return a csv formatted string.
     * @throws IkatsDaoMissingRessource error raised when no matching resource is found, for a tsuid
     *                                  different from '*'
     * @throws IkatsDaoException        any error raised by DAO layer.
     */
    public String getListAsCSV(String tsuid) throws IkatsDaoMissingRessource, IkatsDaoException {
        // Convert String to List<String>
        List<String> tsuids = new ArrayList<String>();
        tsuids.add(tsuid);

        return getListAsCSV(tsuids);
    }

    /**
     * persist meta data for a given tsuid, name and values. One MetaData per
     * value is created.
     *
     * @param tsuid  tsuid
     * @param funcId functional identifier. must not be null
     * @return the number of functionalIdentifier actually stored.
     */
    public int persistFunctionalIdentifier(String tsuid, String funcId) throws IkatsDaoException {

        if (tsuid != null) {
            FunctionalIdentifier id = getMetaDataFacade().getFunctionalIdentifierByTsuid(tsuid);
            if (id != null && id.getFuncId().equals(funcId)) {
                throw new IkatsDaoConflictException("Functional Identifier " + funcId + " already created for tsuid " + tsuid);
            } else {
                return getMetaDataFacade().persistFunctionalIdentifier(tsuid, funcId);
            }
        } else {
            logger.warn("tsuid is null");
            return 0;
        }
    }

    /**
     * delete the functional identifier for tsuid
     *
     * @param tsuid the tsuid
     * @return the number of rows deleted
     * @throws IkatsDaoException
     * @throws IkatsDaoConflictException
     */
    public int deleteFunctionalIdentifier(String tsuid) throws IkatsDaoConflictException, IkatsDaoException {
        List<String> tsuids = new ArrayList<String>();
        tsuids.add(tsuid);
        return getMetaDataFacade().removeFunctionalIdentifier(tsuids);
    }

    /**
     * Get the FunctionalIdentifier matching the value tsuid
     *
     * @param tsuid the internal identifier of a TS
     * @return found FunctionalIdentifier, or null
     */
    public FunctionalIdentifier getFunctionalIdentifierByTsuid(String tsuid) {

        return getMetaDataFacade().getFunctionalIdentifierByTsuid(tsuid);

    }

    /**
     * Get the functional identifier list matching the list of tsuid values
     *
     * @param tsuids list of search criteria: tsuid values
     * @return a list of FunctionalIdentifier, or null if nothing is found.
     */
    public List<FunctionalIdentifier> getFunctionalIdentifierByTsuidList(List<String> tsuids) {

        return getMetaDataFacade().getFunctionalIdentifierByTsuidList(tsuids);

    }

    /**
     * Get the functional identifier list matching the list of funcId values
     *
     * @param funcIds : list of functional identifiers
     * @return a list of FunctionalIdentifier, or null if nothing is found.
     */
    public List<FunctionalIdentifier> getFunctionalIdentifierByFuncIdList(List<String> funcIds) {

        return getMetaDataFacade().getFunctionalIdentifierByFuncIdList(funcIds);

    }

    /**
     * Get the functional identifier list matching the list of funcId values
     *
     * @return a list of FunctionalIdentifier, or null if nothing is found.
     */
    public List<FunctionalIdentifier> getAllFunctionalIdentifiers() {

        return getMetaDataFacade().getFunctionalIdentifiersList();

    }

    /**
     * delete metadata for a TS
     *
     * @param tsuid the TS
     * @return the number of metadata deleted
     * @throws IkatsDaoException error deleting the resource
     */
    public int deleteMetaData(String tsuid) throws IkatsDaoException {
        return getMetaDataFacade().removeMetaDataForTS(tsuid);
    }

    /**
     * delete metadata for a TS
     *
     * @param tsuid the TS
     * @param name  name of the metadata
     * @return the number of metadata deleted
     * @throws IkatsDaoException dao error deleting the resource
     */
    public int deleteMetaData(String tsuid, String name) throws IkatsDaoException {
        return getMetaDataFacade().removeMetaDataForTS(tsuid, name);
    }

    /**
     * Search FunctionalIdentifiers matched by the filter FilterOnTsWithMetadata
     *
     * @param filterByMeta is the filter defining metadata criterion and a subset of
     *                     FunctionalIdentifier
     * @return
     */
    public List<FunctionalIdentifier> searchFunctionalIdentifiers(FilterOnTsWithMetadata filterByMeta) throws IkatsDaoException {

        List<FunctionalIdentifier> lFuncIdentifiers = new ArrayList<FunctionalIdentifier>();

        try {
            String datasetName = filterByMeta.getDatasetName();
            if (!datasetName.isEmpty()) {

                return filterByMetaWithDatasetName(filterByMeta.getDatasetName(), filterByMeta.getCriteria());


            } else {
                return filterByMetaWithTsuidList(filterByMeta.getTsList(), filterByMeta.getCriteria());
            }

        } catch (IkatsDaoException daoError) {
            throw daoError;
        } catch (Throwable e) {
            throw new IkatsDaoException("MetadataManager::searchFunctionalIdentifiers ended with unhandled error ", e);
        }

    }


    /**
     * Simplify a criteria list by converting some comparators
     *
     * @param criteria criteria list to convert
     * @return the new criteria list
     * @throws IkatsDaoException
     * @throws SQLException
     * @throws IkatsException            if the database can't be reach
     * @throws ResourceNotFoundException if table or column from table is not found
     */
    private List<MetadataCriterion> criteriaConverter(List<MetadataCriterion> criteria)
            throws IkatsDaoException, SQLException, IkatsException, ResourceNotFoundException {

        ArrayList<MetadataCriterion> convertedCriteria = new ArrayList<MetadataCriterion>();

        // Parsing every criterion to detect which one must be converted
        for (MetadataCriterion criterion : criteria) {

            String metadataName = criterion.getMetadataName();
            String criterionValue = criterion.getValue();
            SingleValueComparator criterionOperator = criterion.getTypedComparator();
            String sqlOperator = criterionOperator.getText();
            SingleValueComparator comparator = SingleValueComparator.parseComparator(sqlOperator);

            switch (comparator) {
                case IN_TABLE: {

                    // Get the table information
                    // Allowed pattern is 'tableName.column'
                    List<String> tableInformation = Arrays.asList(criterionValue.split("\\."));
                    String tableName = tableInformation.get(0);

                    // Use the same name as Metadata Name by default for column selection
                    String column = metadataName;
                    if (tableInformation.size() == 2) {
                        // But if a column is specified, use this name.
                        column = tableInformation.get(1);
                    }

                    // Extract the desired column form the table content
                    TableManager tableManager = new TableManager();
                    List<String> splitValues = tableManager.getColumnFromTable(tableName, column);

                    // Changing comparator to IN
                    criterion.setComparator(SingleValueComparator.IN.getText());

                    // Setting the extracted list from the column as new value content
                    criterion.setValue(String.join(";", splitValues));

                    convertedCriteria.add(criterion);
                    break;

                }
                default:
                    // No conversion, use it directly
                    convertedCriteria.add(criterion);
            }
        }
        return convertedCriteria;
    }

    /**
     * Filter a dataset based on criteria applied on its metadata
     *
     * @param datasetName Name of the dataset to filter
     * @param criteria    criteria list to use
     * @return the filtered list of functional identifiers
     * @throws IkatsDaoException
     * @throws SQLException
     * @throws IkatsException            if the database can't be reach
     * @throws ResourceNotFoundException if table or column from table is not found
     */
    private List<FunctionalIdentifier> filterByMetaWithDatasetName(String datasetName,
                                                                   List<MetadataCriterion> criteria)
            throws IkatsDaoException, IkatsException, SQLException, ResourceNotFoundException {

        List<MetadataCriterion> convertedCriteria = criteriaConverter(criteria);

        return getMetaDataFacade().searchFuncId(datasetName, convertedCriteria);

    }

    /**
     * Get the filtered list of TS from the input list with the criteria
     *
     * @param tsuidList the list to filter
     * @param lCriteria the criteria to use
     * @return the filtered list of functional identifiers
     * @throws IkatsDaoInvalidValueException
     * @throws IkatsDaoException
     */
    List<FunctionalIdentifier> filterByMetaWithTsuidList(
            List<FunctionalIdentifier> tsuidList,
            List<MetadataCriterion> lCriteria)
            throws IkatsDaoInvalidValueException, IkatsDaoException, IkatsException, SQLException, ResourceNotFoundException {

        Group<MetadataCriterion> lFormula = new Group<MetadataCriterion>();
        lFormula.connector = ConnectorExpression.AND;
        lFormula.terms = new ArrayList<Expression<MetadataCriterion>>();

        List<MetadataCriterion> convertedCriteria = criteriaConverter(lCriteria);

        // expression is always a group with depth = 1 and connector AND
        for (MetadataCriterion metadataCriterion : convertedCriteria) {
            metadataCriterion.computeServerValue(); // '*' to '%' for operator like
            Atom<MetadataCriterion> atomCriterion = new Atom<MetadataCriterion>();
            atomCriterion.atomicTerm = metadataCriterion;
            lFormula.terms.add(atomCriterion);
        }

        // plug the restriction of size below
        // instead of:
        // return getMetaDataFacade().searchFuncId(lFuncIdentifiers,
        // lFormula);

        List<List<FunctionalIdentifier>> lWellDimensionedTsIdLists = new ArrayList<List<FunctionalIdentifier>>();
        int currentSize = 0;
        int maxsize = 100;
        List<FunctionalIdentifier> lCurrentListIdentifiers = null;

        // case of no scope is provided => retrieve all funcId from db
        if (tsuidList.isEmpty()) {
            tsuidList = getMetaDataFacade().getFunctionalIdentifiersList();
        }
        // creating samples of 100 funcId
        for (FunctionalIdentifier functionalIdentifier : tsuidList) {
            if ((currentSize == maxsize) || (currentSize == 0)) {
                lCurrentListIdentifiers = new ArrayList<FunctionalIdentifier>();
                lWellDimensionedTsIdLists.add(lCurrentListIdentifiers);
                currentSize = 0;
            }
            lCurrentListIdentifiers.add(functionalIdentifier);
            currentSize++;
        }

        List<FunctionalIdentifier> lResult = new ArrayList<FunctionalIdentifier>();
        for (List<FunctionalIdentifier> currentWellDimensionedList : lWellDimensionedTsIdLists) {
            lResult.addAll(getMetaDataFacade().searchFuncId(currentWellDimensionedList, lFormula));
        }
        return lResult;
    }

}
