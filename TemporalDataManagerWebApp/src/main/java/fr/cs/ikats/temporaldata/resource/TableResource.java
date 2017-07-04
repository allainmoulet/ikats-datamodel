package fr.cs.ikats.temporaldata.resource;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.FormDataParam;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.expr.SingleValueComparator;
import fr.cs.ikats.metadata.MetaDataFacade;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.metadata.model.MetadataCriterion;
import fr.cs.ikats.temporaldata.business.DataSetManager;
import fr.cs.ikats.temporaldata.business.FilterOnTsWithMetadata;
import fr.cs.ikats.temporaldata.business.MetaDataManager;
import fr.cs.ikats.temporaldata.business.TableElement;
import fr.cs.ikats.temporaldata.business.TableInfo;
import fr.cs.ikats.temporaldata.business.TableInfo.DataLink;
import fr.cs.ikats.temporaldata.business.TableManager;
import fr.cs.ikats.temporaldata.business.TableManager.Table;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.IkatsJsonException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.utils.Chronometer;
import fr.cs.ikats.ts.dataset.model.LinkDatasetTimeSeries;

/**
 * resource for Table
 */
@Path("table")
public class TableResource extends AbstractResource {

    /**
     * 
     */

    private static Logger logger = Logger.getLogger(TableResource.class);

    private static final String MSG_DAO_KO_JOIN_BY_METRICS = "Failed to apply joinByMetrics(): DAO error occured with dataset name=''{0}'' on metrics=''{1}'' on table=''{2}''";
    private static final String MSG_INVALID_METRICS_FOR_JOIN_BY_METRICS = "Invalid metrics value=''{0}'' for joinByMetrics with dataset name=''{1}'' and table name=''{2}''";
    private static final String MSG_INVALID_TABLE_FOR_JOIN_BY_METRICS = "Invalid table ''{0}'' for joinByMetrics on dataset name=''{1}'' and metrics=''{2}''";
    private static final String MSG_RESOURCE_NOT_FOUND_JOIN_BY_METRICS = "Resource not found error occured for joinByMetrics with dataset name=''{0}'' metrics=''{1}'' and table=''{2}''";
    private static final String MSG_UNEXPECTED_ERROR_JOIN_BY_METRICS = "Unexpected error occured in joinByMetrics with dataset name=''{0}'' on metrics=''{1}'' and table=''{2}''";
    private static final String MSG_INVALID_INPUT_ERROR_JOIN_BY_METRICS = "Invalid input name=''{0}'' value=''{1}'' in joinByMetrics with dataset name=''{2}'' on metrics=''{3}'' and table=''{4}''";

    
    /**
     * TableManager
     */
    private TableManager tableManager;

    /**
     * MetadataManager
     */
    private MetaDataManager metaManager;

    /**
     * DatasetManager
     */
    private DataSetManager datasetManager;

    /**
     * init the processDataManager
     */
    public TableResource() {

        datasetManager = new DataSetManager();
        metadataManager = new MetaDataManager();

    }


    /**
     * get the JSON result as an attachement file in the response.
     *
     * @param tableName
     *            the name of the table to retrieve
     * @return a Response with content-type json
     * @throws ResourceNotFoundException
     *             if table not found
     * @throws IkatsDaoException
     *             if hibernate exception raised while storing table in db
     * @throws IkatsException
     *             others unexpected exceptions
     */
    @GET
    @Path("/{tableName}")
    public Response downloadTable(@PathParam("tableName") String tableName) throws ResourceNotFoundException, IkatsException, IkatsDaoException, IkatsJsonException {
        // get id of table in processData db
        // assuming there is only one table by tableName
        TableInfo table = tableManager.readFromDatabase(tableName);
        try {

            String jsonString = tableManager.serializeToJson(table);

            return Response.ok(jsonString, MediaType.APPLICATION_JSON_TYPE).build();
        }
        catch (Exception e) {
            throw new IkatsException("Failed: service downloadTable() " + tableName + " : caught unexpected Throwable:", e);
        }

    }

    /**
     * Database (processData table) import of a csv table
     *
     * @param tableName
     *            name of the table
     * @param fileis
     *            the file input stream
     * @param fileDisposition
     *            information about the Multipart with file
     * @param rowName
     *            table row name for unique id
     * @param formData
     *            the form data
     * @param uriInfo
     *            all info on URI
     * @return the internal id
     * @throws IOException           error when parsing input csv file
     * @throws IkatsDaoException     error while accessing database to check if table already
     *                               exists
     * @throws IkatsJsonException    error when mapping imported table to business object Table
     * @throws InvalidValueException if table name does not match expected pattern
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @SuppressWarnings("unchecked")
    public Response importTableFromCSV(@FormDataParam("tableName") String tableName,
                                       @FormDataParam("file") InputStream fileis,
                                       @FormDataParam("file") FormDataContentDisposition fileDisposition,
                                       @FormDataParam("rowName") String rowName, FormDataMultiPart formData,
                                       @Context UriInfo uriInfo) throws IkatsException, IOException, IkatsDaoException, InvalidValueException {

        // check that tableName does not already exist
        if (tableManager.existsInDatabase(tableName)) {
            String context = "Table name already exists : " + tableName;
            logger.error(context);
            return Response.status(Response.Status.CONFLICT).entity(context).build();
        }

        Chronometer chrono = new Chronometer(uriInfo.getPath(), true);
        String fileName = fileDisposition.getFileName();
        try {
            logger.info("Import csv file : " + fileName);
            logger.info("Table Name : " + tableName);

            BufferedReader reader = new BufferedReader(new InputStreamReader(fileis));
            String separator = ",";
            Integer rowIndexId = -1;
            List<String> columnHeaders;
            List<String> rowHeaders = new ArrayList<>();
            List<List<String>> cells = new ArrayList<>();

            // consume header to retrieve column index of unique identifier in the table
            columnHeaders = Arrays.asList(reader.readLine().split(separator));
            for (int i = 0; i < columnHeaders.size(); i++) {
                if (!columnHeaders.get(i).isEmpty() && columnHeaders.get(i).equals(rowName)) {
                    rowIndexId = i;
                    break;
                }
            }

            if (rowIndexId == -1) {
                String context = "Row name not found in csv file header : " + rowName;
                logger.error(context);
                return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
            }

            HashMap<String, String> keyTableMap = new HashMap<>();
            String line;
            // first element of row header is column header : set to null
            rowHeaders.add(null);

            // init output table
            Table outputTable = tableManager.initEmptyTable();

            // parse csv content to :
            // 1. retrieve data to build json table structure to store
            // 2. fix duplicates : if occurs, we stop at first duplicate and
            // send back 409 http code
            while ((line = reader.readLine()) != null) {

                // skip empty lines
                if (line.isEmpty()) {
                    continue;
                }

                String[] items = line.split(separator, -1);
                // check table content consistency
                if (items.length != columnHeaders.size()) {
                    String context = "Line length does not fit headers size in csv file : " + fileName;
                    logger.error(context);
                    return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
                }

                // retrieving table data
                rowHeaders.add(items[0]);
                List<String> newRow = new ArrayList<>();
                for (int i = 1; i < items.length; i++) {
                    newRow.add(items[i]);
                }
                // add row to output table
                outputTable.appendRow(newRow);

                // seeking for duplicates
                String idRef = items[rowIndexId];
                if (!keyTableMap.containsKey(idRef)) {
                    keyTableMap.put(idRef, "NA");
                } else {
                    String context = "Duplicate found in csv file: " + rowName + " = " + idRef;
                    logger.error(context);
                    return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
                }
            }

            // fill table description
            outputTable.setName(tableName);
            outputTable.setTitle(tableName);
            outputTable.setDescription(fileName);

            // fill headers
            outputTable.getColumnsHeader().getData().addAll(columnHeaders);
            outputTable.getRowsHeader().getData().addAll(rowHeaders);

            // store Table
            String rid = tableManager.createInDatabase(tableName, outputTable.getTableInfo());
            chrono.stop(logger);

            // result id is returned in the body
            return Response.status(Response.Status.OK).entity(rid).build();

        } catch (IOException e) {
            String contextError = "Unexpected interruption while parsing CSV file : " + fileName;
            throw new IOException(contextError, e);
        }

    }

    /**
     * Extends the table defined by the tableJson parameter, by adding one
     * column per selected metric:
     * <ul>
     * <li>Insert column header having the metric name,</li>
     * <li>Insert cells of timeseries references, from selected dataset,
     * selected by the join column, present in original table.</li>
     * </ul>
     * <p>
     * Join principle: for the inserted column J for metric XXX, each joined
     * timeseries at table[i,J] has the specified metric metadata value=XXX and
     * has the metadata
     * <ul>
     * <li>with name defined by the parameter joinMetaName</li>,
     * <li>whose value is equal to the value table[i,K] from join column K,
     * defined by the parameter joinColName.</li>
     * </ul>
     * <p>
     * Created columns are inserted according to the parameter targetColName.
     * 
     * @param tableJson
     *            the raw String representing the JSON plain content
     * @param metrics
     *            selected metrics separated by ";". Spaces are ignored.
     * @param dataset
     *            the dataset name.
     * @param joinColName
     *            the name of the table column used by the join. Optional: if
     *            undefined (""), the first column will be used by the join.
     * @param joinMetaName
     *            defines the name of metadata used by the join, useful when the
     *            column and metadata names are different. Optional default is
     *            undefined (""): if joinMetaName is undefined, then the
     *            metadata has the name of the table column used by the join
     *            (see joinColName), and if both criteria (joinColName +
     *            joinMetaName) are undefined: it is assumed that the first
     *            column header provides the expected metadata name.
     * @param targetColName
     *            name of the target column. Optional: default is undefined
     *            (""). When target name is defined, the joined columns are
     *            inserted before the target column; when undefined, the joined
     *            columns are appended at the end.
     * @param outputTableName
     *            name of the table joined by metric, and created in the
     *            database. The name ought to be conformed to the pattern:
     *            {@link TableManager#TABLE_NAME_PATTERN}
     * @return
     * @throws IkatsDaoException
     *             a database access error occured during the service.
     * @throws InvalidValueException
     *             error raised if one of the inputs is invalid.
     * @throws ResourceNotFoundException
     *             error raised if one of the resource required by computing is
     *             not found
     * @throws IkatsException
     *             unexpected error occured on the server.
     */
    @POST
    @SuppressWarnings("unchecked")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/join/metrics")
    public Response joinByMetrics(@FormDataParam("tableJson") String tableJson, @FormDataParam("metrics") String metrics,
            @FormDataParam("dataset") String dataset, @FormDataParam("joinColName") @DefaultValue("") String joinColName,
            @FormDataParam("joinMetaName") @DefaultValue("") String joinMetaName,
            @FormDataParam("targetColName") @DefaultValue("") String targetColName, @FormDataParam("outputTableName") String outputTableName)
            throws IkatsDaoException, InvalidValueException, ResourceNotFoundException, IkatsException {
        // the tableExprLogged is the logged expression about input table:
        // - temporary solution before passing explicite RID or table name to
        // the service
        String tableExprLogged = "null";
        // will be recomputed at step 0

        try {

            // step 0: check + prepare data
            // 
            // parse JSON resource
            TableManager tableManager = new TableManager();
            Table table = tableManager.initTable(tableJson);

            // toString() service provides a summary about the table
            tableExprLogged = table.toString();

            String finalJoinByColName = joinColName == null ? "" : joinColName.trim();
            String finalJoinByMetaName = joinMetaName == null ? "" : joinMetaName.trim();
            if (finalJoinByColName.isEmpty())
                finalJoinByColName = table.getColumnsHeader().getData().get(0).toString();
            if (finalJoinByMetaName.isEmpty())
                finalJoinByMetaName = finalJoinByColName;

            String finalTargetName = targetColName.equals("") ? null : targetColName.trim();

            if ( outputTableName == null || outputTableName.equals(""))
            {
                String msg = MessageFormat.format(TableResource.MSG_INVALID_INPUT_ERROR_JOIN_BY_METRICS,
                                                  "outputTableName", "", dataset, metrics, tableExprLogged );
                throw new InvalidValueException(msg);
            }
            
            
            // 1: restrict Timeseries to those having the metadata named
            // "metric" in the metrics list
            //
            // The metadata filtering is ignoring spaces around ';' but
            // we also remove the spaces starting/ending the metrics:
            String preparedMetrics = metrics.trim();

            if (preparedMetrics.length() == 0)
                throw new InvalidValueException(MessageFormat.format(MSG_INVALID_METRICS_FOR_JOIN_BY_METRICS, metrics, dataset, tableExprLogged));

            List<MetadataCriterion> selectByMetrics = new ArrayList<>();
            selectByMetrics.add(new MetadataCriterion("metric", SingleValueComparator.IN.getText(), preparedMetrics));
            FilterOnTsWithMetadata filterDataseByMetrics = new FilterOnTsWithMetadata();

            // Determine the list of dataset links from the datasetManager
            // => converts list of LinkDatasetTimeSeries into list of
            // FunctionalIdentifier
            List<FunctionalIdentifier> allDatasetFuncIds = datasetManager.getDataSetContent(dataset).stream()
                    .map(LinkDatasetTimeSeries::getFuncIdentifier).collect(Collectors.toList());

            filterDataseByMetrics.setTsList(allDatasetFuncIds);
            filterDataseByMetrics.setCriteria(selectByMetrics);

            List<FunctionalIdentifier> filteredFuncIds = metadataManager.searchFunctionalIdentifiers(filterDataseByMetrics);

            // 2: read, organize and store each used metadata on retained
            // timeseries in a specific join Map: ( <join identifier> => (
            // <metric> => ( funcId + tsuid )))
            // - key: <join identifier> (example: FlihtId= "899" )
            // - value: the map ( <metric> => ( funcId + tsuid )):
            // - key: <metric>
            // - value: FunctionalIdentifier
            //
            // 2.1 prepare the String list of tsuids: useful to create a map
            // associating tsuids to FunctionalIdentifier
            //
            Map<String, FunctionalIdentifier> originalRefs = new HashMap<>();
            for (FunctionalIdentifier functionalIdentifier : filteredFuncIds)
                originalRefs.put(functionalIdentifier.getTsuid(), functionalIdentifier);

            // 2.2 get the metadata map: TSUID => list of Metadata
            //
            Set<String> filteredTsuids = originalRefs.keySet();
            Map<String, List<MetaData>> metaGroupedByTsuid = metadataManager.getMapGroupingByTsuid(filteredTsuids);

            // 2.3 finalize the map: joinMap
            //
            // <join name> => Metric name => TSUID
            //
            List<String> listMetrics = Arrays.asList(preparedMetrics.split("\\s*;\\s*"));
            Map<String, Map<String, FunctionalIdentifier>> joinMap = new HashMap<String, Map<String, FunctionalIdentifier>>();

            for (Map.Entry<String, List<MetaData>> entryMeta : metaGroupedByTsuid.entrySet()) {
                String tsuid = entryMeta.getKey();
                List<MetaData> metaForTsuid = entryMeta.getValue();

                // from current TS, search the metric value and the join
                // idendifier
                //
                String joinIdentifier = null;
                String metric = null;
                Iterator<MetaData> iterMeta = metaForTsuid.iterator();
                while (iterMeta.hasNext() && ((metric == null) || (joinIdentifier == null))) {
                    MetaData metaData = iterMeta.next();
                    String metaName = metaData.getName();

                    if (finalJoinByMetaName.equals(metaName)) {
                        joinIdentifier = metaData.getValue();
                    }
                    else if (metaName.equals("metric") && listMetrics.contains(metaData.getValue())) {
                        metric = metaData.getValue();
                    }
                }

                if ((metric != null) && (joinIdentifier != null)) {
                    // ... complete the joinMap, updating the
                    // entry ( key: joinIdentifier , value: fromMetricToFuncId )
                    //
                    Map<String, FunctionalIdentifier> fromMetricToFuncId = joinMap.get(joinIdentifier);
                    if (fromMetricToFuncId == null) {
                        fromMetricToFuncId = new HashMap<String, FunctionalIdentifier>();
                        joinMap.put(joinIdentifier, fromMetricToFuncId);
                    }
                    fromMetricToFuncId.put(metric, originalRefs.get(tsuid));
                }
            }

            // 3: complete and save the table
            // store table in db

            // throws ResourceNotFoundException
            table.sortRowsByColumnValues(finalJoinByColName);
            List<String> joinIdentifers = table.getColumn(finalJoinByColName);

            Collections.sort(listMetrics);
            DataLink defaultLink = new DataLink();

            // triggers ON the links, filling undefined ones with null
            table.enableLinks(false, null, false, null, true, DataLink.buildLink("todo link type", null, "todo link context"));

            for (String insertedMetric : listMetrics) {

                List<TableElement> metricColumn = new ArrayList<>();
                for (String joinIdentifier : joinIdentifers) {
                    FunctionalIdentifier addData = joinMap.get(joinIdentifier).get(insertedMetric);
                    DataLink link = new DataLink();
                    link.val = addData;

                    // add TableElement:
                    // - data: the funcId text
                    // - link: the json generated by FunctionalIdentifier
                    TableElement elem = new TableElement(addData.getFuncId(), link);
                    metricColumn.add(elem);
                }

                table.insertColumn(finalTargetName, insertedMetric, metricColumn);
            }

            String rid = tableManager.createInDatabase(outputTableName, table.getTableInfo());

            // result id is returned in the body
            return Response.status(Response.Status.OK).entity(rid).build();
        }
        catch (IkatsJsonException jsonError) {
            String msg = MessageFormat.format(MSG_INVALID_TABLE_FOR_JOIN_BY_METRICS, tableExprLogged, dataset, metrics);
            throw new InvalidValueException(msg, jsonError);
        }
        catch (IkatsDaoException daoError) {
            String msg = MessageFormat.format(MSG_DAO_KO_JOIN_BY_METRICS, dataset, metrics, tableExprLogged);
            throw new IkatsDaoException(msg, daoError);
        }
        catch (ResourceNotFoundException rnfError) {
            // Resource not found error occured ...
            String msg = MessageFormat.format(MSG_RESOURCE_NOT_FOUND_JOIN_BY_METRICS, dataset, metrics, tableExprLogged);
            throw new ResourceNotFoundException(msg, rnfError);
        }
        catch (Exception e) {
            String msg = MessageFormat.format(MSG_UNEXPECTED_ERROR_JOIN_BY_METRICS, dataset, metrics, tableExprLogged);
            throw new IkatsException(msg, e);
        }

    }

    /**
     * Table process to :
     * - change table key from metadata (populationId)
     * - add metadata (metaName) value to original column headers  : metaName_colHeader
     * <p>
     * Input table first column must be time series functional identifiers
     *
     * @param tableJson       the table to convert (json)
     * @param metaName        name of metadata to concat with agregates ref
     * @param populationId    id of population (which is in fact a metadata name) = key of output table
     * @param outputTableName name of the table generated
     * @param formData        the form data
     * @param uriInfo         all info on URI
     * @return the internal id
     * @throws IOException               error when parsing input csv file
     * @throws IkatsDaoException         error while accessing database to check if table already exists
     * @throws ResourceNotFoundException if table not found
     * @throws InvalidValueException     if table name does not match expected pattern
     * @throws IkatsException            others unexpected exceptions
     */
    @POST
    @Path("/ts2feature")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @SuppressWarnings("unchecked")
    public Response ts2Feature(@FormDataParam("tableJson") String tableJson,
                               @FormDataParam("metaName") String metaName,
                               @FormDataParam("populationId") String populationId,
                               @FormDataParam("outputTableName") String outputTableName,
                               FormDataMultiPart formData,
                               @Context UriInfo uriInfo) throws IOException, IkatsDaoException, IkatsException, ResourceNotFoundException, InvalidValueException {


        if (outputTableName == null) {
            // check outputTableName is not null
            String context = "outputTableName shall not be null";
            return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
        }

        if (tableManager.existsInDatabase(outputTableName)) {
            // check output table name does not already exists
            String context = "Output table name already exists : choose a different one (" + outputTableName + ")";
            return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
        }

        if (populationId == null) {
            // check population id is not null
            String context = "populationId shall not be null";
            return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
        }

        Chronometer chrono = new Chronometer(uriInfo.getPath(), true);

        // convert tableJson to table
        TableInfo tableInfo = tableManager.loadFromJson(tableJson);
        Table table = tableManager.initTable(tableInfo, false);

        logger.info("Working on table (" + table.getDescription() + ") " +
                "with metaName (" + metaName + ") " +
                "and with populationId (" + populationId + ")");
        logger.info("Output table name is (" + outputTableName + ")");

        // retrieve headers
        List<Object> colHeaders = table.getColumnsHeader().getData();
        List<Object> rowHeaders = table.getRowsHeader().getData();

        MetaDataFacade metaFacade = new MetaDataFacade();
        List<String> colPopId = new ArrayList<>();
        List<String> colMetaTs = new ArrayList<>();
        for (int i = 1; i < rowHeaders.size(); i++) {

            // retrieving tsuids of input
            String tsuid = metaFacade.getFunctionalIdentifierByFuncId(rowHeaders.get(i).toString()).getTsuid();
            MetaData metaTs = metaFacade.getMetaData(tsuid, metaName);
            MetaData metaPopId = metaFacade.getMetaData(tsuid, populationId);

            // filling new colPopId column
            colPopId.add(metaPopId.getValue());

            // filling new colMetaTs column
            colMetaTs.add(metaTs.getValue());
        }

        // processing an ordered list of populationId values without duplicates
        Set<String> setPopId = new HashSet<>(colPopId);
        List<String> listPopId = new ArrayList<>(setPopId);
        Collections.sort(listPopId);

        // processing an ordered list of metaName values by ts without duplicates
        Set<String> setMetaTs = new HashSet<>(colMetaTs);
        List<String> listMetaTs = new ArrayList<>(setMetaTs);
        Collections.sort(listMetaTs);

        // filling outputTable
        // filling col headers
        // first element is null
        Table outputTable = tableManager.initEmptyTable();
        outputTable.getColumnsHeader().addItem(null);
        for (String metaTs : listMetaTs) {
            for (int i = 1; i < colHeaders.size(); i++) {
                outputTable.getColumnsHeader().addItem(metaTs + "_" + colHeaders.get(i));
            }
        }
        int tableContentWidth = outputTable.getColumnsHeader().getData().size() - 1;

        // filling rows headers and content by popId
        outputTable.getRowsHeader().addItem(populationId);
        for (String popId : listPopId) {
            outputTable.getRowsHeader().addItem(popId);
            List<Integer> listIndexPopId = retrieveIndexesListOfEltInList(popId, colPopId);

            List<Object> cellsLine = new ArrayList<>();
            for (String metaTS : listMetaTs) {
                for (Integer index : listIndexPopId) {
                    if (metaTS.equals(colMetaTs.get(index))) {
                        cellsLine.addAll(table.getRow(index + 1));
                    }
                }
            }
            // check line size is consistent
            if (cellsLine.size() != tableContentWidth) {
                String context = "Output table : line length inconsistency";
                return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
            }
            outputTable.appendRow(cellsLine);
        }
        // store table in db
        String rid = tableManager.createInDatabase(outputTableName, outputTable.getTableInfo());

        chrono.stop(logger);

        // result id is returned in the body
        return Response.status(Response.Status.OK).entity(rid).build();
    }

    /**
     * Return list of indexes where element is present in the list
     */
    private List<Integer> retrieveIndexesListOfEltInList(String element, List<String> list) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).equals(element)) {
                result.add(i);
            }
        }
        return result;
    }

}
