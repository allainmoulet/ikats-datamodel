/**
 * LICENSE:
 * --------
 * Copyright 2017 CS SYSTEMES D'INFORMATION
 * <p>
 * Licensed to CS SYSTEMES D'INFORMATION under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. CS licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * @author Fabien TORAL <fabien.toral@c-s.fr>
 * @author Fabien TORTORA <fabien.tortora@c-s.fr>
 * @author Mathieu BERAUD <mathieu.beraud@c-s.fr>
 * @author Maxime PERELMUTER <maxime.perelmuter@c-s.fr>
 */

package fr.cs.ikats.operators;

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

import fr.cs.ikats.temporaldata.business.*;
import org.apache.log4j.Logger;

//Review#158227 FTL CTRL + SHIFT + o -> réorganise les imports, en enlevant les inutiles.
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.expr.SingleValueComparator;
import fr.cs.ikats.lang.NaturalOrderComparator;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.metadata.model.MetadataCriterion;
import fr.cs.ikats.temporaldata.business.TableInfo.DataLink;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.IkatsJsonException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.resource.TableResource;
import fr.cs.ikats.temporaldata.utils.Chronometer;
import fr.cs.ikats.ts.dataset.model.LinkDatasetTimeSeries;

/**
 * Provides the computation for the Table Operator "Join By Metrics". <br>
 * The documentation of the operator is available at {@link TableResource#joinByMetrics(String, String, String, String, String, String, String)}
 */
public class JoinTableWithTs {

    /**
     * the LOGGER instance for this class
     */
    private static final Logger LOGGER = Logger.getLogger(JoinTableWithTs.class);

    final static String MSG_TABLE_PROCESSING_CONTEXT = "[JoinTableWithTs step ''{0}'' on table='''{1}']";
    static final String MSG_DAO_KO_JOIN_BY_METRICS = "Failed to apply operator in context=''{2}'': DAO error occured with dataset name=''{0}'' on metrics=''{1}''";
    static final String MSG_INVALID_METRICS_FOR_JOIN_BY_METRICS = "Invalid metrics value=''{0}'' with dataset name=''{1}'' context=''{2}''";
    static final String MSG_INVALID_TABLE_FOR_JOIN_BY_METRICS = "Invalid table in context=''{0}'' with dataset name=''{1}'' and metrics=''{2}''";
    static final String MSG_RESOURCE_NOT_FOUND_JOIN_BY_METRICS = "Resource not found error occurred in context=''{2}'' with dataset name=''{0}'' metrics=''{1}''";
    static final String MSG_UNEXPECTED_ERROR_JOIN_BY_METRICS = "Unexpected error occured in context=''{2}'' with dataset name=''{0}'' on metrics=''{1}''";
    static final String MSG_INVALID_INPUT_ERROR_JOIN_BY_METRICS = "Invalid input name=''{0}'' value=''{1}'' in context=''{4}'' with dataset name=''{2}'' on metrics=''{3}''";
    static final String MSG_ERROR_SELECTED_DATASET_WITHOUT_SELECTED_METRICS = "Selected dataset does not contain any of the selected metrics";

    // presently there is no specific rule about the title and desc of output: here are proposed the title+desc set on
    // output
    static final String MSG_NEW_TITLE = "''{0}'' = JoinTableWithTs( ''{1}'' )";
    static final String MSG_NEW_DESC = "JoinTableWithTs with metrics=''{0}'', joinColName=''{1}'', joinMetaName=''{2}'', targetColName=''{3}'', outputTableName=''{4}'': on table with title=''{5}'' and desc=''{6}''";

    /**
     * The processed Table for the logs
     */
    private Table processedTable;

    /**
     * The processing context for the logs
     */
    private String processingContext = "initial";

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
     * Default constructor initializes the resources
     */
    public JoinTableWithTs() {
        super();
        this.processedTable = null;
        this.tableManager = new TableManager();
        this.metaManager = new MetaDataManager();
        this.datasetManager = new DataSetManager();
    }

    /**
     * See API doc reference in
     * {@link TableResource#joinByMetrics(String, String, String, String, String, String, String)}
     *
     * @param tableInfo
     * @param metrics
     * @param dataset
     * @param joinColName
     * @param joinMetaName
     * @param targetColName
     * @param outputTableName
     * @return the
     * @throws IkatsDaoException
     * @throws InvalidValueException
     * @throws ResourceNotFoundException
     * @throws IkatsException
     */
    public void apply(TableInfo tableInfo, String metrics, String dataset, String joinColName, String joinMetaName,
                         String targetColName, String outputTableName)
            throws IkatsDaoException, InvalidValueException, ResourceNotFoundException, IkatsException {
        String prefixeChrono = "JoinTableWithTs: init";
        Chronometer chrono = new Chronometer(prefixeChrono, true);
        try {
            processingContext = "computing table";
            chrono.start(prefixeChrono + processingContext);
            processedTable = computeTable(tableInfo, metrics, dataset, joinColName, joinMetaName, targetColName,
                    outputTableName);
            chrono.stop(LOGGER);

            processingContext = "creating table in DB";
            chrono.start(prefixeChrono + processingContext);
            processedTable.setName(outputTableName);
            tableManager.createInDatabase(processedTable.getTableInfo());
            chrono.stop(LOGGER);
        } catch (IkatsJsonException jsonError) {
            chrono.stop(LOGGER);
            String msg = MessageFormat.format(MSG_INVALID_TABLE_FOR_JOIN_BY_METRICS, getContext(), dataset, metrics);
            throw new InvalidValueException(msg, jsonError);
        } catch (IkatsDaoException daoError) {
            chrono.stop(LOGGER);
            String msg = MessageFormat.format(MSG_DAO_KO_JOIN_BY_METRICS, dataset, metrics, getContext());
            throw new IkatsDaoException(msg, daoError);
        } catch (Exception e) {
            chrono.stop(LOGGER);
            String msg = MessageFormat.format(MSG_UNEXPECTED_ERROR_JOIN_BY_METRICS, dataset, metrics, getContext());
            throw new IkatsException(msg, e);
        }
    }

    /**
     * The main step computing the table with added metric columns
     *
     * @param tableInfo
     * @param metrics
     * @param dataset
     * @param joinColName
     * @param joinMetaName
     * @param targetColName
     * @param outputTableName
     * @return
     * @throws IkatsDaoException
     * @throws InvalidValueException
     * @throws ResourceNotFoundException
     * @throws IkatsException
     */
    Table computeTable(TableInfo tableInfo, String metrics, String dataset, String joinColName, String joinMetaName,
                       String targetColName, String outputTableName)
            throws IkatsDaoException, InvalidValueException, ResourceNotFoundException, IkatsException {
        try {

            // step 0: check + prepare data
            //
            processingContext = "loads the JSON content";

            TableManager tableManager = new TableManager();
            processedTable = tableManager.initTable(tableInfo, false);

            processingContext = "check+prepare parameters";
            String finalJoinByColName = joinColName == null ? "" : joinColName.trim();
            String finalJoinByMetaName = joinMetaName == null ? "" : joinMetaName.trim();
            if (finalJoinByColName.isEmpty())
                finalJoinByColName = processedTable.getColumnsHeader().getItems().get(0);
            if (finalJoinByMetaName.isEmpty())
                finalJoinByMetaName = finalJoinByColName;

            String finalTargetName = targetColName == null ? "" : targetColName.trim();

            if (outputTableName == null || outputTableName.equals("")) {
                String msg = MessageFormat.format(JoinTableWithTs.MSG_INVALID_INPUT_ERROR_JOIN_BY_METRICS,
                        "outputTableName", "", dataset, metrics, getContext());
                throw new InvalidValueException(msg);
            }

            // updates the new desc section from input table
            // - new title
            // - new desc
            String oldTitle = processedTable.getTitle();
            if (oldTitle == null)
                oldTitle = "";
            String oldDesc = processedTable.getDescription();
            if (oldDesc == null)
                oldDesc = "";
            String newTitle = MessageFormat.format(MSG_NEW_TITLE, outputTableName, oldTitle);
            String newDesc = MessageFormat.format(MSG_NEW_DESC, metrics, joinColName, joinMetaName,
                    targetColName, outputTableName, oldTitle, oldDesc);

            processedTable.setTitle(newTitle);
            processedTable.setDescription(newDesc);

            // step 1: restrict Timeseries to those having the metadata named
            // "metric" in the metrics list
            processingContext = "finds the timeseries matching metrics";
            // The metadata filtering is ignoring spaces around ';' but
            // we also remove the spaces starting/ending the metrics:
            String preparedMetrics = metrics.trim();

            if (preparedMetrics.length() == 0)
                throw new InvalidValueException(
                        MessageFormat.format(MSG_INVALID_METRICS_FOR_JOIN_BY_METRICS, metrics, dataset, getContext()));

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

            List<FunctionalIdentifier> filteredFuncIds = metaManager.searchFunctionalIdentifiers(filterDataseByMetrics);

            // 2: read, organize and store each used metadata on retained
            // timeseries in a specific join Map: ( <join identifier> => (
            // <metric> => ( funcId + tsuid )))
            // - key: <join identifier> (example: FlihtId= "899" )
            // - value: the map ( <metric> => ( funcId + tsuid )):
            // - key: <metric>
            // - value: FunctionalIdentifier
            //
            processingContext = "prepare the join map from metadata";

            // 2.1 prepare the String list of tsuids: useful to create a map
            // associating tsuids to FunctionalIdentifier
            Map<String, FunctionalIdentifier> originalRefs = new HashMap<>();
            for (FunctionalIdentifier functionalIdentifier : filteredFuncIds)
                originalRefs.put(functionalIdentifier.getTsuid(), functionalIdentifier);

            // 2.2 get the metadata map: TSUID => list of Metadata
            //
            Set<String> filteredTsuids = originalRefs.keySet();
            Map<String, List<MetaData>> metaGroupedByTsuid = metaManager.getMapGroupingByTsuid(filteredTsuids);

            // 2.3 finalize the map: joinMap
            //
            // <join name> => Metric name => TSUID
            //
            List<String> listMetrics = Arrays.asList(preparedMetrics.split("\\s*;\\s*"));
            Map<String, Map<String, FunctionalIdentifier>> joinMap = new HashMap<String, Map<String, FunctionalIdentifier>>();

            // set metricsInDataset: set of metrics in dataset selection
            Set<String> metricsInDataset = new HashSet<>();
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
                    } else if (metaName.equals("metric") && listMetrics.contains(metaData.getValue())) {
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
                    metricsInDataset.add(metric);
                }
            }

            if (metricsInDataset.isEmpty()) {
                // ResourceNotFoundException error will be wrapped with precise context: see catch below.
                throw new ResourceNotFoundException(MSG_ERROR_SELECTED_DATASET_WITHOUT_SELECTED_METRICS);
            }

            // 3: complete the table
            processingContext = "insert the TS columns";

            // throws ResourceNotFoundException
            processedTable.sortRowsByColumnValues(finalJoinByColName, false);
            List<String> joinIdentifers = processedTable.getColumn(finalJoinByColName);

            // triggers ON the links, filling undefined ones with null
            processedTable.enableLinks(false, null, false, null, true, DataLink.buildLink("ts_list", null, "raw"));

            // Iterate only on the metrics in the dataset
            List<String> filteredAndSortedMetrics = new ArrayList<>(listMetrics);
            filteredAndSortedMetrics.retainAll(metricsInDataset);
            Collections.sort(filteredAndSortedMetrics, new NaturalOrderComparator());

            for (String insertedMetric : filteredAndSortedMetrics) {

                List<TableElement> metricColumn = new ArrayList<>();
                for (String joinIdentifier : joinIdentifers) {

                    // prepare added element for the joinIdentifier
                    TableElement elem = null;
                    Map<String, FunctionalIdentifier> mapFuncIdsByMetrics = joinMap.get(joinIdentifier);
                    if (mapFuncIdsByMetrics != null) {
                        // ID=joinIdentifier exists in selected dataset
                        // => Find element for insertedMetric ...
                        FunctionalIdentifier addData = mapFuncIdsByMetrics.get(insertedMetric);
                        if (addData != null) {
                            // element is defined: one TS matches ID+insertedMetric
                            DataLink link = new DataLink();

                            // it is needed to wrap the unique FunctionalIdentifier into a list:
                            // because the ts_list type, in json, is like [{ 'tsuid': ..., 'funcId': ... }, ...]
                            link.val = Arrays.asList(addData);

                            // add TableElement:
                            // - data: the funcId text
                            // - link: the json generated by FunctionalIdentifier
                            elem = new TableElement(addData.getFuncId(), link);
                        }
                    }
                    // treat cases when elem is undefined:
                    // - missing entry joinIdentifier in joinMap: there is no such ID in the dataset selection
                    // - missing entry insertedMetric in mapFuncIdsByMetrics: ID exists but not for insertedMetric
                    if (elem == null) {
                        elem = new TableElement(null, null);
                    }

                    metricColumn.add(elem);
                }

                // append or insert the new column ...
                if ("".equals(finalTargetName)) {
                    processedTable.appendColumn(insertedMetric, metricColumn);
                } else {
                    processedTable.insertColumn(finalTargetName, insertedMetric, metricColumn);
                }

            }

            return processedTable;
        } catch (IkatsJsonException jsonError) {
            String msg = MessageFormat.format(MSG_INVALID_TABLE_FOR_JOIN_BY_METRICS, getContext(), dataset, metrics);
            throw new InvalidValueException(msg, jsonError);
        } catch (IkatsDaoException daoError) {
            String msg = MessageFormat.format(MSG_DAO_KO_JOIN_BY_METRICS, dataset, metrics, getContext());
            throw new IkatsDaoException(msg, daoError);
        } catch (ResourceNotFoundException rnfError) {
            // Resource not found error occured ...
            String msg = MessageFormat.format(MSG_RESOURCE_NOT_FOUND_JOIN_BY_METRICS, dataset, metrics, getContext());
            throw new ResourceNotFoundException(msg, rnfError);
        } catch (Exception e) {
            String msg = MessageFormat.format(MSG_UNEXPECTED_ERROR_JOIN_BY_METRICS, dataset, metrics, getContext());
            throw new IkatsException(msg, e);
        }
    }

    /**
     *
     * @return information about the operator context: processing context and processed table
     */
    public String getContext() {
        return MessageFormat.format(MSG_TABLE_PROCESSING_CONTEXT, processingContext, processedTable);

    }

}

