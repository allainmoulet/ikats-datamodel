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
 * @author ftoral
 */

package fr.cs.ikats.operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import fr.cs.ikats.temporaldata.business.table.Table;
import fr.cs.ikats.temporaldata.business.table.TableInfo;
import fr.cs.ikats.temporaldata.business.table.TableManager;
import fr.cs.ikats.temporaldata.business.table.TableUtils;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;

/**
 * Class TrainTestSplitTable
 */
public class TrainTestSplitTable {

    /**
     * Information to be provided to the {@link TablesMerge} operator
     */
    public static class Request {

        public String tableName;
        public String targetColumnName;
        public double repartitionRate;
        public String outputTableName;

        public Request() {
            ; // default constructor
        }
    }

    private Request request;
    private TableManager tableManager;

    /**
     * TrainTestSplit Table operator initialization
     *
     * @param request the input data provided to the operator
     */
    public TrainTestSplitTable(Request request) {

        this.request = request;
        this.tableManager = new TableManager();
    }
    
    /**
     * Package private method to be used in tests
     */
    TrainTestSplitTable() {
        this.tableManager = new TableManager();
    }
    
    /**
     * Apply the operator to the {@link Request}, save the result.
     * @throws ResourceNotFoundException 
     * @throws IkatsException 
     * @throws IkatsDaoMissingRessource 
     * @throws InvalidValueException 
     * @throws IkatsDaoConflictException 
     *
     * @throws IkatsOperatorException
     */
    public void apply() throws IkatsDaoMissingRessource, IkatsException, ResourceNotFoundException, IkatsDaoConflictException, InvalidValueException {
        
        // do the job
        List<Table> tabListResult = doCompute();
        
        // Store tables in database
        tabListResult.get(0).setName(request.outputTableName + "_Train");
        tabListResult.get(1).setName(request.outputTableName + "_Test");
        tableManager.createInDatabase(tabListResult.get(0).getTableInfo());
        tableManager.createInDatabase(tabListResult.get(1).getTableInfo());
    }
    
    /**
     * @param tableName
     * @param targetColumnName
     * @param repartitionRate
     * @param outputTableName
     * @throws IkatsException 
     * @throws IkatsDaoMissingRessource 
     * @throws ResourceNotFoundException 
     */
    public List<Table> doCompute() throws IkatsDaoMissingRessource, IkatsException, ResourceNotFoundException {
        
        // retrieve table tableName from db
        TableInfo tableInfo = tableManager.readFromDatabase(request.tableName);
        Table table = tableManager.initTable(tableInfo, false);
    
        List<Table> tabListResult;
        if ("".equals(request.targetColumnName)) {
            tabListResult = randomSplitTable(table, request.repartitionRate);
        } else {
            tabListResult = trainTestSplitTable(table, request.targetColumnName, request.repartitionRate);
        }
    
        return tabListResult;
    }

    /**
     * Randomly split table in 2 tables according to repartition rate
     * ex : repartitionRate = 0.6
     * => table1 = 60% of input table
     * => table2 = 40% of input table
     * output = [table1 ; table2]
     *
     * @param table           original table to process
     * @param repartitionRate repartition rate between two output tables
     * @throws IkatsException            row from original table is undefined
     * @throws ResourceNotFoundException row from original table is not found
     */
    List<Table> randomSplitTable(Table table, double repartitionRate) throws ResourceNotFoundException, IkatsException {
    
        List<Integer> indexListInput = new ArrayList<>();
        List<List<Integer>> indexListOutput;
        boolean withColHeaders = table.isHandlingColumnsHeader();
        boolean withRowHeaders = table.isHandlingRowsHeader();
    
        // Generate list of row indexes of input table
        for (int i = 0; i < table.getRowCount(false); i++) {
            indexListInput.add(i);
        }
    
        // Randomly split indexes list
        indexListOutput = randomSplitTableIndexes(indexListInput, repartitionRate);
    
        // Result initialization
        Table table1 = TableUtils.initEmptyTable(withColHeaders, withRowHeaders);
        Table table2 = TableUtils.initEmptyTable(withColHeaders, withRowHeaders);
        List<Table> result = new ArrayList<>();
        result.add(table1);
        result.add(table2);
    
        // Extract rows at split indexes to generate output
        table1 = extractIndexes(table, table1, indexListOutput.get(0));
        table2 = extractIndexes(table, table2, indexListOutput.get(1));
        if (withColHeaders) {
            table1.getColumnsHeader().addItems(table.getColumnsHeader().getItems().toArray());
            table2.getColumnsHeader().addItems(table.getColumnsHeader().getItems().toArray());
        }
    
        // Assuming first column is id, sorting output tables
        table1.sortRowsByColumnValues(0, false);
        table2.sortRowsByColumnValues(0, false);
    
        return result;
    
    }
    
    /**
     * Randomly split table list indexes in 2 indexes lists according to repartition rate
     * ex : repartitionRate = 0.6
     * => list1 = 60% of input list (
     * => list2 = 40% of input list
     * output = [table1 ; table2]
     * <p>
     * NB: number of items in output lists are rounded to the nearest value
     *
     * @param indexList       list of table indexes
     * @param repartitionRate repartition rate between two output indexes lists
     */
    private List<List<Integer>> randomSplitTableIndexes(List<Integer> indexList, double repartitionRate) {
    
        // Randomize list content
        Collections.shuffle(indexList);
        int nbItems = indexList.size();
    
        // Constraint the range of repartition to [0, 1] if overshoot the limits
        repartitionRate = Math.max(repartitionRate, 0);
        repartitionRate = Math.min(repartitionRate, 1);
    
        // Compute index of list where to split
        int indexSplit = (int) Math.round(nbItems * repartitionRate);
    
        List<List<Integer>> result = new ArrayList<>();
    
        // Splitting
        result.add(new ArrayList<>(indexList.subList(0, indexSplit)));
        result.add(new ArrayList<>(indexList.subList(indexSplit, nbItems)));
    
        return result;
    
    }
    

    /**
     * Original input table is randomly split into 2 tables according to repartition rate
     * Here values from targetColumnName are equally distributed in each new table
     * <p>
     * ex :
     * 2 classes A, B
     * table : 10 items => 3 items A, 7 items B
     * repartitionRate = 0.6
     * => table1 = 60% of input table (6 items => 4 items A, 2 items B)
     * => table2 = 40% of input table (4 items => 2 items A, 2 items B)
     * output = [table1 ; table2]
     * <p>
     * Note: number of items in output tables are rounded to the nearest value
     *
     * @param table            the table to split
     * @param targetColumnName name of the target column in input table
     * @param repartitionRate  repartition rate between learning and test sets in output
     * @throws ResourceNotFoundException if target column name not found in table
     * @throws IkatsException            if targetColumnName is null
     */
    List<Table> trainTestSplitTable(Table table, String targetColumnName, double repartitionRate) throws
            ResourceNotFoundException, IkatsException {
    
        boolean withColHeaders = table.isHandlingColumnsHeader();
        boolean withRowHeaders = table.isHandlingRowsHeader();
    
        // Sort table by column 'target'
        table.sortRowsByColumnValues(targetColumnName, false);
    
        // Extract classes column
        List<String> classColumnContent = table.getColumn(targetColumnName);
    
        // Building list of indexes where classes change
        List<Integer> indexList = new ArrayList<>();
        Object lastClassValue = classColumnContent.get(0);
        for (int i = 1; i < classColumnContent.size(); i++) {
            if (!classColumnContent.get(i).equals(lastClassValue)) {
                indexList.add(i - 1);
                lastClassValue = classColumnContent.get(i);
            }
        }
    
        // Creating indexes list by class
        int nbLines = classColumnContent.size();
        List<List<List<Integer>>> indexesListByClass = new ArrayList<>();
        Iterator<Integer> iteratorIndexList = indexList.iterator();
        List<Integer> listIndexToAppend = new ArrayList<>();
    
        // Handle the case where there is only 1 class
        int nextIndex = nbLines - 1;
        if (iteratorIndexList.hasNext()) {
            nextIndex = iteratorIndexList.next();
        }
        for (int i = 0; i < nbLines; i++) {
            listIndexToAppend.add(i);
            if (i >= nextIndex) {
                indexesListByClass.add(randomSplitTableIndexes(listIndexToAppend, repartitionRate));
                listIndexToAppend.clear();
                if (iteratorIndexList.hasNext()) {
                    nextIndex = iteratorIndexList.next();
                } else {
                    nextIndex = nbLines - 1;
                }
            }
        }
    
        // Result initialization
        Table table1 = TableUtils.initEmptyTable(withColHeaders, withRowHeaders);
        Table table2 = TableUtils.initEmptyTable(withColHeaders, withRowHeaders);
        List<Table> result = new ArrayList<>();
        result.add(table1);
        result.add(table2);
    
        // Filling column headers
        if (withColHeaders) {
            table1.getColumnsHeader().addItems(table.getColumnsHeader().getItems().toArray());
            table2.getColumnsHeader().addItems(table.getColumnsHeader().getItems().toArray());
        }
        // Initialization of row headers (first item must be null)
        if (withRowHeaders) {
            table1.getRowsHeader().addItem(null);
            table2.getRowsHeader().addItem(null);
        }
    
        // Retrieving rows from original table according to list of indexes previously generated
        // and filling output tables
        // Shifting indexes in case of row headers
        int shift = (table.isHandlingColumnsHeader()) ? 1 : 0;
        for (List<List<Integer>> indexesList : indexesListByClass) {
            List<Integer> tableRated1 = indexesList.get(0);
            List<Integer> tableRated2 = indexesList.get(1);
            for (Integer aTableRated1 : tableRated1) {
                table1.appendRow(table.getRow(aTableRated1 + shift, Object.class));
                if (withRowHeaders) {
                    table1.getRowsHeader().addItem(table.getRowsHeader().getItems().get(aTableRated1 + 1));
                }
            }
            for (Integer aTableRated2 : tableRated2) {
                table2.appendRow(table.getRow(aTableRated2 + shift, Object.class));
                if (withRowHeaders) {
                    table2.getRowsHeader().addItem(table.getRowsHeader().getItems().get(aTableRated2 + 1));
                }
            }
        }
    
        // assuming first column is id, sorting output tables
        table1.sortRowsByColumnValues(0, false);
        table2.sortRowsByColumnValues(0, false);
    
        return result;
    }

    /**
     * Extract rows from tableIn matching list of indexes (indexList) to tableOut.
     *
     * @param tableIn   table in from which rows are extracted
     * @param tableOut  table out contains only rows from tableIn matching indexList indexex
     * @param indexList list of rows indexes to extract from tableIn
     * @throws IkatsException            row from original table is undefined
     * @throws ResourceNotFoundException row from original table is not found
     */
    private Table extractIndexes(Table tableIn, Table tableOut, List<Integer> indexList) throws ResourceNotFoundException, IkatsException {
    
        // Shifting indexes in case of row headers
        int shift = (tableIn.isHandlingColumnsHeader()) ? 1 : 0;
    
        // Init row headers (first item is null)
        if (tableIn.isHandlingRowsHeader()) {
            tableOut.getRowsHeader().addItem(null);
        }
    
        // Retrieving rows from original table according to list of indexes previously generated
        // and filling output tables (row headers included, if managed)
        for (Integer index : indexList) {
            if (tableOut.isHandlingRowsHeader()) {
                tableOut.getRowsHeader().addItem(tableIn.getRowsHeader().getItems().get(index + 1));
            }
            tableOut.appendRow(tableIn.getRow(index + shift, Object.class));
        }
        return tableOut;
    
    }

    
}
