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
 * @author Mathieu BERAUD <mathieu.beraud@c-s.fr>
 * @author Maxime PERELMUTER <maxime.perelmuter@c-s.fr>
 */

package fr.cs.ikats.operators;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import fr.cs.ikats.temporaldata.business.Table;
import fr.cs.ikats.temporaldata.business.TableElement;
import fr.cs.ikats.temporaldata.business.TableInfo;
import fr.cs.ikats.temporaldata.business.TableInfo.DataLink;
import fr.cs.ikats.temporaldata.business.TableInfo.Header;
import fr.cs.ikats.temporaldata.business.TableManager;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;

/**
 * <p>IKATS Operator Tables Merge</p>
 * <p>
 * <p>Provides the ability to join 2 tables with an inner join. The instructions are set by the {@link Request}.</p>
 * <p>
 * <p>Rules:
 * <ul>
 * <li>The operator produces an inner join between 2 tables only</li>
 * <li>The first table (in the order) is the reference : all columns will be in the result</li>
 * <li>The operator will copy all the columns of the second table except the join column</li>
 * <li>The join key (see {@link Request#joinOn}) is case sensitive</li>
 * <li>The join key should be found in the 2 tables</li>
 * <li>If the join key is not provided, the first column in the first table is selected,
 * <ul>
 * <li>If the first table has a header name for that column, that name will be used as join  key to select the join column in the second table</li>
 * <li>If the first table has no header, the first column of the second table is used to search matches for the join</li>
 * </ul>
 * <li>If the first table has no header and there is no key provided, the first columns of the two tables are used to match for the join</li>
 * <li>As per {@link TableInfo} construction, each of the two tables could, or not, manage headers, and datalinks. The result will report all the elements.
 * </ul>
 * </p>
 */
public class TablesMerge {

    static private final Logger logger = Logger.getLogger(TablesMerge.class);

    /**
     * Information to be provided to the {@link TablesMerge} operator
     */
    public static class Request {

        public String[] tableNames;
        public String joinOn;
        public String outputTableName;

        public Request() {
            ; // default constructor
        }
    }

    private Request request;
    private TableManager tableManager;

    /**
     * Table Merge operator initialization
     *
     * @param request the input data provided to the operator
     * @throws IkatsOperatorException when there is only 1 table to merge
     */
    public TablesMerge(Request request) throws IkatsOperatorException {

        // Check the inputs
        if (request.tableNames == null || request.tableNames.length < 2) {
            throw new IkatsOperatorException("There should be 2 tables for a merge");
        }

        this.request = request;
        this.tableManager = new TableManager();
    }

    /**
     * Package private method to be used in tests
     */    
    TablesMerge() {
        this.tableManager = new TableManager();        
    }

    /**
     * Apply the operator the {@link Request}, save the result.
     *
     * @throws IkatsOperatorException
     */
    public void apply() throws IkatsOperatorException, IkatsException, IkatsDaoConflictException {

        // Retrieve the tables from database
        Table firstTable;
        Table secondTable;
        String tableNameToRead = null;
        try {
            tableNameToRead = this.request.tableNames[0];
            firstTable = tableManager.initTable(tableManager.readFromDatabase(tableNameToRead), false);
            tableNameToRead = this.request.tableNames[1];
            secondTable = tableManager.initTable(tableManager.readFromDatabase(tableNameToRead), false);
        } catch (IkatsDaoMissingRessource e) {
            String msg = "Table " + tableNameToRead + " not found in database";
            throw new IkatsOperatorException(msg, e);
        }

        // do the job
        Table resultTable = doMerge(firstTable.getTableInfo(), secondTable.getTableInfo(), request.joinOn, request.outputTableName);

        // then try to store it in the database
        try {
            tableManager.createInDatabase(resultTable.getTableInfo());
            logger.info("Table '" + resultTable.getName() + "' by '" + TablesMerge.class.getName() + "' operator, created in database");
        } catch (IkatsException | InvalidValueException e) {
            String msgFormat = "The table ''{0}'' could not be saved to database. Error message: ''{1}''";
            String msg = MessageFormat.format(msgFormat, resultTable.getName(), e.getMessage());
            throw new IkatsOperatorException(msg, e);
        }
    }

    /**
     * Operator processing for the merge
     *
     * @param tableInfo1 the reference table to join on
     * @param tableInfo2 the second table where columns should match
     * @param joinOn the join key
     * @param outputTableName the result table name
     * @return the merged table
     * @throws IkatsOperatorException if table is badly formatted
     */
    Table doMerge(TableInfo tableInfo1, TableInfo tableInfo2, String joinOn, String outputTableName) throws IkatsOperatorException, IkatsException {

        // Normalize the request data
        Table firstTable = tableManager.initTable(tableInfo1, false);
        Table secondTable = tableManager.initTable(tableInfo2, false);
        String joinKey = (joinOn != null && !joinOn.isEmpty()) ? joinOn : null;

        // -- Get the left join column header name and index for the first table
        int joinIndexOnFirstTable = -1;
        if (joinKey == null) {
            joinIndexOnFirstTable = 0;
            // Case of the non provided join key => get the first column in the first table
            try {
                if (firstTable.getColumnsHeader() != null) {
                    joinKey = firstTable.getColumnsHeader().getItems().get(0);
                }
            }
            catch (IkatsException e) {
                throw new IkatsOperatorException("The table '" + firstTable.getName() + "' has no column", e);
            }
        }
        else {
            try {
                joinIndexOnFirstTable = firstTable.getIndexColumnHeader(joinKey);
            }
            catch (IkatsException e) {
                throw new IkatsOperatorException("Join column '" + joinKey + "' not found in table '" + firstTable.getName() + "'. Additional info: " + e.getMessage(), e);
            }

            if (joinIndexOnFirstTable == -1) {
                throw new IkatsOperatorException("Join column '" + joinKey + "' not found in table '" + firstTable.getName());
            }
        }

        // -- Verify that the join column is in the second, if found :
        // - store the index
        // - point the to the column values
        int joinIndexInSecondTable = 0; // default: use the first column for the join
        List<String> columnValues = null;

        // -- find join in the table and get the index of the row to merge
        if (joinKey != null) {
            // case where we have a join key -> (try to) find the corresponding
            // column in the current table
            try {
                joinIndexInSecondTable = secondTable.getIndexColumnHeader(joinKey);
            }
            catch (IkatsException e) {
                // Exception is synonym of not found
                // INNER JOIN could not be realized
                throw new IkatsOperatorException("Join column not found in the second table", e);
            }

            if (joinIndexInSecondTable == -1) {
                throw new IkatsOperatorException("Join column '" + joinKey + "' not found in table '" + secondTable.getName());
            }
        }

        try {
            columnValues = secondTable.getColumn(joinIndexInSecondTable, String.class);
        }
        catch (IkatsException | ResourceNotFoundException e) {
            logger.error("Can't get the column data at index " + joinIndexInSecondTable + " for table " + secondTable.getName(), e);
            // Do nothing here because joinFound is already false
        }

        // -- Initialize the result/merged table
        boolean withColHeaders = firstTable.isHandlingColumnsHeader() | secondTable.isHandlingColumnsHeader();
        boolean withRowHeaders = firstTable.isHandlingRowsHeader() | secondTable.isHandlingRowsHeader();

        Table resultTable = tableManager.initEmptyTable(withColHeaders, withRowHeaders);
        resultTable.setName(outputTableName);
        resultTable.enableLinks(withColHeaders, new DataLink(), withRowHeaders, new DataLink(), true,
                new DataLink());

        // -- Loop over the values in the join column of the first table to find matching keys in the second
        int firstRow = firstTable.isHandlingColumnsHeader() ? 1 : 0;
        int rowCount = firstTable.getRowCount(firstTable.isHandlingColumnsHeader());
        for (int i = firstRow; i < rowCount; i++) {

            // -- Get the join value of the row in the first table
            List<TableElement> firstTableRowData = new ArrayList<>();
            // First : try to get the row with link
            try {
                if (firstTable.isHandlingRowsHeader()) {
                    TableElement headerLine = new TableElement(firstTable.getRowsHeader().getItems(String.class).get(i), null);
                    firstTableRowData.add(headerLine);
                }
                firstTableRowData.addAll(firstTable.getRow(i, TableElement.class));
            }
            catch (IkatsException | ResourceNotFoundException e) {
                logger.error("Can't get the row at index " + i + " for table " + firstTable.getName());
                throw new IkatsOperatorException("Can't get the row at index " + i + " for table " + firstTable.getName(), e);
            }
            String joinValue = firstTableRowData.get(joinIndexOnFirstTable).data.toString();

            // -- Find the join value in the join column and get the row index of the second table
            int rowIndexForMerge = -1;
            boolean joinFound = false;

            if (columnValues != null) {
                for (int k = 0; k < columnValues.size(); k++) {
                    if (joinValue.equals(columnValues.get(k))) {
                        if (secondTable.isHandlingColumnsHeader()) {
                            rowIndexForMerge = k + 1;
                        }
                        else {
                            rowIndexForMerge = k;
                        }
                        joinFound = true;
                        break;
                    }
                }
            }

            if (!joinFound) {
                // INNER JOIN could not be realized -> no matching value for that row
                continue;
            }
            // ELSE -> we could merge

            // Append the row values to the firstTableRowData
            try {
                List<TableElement> secondTableRowData = new ArrayList<>();
                if (secondTable.isHandlingRowsHeader()) {
                    TableElement headerLine = new TableElement(secondTable.getRowsHeader().getItems(String.class).get(rowIndexForMerge), null);
                    secondTableRowData.add(headerLine);
                }
                // Get the row
                secondTableRowData.addAll(secondTable.getRow(rowIndexForMerge, TableElement.class));
                // Loop over the row values to add them to the firstTable
                for (int j = 0; j < secondTableRowData.size(); j++) {
                    if (j == joinIndexInSecondTable) {
                        // Skip the join value
                        continue;
                    }
                    firstTableRowData.add(secondTableRowData.get(j));
                }

                // Finally append the new row
                resultTable.appendRow(firstTableRowData);
            }
            catch (IkatsException | ResourceNotFoundException e) {
                logger.error("Can't get the row at index " + rowIndexForMerge + " for table " + secondTable.getName());
                throw new IkatsOperatorException("Can't get the row at index " + rowIndexForMerge + " for table " + secondTable.getName(), e);
            }

        }

        // -- Set the result table columns header from the first and second table
            // Put only the headers if we got some data in the table
        if (resultTable.getRowCount(false) > 0 && withColHeaders) {
            reportTableColumnsHeader(firstTable, resultTable, -1);
            reportTableColumnsHeader(secondTable, resultTable, joinIndexInSecondTable);
        }

        resultTable = extractColAsRowHeader(resultTable, joinIndexOnFirstTable);


        return resultTable;
    }

    /**
     * Move a column from a Table to a "row Header" column and return the new Table
     *
     * @param table    Table to change
     * @param colIndex index of the column (in data) to set as row Header
     * @return the converted table
     * @throws IkatsOperatorException
     */
    private Table extractColAsRowHeader(Table table, int colIndex) throws IkatsOperatorException {

        try {
            if (table.isHandlingRowsHeader()) {
                // Extract column
                int firstCol = table.isHandlingRowsHeader() ? 1 : 0;
                List<TableElement> headerValues = table.getColumn(colIndex + firstCol, TableElement.class);

                // Create Headers
                table.getRowsHeader().addItem(null);
                table.getRowsHeader().addItems(headerValues.toArray());

                // Remove extracted column from content
                int rowCount = table.getRowCount(false);
                for (int i = 0; i < rowCount; i++) {
                    table.getTableInfo().content.getRowData(i).remove(colIndex);
                    table.getTableInfo().content.links.get(i).remove(colIndex);
                }

                // Remove extracted column from column headers
                table.getTableInfo().headers.col.data.add(0, table.getTableInfo().headers.col.data.remove(colIndex));
            }

            return table;

        } catch (IndexOutOfBoundsException | IkatsException | ResourceNotFoundException e) {
            throw new IkatsOperatorException("Could not set the column " + colIndex + " as a row header for the result table.", e);
        }

    }

    /**
     * Fill the columns header for the result table
     *
     * @param fromTable       the table from where header should be copied
     * @param resultTable     the result table where to fill the header
     * @param skipColumnIndex index of the column to skip (case of the join column), put -1 for not skipping any column
     * @throws IkatsOperatorException In case the operation could not complete
     */
    private void reportTableColumnsHeader(Table fromTable, Table resultTable, int skipColumnIndex) throws IkatsOperatorException {

        Header resultHeader = resultTable.getColumnsHeader();
        int numberOfColumnsHeaders = fromTable.getColumnCount(true);

        if (fromTable.isHandlingColumnsHeader()) {
            // Report columns header
            List<TableElement> colHeaderElements = null;

            try {
                // Try to get header with links
                colHeaderElements = fromTable.getColumnsHeader().getDataWithLink();
            } catch (IkatsException e1) {
                // Else manage header with no links
                colHeaderElements = new ArrayList<TableElement>();
                try {
                    // An a new element to the headers
                    for (String stringHeader : fromTable.getColumnsHeader().getItems()) {
                        colHeaderElements.add(new TableElement(stringHeader, null));
                    }
                } catch (IkatsException e) {
                    throw new IkatsOperatorException("Could not get columns header items from table " + fromTable.getName(), e);
                }
            }

            for (int i = 0; i < numberOfColumnsHeaders; i++) {
                if (i == skipColumnIndex) {
                    // Skip that column header
                    continue;
                }

                try {
                    TableElement headerElement = colHeaderElements.get(i);
                    resultHeader.addItem(headerElement.data, headerElement.link);
                } catch (IkatsException e) {
                    logger.error("Should not be raised", e);
                    throw new IkatsOperatorException("Could not merge", e);
                }
            }
        } else {
            // Or fill with empty header if the first table do not have headers
            if (skipColumnIndex != -1) {
                numberOfColumnsHeaders -= 1;
            }

            for (int i = 0; i < numberOfColumnsHeaders; i++) {
                try {
                    resultHeader.addItems((Object) null);
                    // cast trick to avoid compiler warning
                } catch (IkatsException e) {
                    logger.error("Should not be raised", e);
                    throw new IkatsOperatorException("Could not merge", e);
                }
            }
        }

        // Finally, reset the result table columns header with the merged columns headers
        resultTable.setColumnsHeader(resultHeader);
    }

}

