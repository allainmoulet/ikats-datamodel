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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
 * @author Pierre BONHOURE <pierre.bonhoure@c-s.fr>
 * 
 */

package fr.cs.ikats.temporaldata.business;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import fr.cs.ikats.lang.NaturalOrderComparator;
import fr.cs.ikats.table.TableDAO;
import fr.cs.ikats.table.TableEntity;
import fr.cs.ikats.table.TableEntitySummary;
import fr.cs.ikats.temporaldata.business.TableInfo.Header;
import fr.cs.ikats.temporaldata.business.TableInfo.TableContent;
import fr.cs.ikats.temporaldata.business.TableInfo.TableDesc;
import fr.cs.ikats.temporaldata.business.TableInfo.TableHeaders;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.IkatsJsonException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;

/**
 * The manager is grouping services on the Table objects
 * <ul>
 * <li>JSON persistence services</li>
 * <li>database persistence services</li>
 * <li>data selection services</li>
 * <li>data modification services</li>
 * </ul>
 */
public class TableManager {

    static private final Logger LOGGER = Logger.getLogger(TableManager.class);

    /**
     * Pattern used in the consistency check of Table name: [a-zA-Z0-9_-]+
     */
    public static final Pattern TABLE_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9_-]+");

    /**
     * String encoding inconsistency error raised by checkConsistency() method
     */
    static final String MSG_INCONSISTENCY_IN_TABLE = "Inconsistency error''{0}'' in table=''{1}'' near ''{2}'' ";

    /**
     * jsonObjectMapper manages JSON persistence of Table
     */
    private ObjectMapper jsonObjectMapper;

    /**
     * DAO object to access the Workflow storage
     */
    public TableDAO dao;

    /**
     * Default constructor for default configuration of jsonObjectMapper.
     */
    public TableManager() {
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        jsonObjectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, false);
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonObjectMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        jsonObjectMapper.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false);

        // In case of null value for an object attribute: do not serialize
        // associated json property.
        jsonObjectMapper.setSerializationInclusion(Include.NON_NULL);

        dao = new TableDAO();
    }

    /**
     * Dao specific class to store all datalinks of functional type table
     */
    // Review#160900 Je suggère de renommer en DataLinksMatrix cela représente un peu mieux le type de données que gère cet élément.
    // Review#160900 Et pour commentaire en passant, car trop tard, dans le même principe pour garder une cohérence avec les valeurs, 
    // Review#160900    il aurait été préférable de gérer une matrice incluant si nécéssaire les row et col headers...
    private static class DataLink implements Serializable {

        // Review#160900 Permet de gérer des expections si on change le format de l'objet... 
        /** Set version of the object for serialization purposes */
        private static final long serialVersionUID = 1L;
        
        private TableInfo.DataLink cellsDefaultDatalink;
        private List<List<TableInfo.DataLink>> cellsDatalink;

        private TableInfo.DataLink rowHeaderDefaultDatalink;
        private List<TableInfo.DataLink> rowHeaderDatalink;

        private TableInfo.DataLink columnHeaderDefaultDatalink;
        private List<TableInfo.DataLink> columnHeaderDatalink;

        private DataLink() {
            super();
        }

        public TableInfo.DataLink getCellsDefaultDatalink() {
            return cellsDefaultDatalink;
        }

        public void setCellsDefaultDatalink(TableInfo.DataLink cellsDefaultDatalink) {
            this.cellsDefaultDatalink = cellsDefaultDatalink;
        }

        public List<List<TableInfo.DataLink>> getCellsDatalink() {
            return cellsDatalink;
        }

        public void setCellsDatalink(List<List<TableInfo.DataLink>> cellsDatalink) {
            this.cellsDatalink = cellsDatalink;
        }

        public TableInfo.DataLink getRowHeaderDefaultDatalink() {
            return rowHeaderDefaultDatalink;
        }

        public void setRowHeaderDefaultDatalink(TableInfo.DataLink rowHeaderDefaultDatalink) {
            this.rowHeaderDefaultDatalink = rowHeaderDefaultDatalink;
        }

        public List<TableInfo.DataLink> getRowHeaderDatalink() {
            return rowHeaderDatalink;
        }

        public void setRowHeaderDatalink(List<TableInfo.DataLink> rowHeaderDatalink) {
            this.rowHeaderDatalink = rowHeaderDatalink;
        }

        public TableInfo.DataLink getColumnHeaderDefaultDatalink() {
            return columnHeaderDefaultDatalink;
        }

        public void setColumnHeaderDefaultDatalink(TableInfo.DataLink columnHeaderDefaultDatalink) {
            this.columnHeaderDefaultDatalink = columnHeaderDefaultDatalink;
        }

        public List<TableInfo.DataLink> getColumnHeaderDatalink() {
            return columnHeaderDatalink;
        }

        public void setColumnHeaderDatalink(List<TableInfo.DataLink> columnHeaderDatalink) {
            this.columnHeaderDatalink = columnHeaderDatalink;
        }

    }

    /**
     * Loads a Table object from the json plain text, using configuration from this.jsonObjectMapper.
     *
     * @param jsonContent: json plain text value.
     * @return the loaded Table
     * @throws IkatsJsonException in case of parsing error.
     */
    public TableInfo loadFromJson(String jsonContent) throws IkatsJsonException {
        try {
            return jsonObjectMapper.readValue(jsonContent, TableInfo.class);
        } catch (Exception e) {
            throw new IkatsJsonException("Failed to load Table business resource from the json content", e);
        }
    }

    /**
     * Serializes the TableInfo into equivalent JSON String, using internal configuration of this.jsonObjectMapper.
     *
     * @param tableInfo the object serialized
     * @return the JSON content written by serialization of tableInfo.
     * @throws IkatsJsonException JSON serialization failed.
     */
    public String serializeToJson(TableInfo tableInfo) throws IkatsJsonException {
        try {
            return jsonObjectMapper.writeValueAsString(tableInfo);
        } catch (Exception e) {
            throw new IkatsJsonException("Failed to serialize Table business resource to the json content", e);
        }
    }

    /**
     * Creates and initializes the structure of an empty Table,
     * <ul>
     * <li>with columns header enabled when parameter withColumnsHeader is true ,</li>
     * <li>with rows header enabled when parameter withColumnsHeader is true ,</li>
     * </ul>
     * This Table is initialized without links managed: see how to configure links management with enablesLinks()
     * method.
     *
     * @return created Table, ready to be completed.
     */
    public Table initEmptyTable(boolean withColumnsHeader, boolean withRowsHeader) {
        TableInfo tableJson = new TableInfo();

        tableJson.table_desc = new TableDesc();
        tableJson.headers = new TableHeaders();

        if (withRowsHeader) {
            tableJson.headers.row = new Header();
            tableJson.headers.row.data = new ArrayList<>();
        }
        if (withColumnsHeader) {
            tableJson.headers.col = new Header();
            tableJson.headers.col.data = new ArrayList<>();
        }

        tableJson.content = new TableContent();
        tableJson.content.cells = new ArrayList<>();

        return new Table(tableJson);
    }

    /**
     * Creates a Table from the JSON content
     *
     * @param tableJson the plain text encoding the JSON
     * @return the Table associated to tableJson
     * @throws IkatsJsonException
     */
    public Table initTable(String tableJson) throws IkatsJsonException {
        TableInfo tableInfo = loadFromJson(tableJson);
        return initTable(tableInfo, false);
    }

    /**
     * Initializes a table, with defined columnHeaders, without links, with rows header enabled by parameter
     * withRowHeader.
     *
     * @param columnHeaders  list of columns header values provided as String
     * @param withRowHeader: true activates the row header management. But the rows header content is set later.
     * @return initialized Table: ready to use appendRow() for example.
     * @throws IkatsException initialization error
     */
    public Table initTable(List<String> columnHeaders, boolean withRowHeader) throws IkatsException {
        Table csvLikeTableH = initTable(new TableInfo(), false);

        // 1: Init Column header
        csvLikeTableH.initColumnsHeader(true, null, new ArrayList<Object>(columnHeaders), null);

        // 2: optional Init Row Header
        if (withRowHeader)
            csvLikeTableH.initRowsHeader(false, null, new ArrayList<>(), null);

        // 3: Init Content
        csvLikeTableH.initContent(false, null);

        return csvLikeTableH;
    }

    /**
     * Initializes the Table business resource from the TableInfo JSON-mapping resource.
     *
     * @param tableInfo     the JSON-mapping resource.
     * @param copyTableInfo true demands that returned Table manages a copy of tableInfo.
     * @return initialized Table, wrapping the (copy of) tableInfo
     */
    public Table initTable(TableInfo tableInfo, boolean copyTableInfo) {
        if (copyTableInfo) {
            return new Table(new TableInfo(tableInfo));
        } else {
            return new Table(tableInfo);
        }
    }


    @SuppressWarnings("unchecked")
    private TableInfo tableEntityToTableInfo(TableEntity table) throws ResourceNotFoundException, IkatsException {

        TableInfo destTable = new TableInfo();
        TableDesc destTableDesc = new TableDesc();
        TableHeaders destTableHeaders = new TableHeaders();
        destTableHeaders.col = new Header();
        TableContent destTableContent = new TableContent();
        destTableContent.cells = new ArrayList<>();
        destTableContent.default_links = new TableInfo.DataLink();
        destTableContent.links = new ArrayList<>();

        // process general attributes of table object
        destTableDesc.desc = table.getDescription();
        destTableDesc.name = table.getName();

        // process table raw data
        List<List<Object>> rawData;
        ObjectInputStream ois;
        try {
            ois = new ObjectInputStream(new ByteArrayInputStream(table.getRawValues()));
            rawData = (List<List<Object>>) ois.readObject();
            if (table.hasColHeader()) {
                destTableHeaders.col.data = rawData.get(0);
            }
            if (table.hasRowHeader()) {
                // Review#160900 enlever la balise TODO, si je comprend bien. Le commentaire est valide pour le code, il me semble.
                // TODO fill right first value of row header
                destTableHeaders.row = new Header();
                destTableHeaders.row.data = new ArrayList<>();
                destTableHeaders.row.data.add(null);
            }
            for (int i = 1; i < rawData.size(); i++) {
                if (table.hasRowHeader()) {
                    destTableHeaders.row.data.add(rawData.get(i).get(0));
                    destTableContent.cells.add(rawData.get(i).subList(1, rawData.get(i).size()));
                } else {
                    destTableContent.cells.add(rawData.get(i));
                }
            }

        } catch (ClassNotFoundException | IOException e) {
            throw new IkatsException("Error raised during table deserialization of raw values");
        }

        // process table raw data links
        DataLink rawDataLinks;
        try {
            ois = new ObjectInputStream(new ByteArrayInputStream(table.getRawDataLinks()));
            rawDataLinks = (DataLink) ois.readObject();
            destTableContent.default_links = rawDataLinks.getCellsDefaultDatalink();
            destTableContent.links = rawDataLinks.getCellsDatalink();

            if (table.hasRowHeader()) {
                destTableHeaders.row.default_links = rawDataLinks.getRowHeaderDefaultDatalink();
                destTableHeaders.row.links = rawDataLinks.getRowHeaderDatalink();
            }

            if (table.hasColHeader()) {
                destTableHeaders.col.default_links = rawDataLinks.getColumnHeaderDefaultDatalink();
                destTableHeaders.col.links = rawDataLinks.getColumnHeaderDatalink();
            }

        } catch (ClassNotFoundException | IOException e) {
            throw new IkatsException("Error raised during table deserialization of raw datalinks");
        }

        destTable.content = destTableContent;
        destTable.headers = destTableHeaders;
        destTable.table_desc = destTableDesc;

        return destTable;
    }

    @SuppressWarnings("unchecked")
    private TableEntity tableInfoToTableEntity(TableInfo tableIn) throws IkatsException {

        TableEntity destTable = new TableEntity();
        Table table = new Table(tableIn);

        // process general attributes of table object
        destTable.setDescription(table.getDescription());
        destTable.setName(table.getName());
        destTable.setCreated(new Date());
        destTable.setColHeader(table.isHandlingColumnsHeader());
        destTable.setRowHeader(table.isHandlingRowsHeader());


        // process table raw data
        // first concatenate table data content
        List<Object> tableFullContent = new ArrayList<>();
        if (table.isHandlingColumnsHeader()) {
            tableFullContent.add(table.getColumnsHeader().data);
        }
        for (int i = 0; i < table.getContentData().size(); i++) {
            List<Object> tempRowData = new ArrayList<>();
            if (table.isHandlingRowsHeader()) {
                tempRowData.add(table.getRowsHeader().data.get(i + 1));
            }
            tempRowData.addAll(table.getContentData().get(i));
            tableFullContent.add(tempRowData);
        }

        ObjectOutputStream oos;
        ByteArrayOutputStream bos;
        try {
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(tableFullContent);
            oos.flush();
            destTable.setRawValues(bos.toByteArray());
        } catch (IOException e) {
            throw new IkatsException("Error raised during table serialization of raw values");
        }


        // process table raw data links
        DataLink dataLinks = new DataLink();
        if (table.isHandlingColumnsHeader()) {
            dataLinks.setColumnHeaderDefaultDatalink(table.getColumnsHeader().default_links);
            dataLinks.setColumnHeaderDatalink(table.getColumnsHeader().links);
        }
        if (table.isHandlingRowsHeader()) {
            dataLinks.setRowHeaderDefaultDatalink(table.getRowsHeader().default_links);
            dataLinks.setRowHeaderDatalink(table.getRowsHeader().links);
        }
        dataLinks.setCellsDefaultDatalink(table.getContent().default_links);
        dataLinks.setCellsDatalink(table.getContent().links);

        try {
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(dataLinks);
            oos.flush();
            destTable.setRawDataLinks(bos.toByteArray());

        } catch (IOException e) {
            throw new IkatsException("Error raised during table serialization of raw datalinks");
        }

        return destTable;
    }

    /**
     * Gets the JSON resource TableInfo from process data database.
     *
     * @param tableName the name of the table is its unique identifier
     * @return read resource TableInfo.
     * @throws IkatsJsonException        failed to read consistent JSON format into TableInfo structure.
     * @throws IkatsDaoException         unexpected DAO error, from Hibernate, reading the database
     * @throws ResourceNotFoundException the table name tableName is not matched in the database.
     */
    public TableInfo readFromDatabase(String tableName)
            throws IkatsDaoException, IkatsDaoMissingRessource, IkatsException, ResourceNotFoundException {

        TableEntity dataTable = dao.getByName(tableName);

        // Convert to Table type
        TableInfo table = tableEntityToTableInfo(dataTable);

        LOGGER.trace("Table retrieved from db OK : name=" + tableName);
        return table;

    }


    /**
     * Gets the JSON resource TableInfo from process data database.
     *
     * @return list of ProcessData. null returned only in case of server error.
     */
    public List<TableEntitySummary> listTables() throws IkatsDaoException {
        return dao.listAll();
    }

    /**
     * Creates a new Table in database:
     * <ul>
     * <li>checks the table+name consistency</li>
     * <li>saves the table json content in database with its key identifier tableName, and tableToStore.getTableInfo()
     * </li>
     * </ul>
     *
     * @param table the Table wrapping the TableInfo required to write the content into the database.
     * @return ID of created processData storing the table.
     * @throws IkatsException            inconsistency error detected in the tableToStore
     * @throws IkatsJsonException        error encoding the JSON content
     * @throws IkatsDaoException         error checking if resource already exists
     * @throws IkatsDaoConflictException error when a resource with processId=tableName exists
     * @throws InvalidValueException     consistency error found in the name of the table: see TABLE_NAME_PATTERN
     */
    public Integer createInDatabase(TableInfo table) throws InvalidValueException, IkatsException, IkatsDaoConflictException {

        String tableName = table.table_desc.name;

        // Validate the name consistency
        validateTableName(tableName, "Create Table in database");

        TableEntity tableToStore = tableInfoToTableEntity(table);

        Integer rid = dao.persist(tableToStore);
        LOGGER.trace("Table stored Ok in db: " + tableName + " with rid: " + rid);

        // and now updates the identifier name for the software part
        tableToStore.setName(tableName);

        return rid;
    }

    /**
     * Deletes the table from the database.
     *
     * @param tableName identifier of the Table is its name
     * @throws IkatsDaoException if error occurs in database
     */
    public void deleteFromDatabase(String tableName) throws IkatsDaoException, ResourceNotFoundException {
        // The name of the table is in the processId column of table processData
        // => so, we can use directly the removeProcessData(processId) service.

        // No exception raised by this remove
        int idTable = dao.getByName(tableName).getId();
        dao.removeById(idTable);
    }

    /**
     * Basically checks if tableName exists as processId in the database.
     * <p>
     * Note: presently the name of the table is saved in Postgres table process_data in column processId.
     *
     * @param tableName the name is the identifier of the Table.
     * @return true if table name already exists in database.
     * @throws IkatsDaoException unexpected Hibernate error.
     */
    public boolean existsInDatabase(String tableName) throws IkatsDaoException {
        return !dao.findByName(tableName, true).isEmpty();
    }

    /**
     * Gets a table column from a table, reading the table in database.
     * <p>
     * <ul>
     * <li>calls readFromDatabase(tableName)</li>
     * <li>and then getColumnFromTable(table, columnName)</li>
     * </ul>
     * <p>
     * Warning: do not repeat this operation if you have several columns to read from the same table, this will clearly
     * be inefficient! Instead, in that case, use readFromDatabase(), then initTable(TableInfo) and finally use services
     * on Table business resource.
     *
     * @param tableName  the name of the Table resource is also its unique identifier.
     * @param columnName column header value identifying the selection.
     * @return the column as list of values, excluding the header value.
     * @throws IkatsException            unexpected error occurred. Example: internal ClassCastException when List<T> does not fit the actual
     *                                   data.
     * @throws IkatsDaoException         unexpected hibernate exception reading the table from database.
     * @throws ResourceNotFoundException either the resource Table named tableName is not found in the database, or the column is not found in
     *                                   the table.
     */
    public <T> List<T> getColumnFromTable(String tableName, String columnName)
            throws IkatsException, IkatsDaoException, ResourceNotFoundException {

        TableInfo table = readFromDatabase(tableName);

        Table tableH = initTable(table, false);

        List<T> column = tableH.getColumn(columnName);

        LOGGER.trace("Column " + columnName + " retrieved from table : " + tableName);

        return column;
    }

    /**
     * Validate that the table name is well encoded.
     *
     * @param name    name of the table, also its unique identifier
     * @param context information to provide in order to improve error log.
     * @throws InvalidValueException
     */
    public final void validateTableName(String name, String context) throws InvalidValueException {
        String nameStr = name == null ? "null" : name;
        if ((name == null) || !TableManager.TABLE_NAME_PATTERN.matcher(name).matches()) {
            String msg = context + ": invalid name of table resource: " + nameStr;
            LOGGER.error(msg);
            throw new InvalidValueException("Table", "name", TableManager.TABLE_NAME_PATTERN.pattern(), nameStr, null);
        }
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
     * @throws
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
    public List<Table> randomSplitTable(Table table, double repartitionRate) throws ResourceNotFoundException, IkatsException {

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
        Table table1 = initEmptyTable(withColHeaders, withRowHeaders);
        Table table2 = initEmptyTable(withColHeaders, withRowHeaders);
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
    public List<Table> trainTestSplitTable(Table table, String targetColumnName, double repartitionRate) throws
            ResourceNotFoundException, IkatsException {

        boolean withColHeaders = table.isHandlingColumnsHeader();
        boolean withRowHeaders = table.isHandlingRowsHeader();

        // Sort table by column 'target'
        table.sortRowsByColumnValues(targetColumnName, false);

        // Extract classes column
        List<Object> classColumnContent = table.getColumn(targetColumnName);

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
        Table table1 = initEmptyTable(withColHeaders, withRowHeaders);
        Table table2 = initEmptyTable(withColHeaders, withRowHeaders);
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
     * Internal use: convert a collection of Object data into a collection of type T. For each item from originalList:
     * if type T is String, item is replaced by item.toString() in the new collection else: objects are casted to the
     * expected type T, using castingClass.
     *
     * @param originalList the original list of objects
     * @param castingClass the class specifying the type of T. Technically required, to call the cast operation. Examples:
     *                     Object.class will configure permissive cast, contrary to Integer.class, which requires that every item
     *                     is an Integer or null. See method description above for possible option String.class.
     * @return the new converted list from the originalList
     * @throws IkatsException error wrapping the ClassCastException
     */
    static <T> List<T> convertList(List<Object> originalList, Class<T> castingClass) throws IkatsException {
        int posCastItem = 0;
        try {
            boolean isConvertingToString = castingClass == String.class;
            List<T> result = new ArrayList<>();
            // iterate and cast the value to T ...
            for (Object dataItem : originalList) {

                if (isConvertingToString) {
                    String strConverted = dataItem == null ? null : dataItem.toString();
                    result.add(castingClass.cast(strConverted));
                } else {
                    result.add(castingClass.cast(dataItem));
                }

                posCastItem++;
            }
            return result;
        } catch (ClassCastException e) {
            throw new IkatsException("TableManager::convertList() failed: on item at position=" + posCastItem, e);
        }

    }

    /**
     * Internal use only: definition of Integer comparator driven by sorting String values. Purpose of returned
     * Comparator is to sort some table indexes (for instance rows indexes) according to a specific criterion, i.e.
     * associated values from mapIndexToSortingValue (for instance values from one specific column).
     *
     * @param mapIndexToSortingValue the map associating each index to its sorting value.
     * @param reverse                true for descending order.
     * @return this comparator
     */
    static Comparator<Integer> getIndexComparator(Map<Integer, String> mapIndexToSortingValue, boolean reverse) {
        // magic comparator: reorder the indexes according to the order of
        // sorting values
        Comparator internalComparator = new NaturalOrderComparator();

        return new Comparator<Integer>() {

            @Override
            public int compare(Integer index, Integer anotherIndex) {
                String sortingVal = mapIndexToSortingValue.get(index);
                String anotherSortingVal = mapIndexToSortingValue.get(anotherIndex);
                int res = internalComparator.compare(sortingVal, anotherSortingVal);
                if (reverse)
                    res = -res;
                return res;
            }
        };
    }
}

