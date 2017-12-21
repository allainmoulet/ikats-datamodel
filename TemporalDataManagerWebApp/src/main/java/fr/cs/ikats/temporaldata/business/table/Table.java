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
 * @author Mathieu BERAUD <mathieu.beraud@c-s.fr>
 * @author Maxime PERELMUTER <maxime.perelmuter@c-s.fr>
 * @author Pierre BONHOURE <pierre.bonhoure@c-s.fr>
 */

package fr.cs.ikats.temporaldata.business.table;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fr.cs.ikats.temporaldata.business.table.TableInfo.DataLink;
import fr.cs.ikats.temporaldata.business.table.TableInfo.Header;
import fr.cs.ikats.temporaldata.business.table.TableInfo.TableContent;
import fr.cs.ikats.temporaldata.business.table.TableInfo.TableHeaders;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;

/**
 * The Table class is the business resource for the 'table' IKATS functional type, it is a wrapper of TableInfo. The
 * Table class provides end-user services in java world.
 * <p/>
 * Note.1: for end-user, the TableManager is the entry point in order to build the Table. This is why the construtor
 * visibility is default, not public.
 * <p/>
 * Note.2: the difference with TableInfo: TableInfo manages the JSON persistence, and can be used by the REST web
 * services, involving JSON media type, grouped in the class TableResource. You can get the TableInfo managed thanks to
 * the getter getTableInfo().
 */
public class Table {

    private final TableInfo tableInfo;

    /**
     * Creates the business Table handling TableInfo. Internally used by TableManager.
     *
     * @param handledTable either from an existing TableInfo, for example, the Table loaded from a json content or from new
     *                     TableInfo(), in order to initialize it.
     */
    Table(TableInfo handledTable) {
        super();
        this.tableInfo = handledTable;
    }

    /**
     * Computes the String representation of this Table: a short description based upon TableDesc section. {@inheritDoc}
     */
    public String toString() {
        String nameStr = getName();
        nameStr = nameStr == null ? "null" : nameStr;

        String titleStr = getTitle();
        titleStr = titleStr == null ? "null" : titleStr;

        String descStr = getDescription();
        descStr = descStr == null ? "null" : descStr;

        return "Table name=" + nameStr + " title=" + titleStr + " desc=" + descStr;
    }

    /**
     * @return true only if this Table is managing a columns header
     */
    public boolean isHandlingColumnsHeader() {
        boolean isManaged = false;
        if ((tableInfo.headers != null) && (tableInfo.headers.col != null))
            isManaged = true;
        return isManaged;
    }

    /**
     * @return true only if this Table is managing a rows header
     */
    public boolean isHandlingRowsHeader() {
        boolean isManaged = false;
        if ((tableInfo.headers != null) && (tableInfo.headers.row != null))
            isManaged = true;
        return isManaged;
    }

    /**
     * Gets the TableInfo object handled by Table: see TableInfo and Table documentation to know more about respective
     * roles of these two classes.
     *
     * @return the TableInfo
     */
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    /**
     * Gets description
     *
     * @return the title of the Table. null when undefined
     */
    public String getDescription() {
        if (tableInfo.table_desc != null) {
            return tableInfo.table_desc.desc;
        } else
            return null;
    }

    /**
     * Gets title
     *
     * @return the title of the Table. null when undefined
     */
    public String getTitle() {
        if (tableInfo.table_desc != null) {
            return tableInfo.table_desc.title;
        } else
            return null;
    }

    /**
     * Gets name
     *
     * @return the name of the Table. null when undefined
     */
    public String getName() {
        if (tableInfo.table_desc != null) {
            return tableInfo.table_desc.name;
        } else
            return null;
    }

    /**
     * Counts the number of rows in the table.
     *
     * @param countColumnsHeaderAsrow set to True if the columns header line shall be counted, false otherwise
     * @return the number of rows
     */
    public int getRowCount(boolean countColumnsHeaderAsrow) {
        // Count rows in Content part:
        List<List<Object>> contentData = getContentData();
        int nbRows = contentData != null ? contentData.size() : 0;

        // If required: add 1 for the column header
        if (countColumnsHeaderAsrow && isHandlingColumnsHeader())
            nbRows++;
        return nbRows;
    }

    /**
     * Counts the number of columns in the table.
     *
     * @param countRowsHeaderAsColumn set to True if the rows header column is counted as a column itself -if managed-
     * @return the number of columns based upon contentData.get(0) and countRowsHeaderAsColumn
     */
    public int getColumnCount(boolean countRowsHeaderAsColumn) {
        int countCol = 0;
        // Count columns in Content part:
        List<List<Object>> contentData = getContentData();
        if ((contentData != null) && !contentData.isEmpty()) {
            countCol = contentData.get(0).size();
        }

        // If required: add 1 for the column header
        if (countRowsHeaderAsColumn && isHandlingRowsHeader())
            countCol++;
        return countCol;
    }

    /**
     * Gets the columns Header, if defined, or null
     *
     * @return the columns Header, if defined, or null
     */
    public Header getColumnsHeader() {
        if (this.tableInfo.headers != null) {
            return this.tableInfo.headers.col;
        }
        return null;
    }

    /**
     * Gets the rows Header, if defined, or null
     *
     * @return the rows Header, if defined, or null
     */
    public TableInfo.Header getRowsHeader() {
        if (this.tableInfo.headers != null) {
            return this.tableInfo.headers.row;
        }
        return null;
    }

    /**
     * Retrieves the header column index matching the value.
     *
     * @param value the value to look for
     * @return index matched by value, if exists, or -1
     * @throws IkatsException when column header is null
     */
    public int getIndexColumnHeader(String value) throws IkatsException {
        return this.getHeaderIndex(this.getColumnsHeader(), value);
    }

    /**
     * Retrieves the header row index matching the value.
     *
     * @param value the value to look for
     * @return index matched by value, if exists, or -1
     * @throws IkatsException when row header is null
     */
    public int getIndexRowHeader(String value) throws IkatsException {
        return this.getHeaderIndex(this.getRowsHeader(), value);
    }

    /**
     * Retrieves the index position of a header name in the header container (theHeader).
     * <p>
     * Note: the value is compared to theHeader.data.get(x).toString(): this enables to manage also header data types
     * different from String.
     *
     * @param theHeader Header is the header items container, where is searched the value.
     * @param value     Name of the searched header item (item: column or row) to get index
     * @return index of header item matching the value. First position is zero. If value is not found -& is returned.
     * @throws IkatsException unexpected error. Examples: null value, or theHeader is null, or theHeader.data is null.
     */
    private int getHeaderIndex(Header theHeader, String value) throws IkatsException {
        if (value == null)
            throw new IkatsException("Unexpected seraching header item: value is null ");

        if (theHeader == null || theHeader.data == null)
            throw new IkatsException("Undefined header: impossible to search the index matching value=" + value);
        int position = 0;

        for (Object headerValue : theHeader.data) {
            if (headerValue != null && headerValue.toString().equals(value)) {
                return position;
            }
            position++;
        }
        return -1;
    }

    /**
     * Gets the column values (as strings) from the Table.
     * <p>
     * Note: this getter ignores the links possibly defined on the column.
     *
     * @param columnName name of the selected column: this criterion is in the column header.
     * @return the content column below selected column header name.
     * @throws ResourceNotFoundException when the column is not found
     * @throws IkatsException            unexpected error occured: for exemple ClassCastException error.
     */
    public List<String> getColumn(String columnName) throws IkatsException, ResourceNotFoundException {

        return getColumn(columnName, String.class);
    }

    /**
     * Gets the column values from the Table.
     *
     * Note: new possibility: in order to read the data+link parts of each cell: pass TableElement.class as
     * castingClass.
     *
     * @param <T>          Type of the castingClass parameter
     * @param columnName   name of the selected column: this criteria is in the column header.
     * @param castingClass the converting class: String.class in order to get toString() on each cell data value (not link);
     *                     TableElement in order to wrapp (data+link) part of each cell, and another class in order to cast the
     *                     cell data value (not link).
     * @return the content column below selected column header name.
     * @throws ResourceNotFoundException when the column is not found
     * @throws IkatsException            unexpected error occured: for exemple ClassCastException error.
     */
    public <T> List<T> getColumn(String columnName, Class<T> castingClass)
            throws IkatsException, ResourceNotFoundException {

        try {

            int matchedIndex = this.getIndexColumnHeader(columnName);
            if (matchedIndex < 0) {
                throw new ResourceNotFoundException("Unmatched getColumn(): in Columns header: no column named "
                        + columnName + " of table " + this.toString());
            }
            return (List<T>) getColumn(matchedIndex, castingClass);
        } catch (IkatsException e) {
            throw new IkatsException("Failed getColumn(" + columnName + ") in table: 1" + this.toString(), e);
        }
    }

    /**
     * Gets the column values from the Table. Note: this getter ignores the links possibly defined on the column. <br/>
     * For end-users: when possible, it is advised to use getColumn(String ...) less confusing than getColumn(int ...).
     *
     * Note: new possibility: in order to read the data+link parts of each cell: pass TableElement.class as
     * castingClass.
     *
     * @param <T>          Type of the castingClass parameter
     * @param index        of selected column. Note: index relative to the global table. If rows header exists: 0 points to rows
     *                     header; otherwise 0 points to first column of this.getContent().
     * @param castingClass the converting class: String.class in order to get toString() on each cell data value (not link);
     *                     TableElement in order to wrapp (data+link) part of each cell, and another class in order to cast the
     *                     cell data value (not link).
     * @return the selected column values. Note the columns header part is not included. And if Rows header is selected:
     * the first rows header element is not included.
     * @throws IkatsException
     * @throws ResourceNotFoundException when the column is not found
     */
    public <T> List<T> getColumn(int index, Class<T> castingClass) throws IkatsException, ResourceNotFoundException {

        try {
            if (index < 0) {
                throw new ResourceNotFoundException(
                        "Unmatched getColumn(): in Columns header: no column at content index=" + index + " of table "
                                + this.toString());
            }
            Header rowsHeader = this.getRowsHeader();
            if (castingClass == TableElement.class) {
                return (List<T>) getColumnWithLinks(index);
            } else {
                List<Object> matchedData;
                if ((rowsHeader != null) && index == 0) {
                    // Read the rows header and ignore its first element
                    matchedData = new ArrayList<>(rowsHeader.getData());
                    matchedData.remove(0);
                } else {
                    // inside the content part: the column is indexed by
                    int contentIndex = (rowsHeader == null) ? index : index - 1;
                    matchedData = this.getContent().getColumnData(contentIndex);
                }

                return TableManager.convertList(matchedData, castingClass);
            }

        } catch (IkatsException typeError) {
            throw new IkatsException("Failed getColumn() in table: cast failed on column at content index=" + index
                    + " in table " + this.toString(), typeError);
        }
    }

    /**
     * Implementation internally used in case of getColumn(..., TableElement.class): gets the Column with links
     *
     * @param index
     * @return
     * @throws IkatsException
     */
    private List<TableElement> getColumnWithLinks(int index) throws IkatsException {

        List<TableElement> matchedData;
        Header rowsHeader = this.getRowsHeader();
        if ((rowsHeader != null) && index == 0) {
            // Read the rows header and ignore its first element
            matchedData = new ArrayList<>(rowsHeader.getDataWithLink());
            matchedData.remove(0);
        } else {
            // inside the content part: the column is indexed by
            int contentIndex = isHandlingRowsHeader() ? index - 1 : index;
            matchedData = this.getContent().getColumnDataWithLink(contentIndex, false);
        }

        return matchedData;
    }

    /**
     * Gets the row values as String from this table, using the toString() on each object instance.
     *
     * Note: this getter ignores the links possibly defined in the row.
     *
     * @param rowName name of the selected row: this criteria is in the row header.
     * @return the content row below selected by the rowName parameter. Note: the selected row does not contain the rows
     * header part.
     * @throws IkatsException            row header is undefined or unexpected error.
     * @throws ResourceNotFoundException not row is selected by rowName.
     */
    public List<String> getRow(String rowName) throws IkatsException, ResourceNotFoundException {

        try {

            int matchedIndex = this.getIndexRowHeader(rowName);
            if (matchedIndex < 0) {
                throw new ResourceNotFoundException(
                        "Unmatched getRow(): in Rows header: no row named " + rowName + " of table " + this.toString());
            }
            return getRow(matchedIndex, String.class);
        } catch (IkatsException e) {
            throw new IkatsException("Failed getRow(" + rowName + ") in table: " + this.toString(), e);
        }
    }

    /**
     * Gets the row values from the Table. Note: this getter ignores the links possibly defined on the row. <br/>
     * For end-users: when possible, it is advised to use getColumn(String), less confusing than getColumn(int).
     *
     * @param rowName name of the row header item selecting the row values.
     * @return the selected row values. Note that the rows header part is not included in the result. And if columns
     * header is selected by index=0: the first columns header element is not included.
     * @throws IkatsException            inconsistency error detected
     * @throws ResourceNotFoundException when the row is not found
     */
    public <T> List<T> getRow(String rowName, Class<T> castingClass) throws IkatsException, ResourceNotFoundException {

        try {

            int matchedIndex = this.getIndexRowHeader(rowName);
            if (matchedIndex < 0) {
                throw new ResourceNotFoundException(
                        "Unmatched getRow(): in Rows header: no row named " + rowName + " of table " + this.toString());
            }
            return getRow(matchedIndex, castingClass);
        } catch (IkatsException e) {
            throw new IkatsException("Failed getRow(" + rowName + ") in table: " + this.toString(), e);
        }
    }

    /**
     * Gets the selected row values from this table. Note: this getter ignores the links possibly defined in the row.
     * <br/>
     * For end-users: when possible, it is advised to use getRow(String), less confusing than getRow(int).
     *
     * @param index        index of selected row. Note: index is relative to the whole table. If column header exists: 0 points
     *                     to columnHeaders; otherwise 0 points to first row of this.getContent().
     * @param castingClass class to which the values are converted
     * @return selected row values. Note the row header part is not included. And if Columns header is selected: first
     * header element is not included.
     * @throws IkatsException            row header is undefined
     * @throws ResourceNotFoundException row is not found
     */
    public <T> List<T> getRow(int index, Class<T> castingClass) throws IkatsException, ResourceNotFoundException {
        try {
            if (index < 0) {
                throw new ResourceNotFoundException("Unmatched getRow(): in Rows header: no row at content index="
                        + index + " of table " + this.toString());
            }

            if (castingClass == TableElement.class) {
                // specifically converting data+links items to TableElement
                return (List<T>) getRowWithLinks(index);
            } else {
                List<Object> matchedData;
                Header columnsHeader = this.getColumnsHeader();
                if ((columnsHeader != null) && index == 0) {
                    // Read the columns header and ignore its first element

                    matchedData = new ArrayList<>(columnsHeader.getData());
                    matchedData.remove(0);

                } else {
                    // inside the content part: the row is indexed by
                    // matchedIndex - 1
                    int contentIndex = (columnsHeader == null) ? index : index - 1;
                    matchedData = this.getContent().getRowData(contentIndex);
                }

                // iterate and convert/cast the data value to T ...
                return TableManager.convertList(matchedData, castingClass);
            }

        } catch (IkatsException typeError) {
            throw new IkatsException(
                    "Failed getRow() in row at content index=" + index + " in table " + this.toString(), typeError);
        }
    }

    /**
     * Implementation internally used in case of getRow(..., TableElement.class): gets the specified Row with links
     *
     * @param index
     * @return list of TableElements, each one grouping data+link by cell.
     * @throws IkatsException
     */
    private List<TableElement> getRowWithLinks(int index) throws IkatsException {

        List<TableElement> matchedData;

        if (isHandlingColumnsHeader() && index == 0) {
            // Read the rows header and ignore its first element
            Header columnsHeader = this.getColumnsHeader();
            matchedData = new ArrayList<>(columnsHeader.getDataWithLink());
            matchedData.remove(0);
        } else {
            // inside the content part: the column is indexed by
            int contentIndex = isHandlingColumnsHeader() ? index - 1 : index;
            matchedData = this.getContent().getRowDataWithLink(contentIndex, false);
        }

        return matchedData;
    }

    /**
     * Gets the specified column values from the content section, this.getContent()
     * <p/>
     * Note1: this getter ignores header information
     * <p/>
     * Note: new possibility: in order to read the data+link parts of each cell: pass TableElement.class as
     * castingClass. Otherwise: the getter will only cast the data content part to castingClass. <br/>
     *
     * @param contentIndex index of selected column. Note: index relative to the content section of this Table, i.e
     *                     this.getContent()
     * @return the selected column values found in this.getContent().
     * @throws IndexOutOfBoundsException unexpected index for this content
     * @throws IkatsException            inconsistency error occurred
     */
    public <T> List<T> getContentColumn(int contentIndex, Class<T> castingClass)
            throws IndexOutOfBoundsException, IkatsException {
        if (castingClass == TableElement.class) {
            return (List<T>) getContent().getColumnDataWithLink(contentIndex, false);
        } else {
            List<Object> col = getContent().getColumnData(contentIndex);
            return TableManager.convertList(col, castingClass);
        }
    }

    /**
     * Gets the specified row from the content section, this.getContent()
     * <p/>
     * Note1: this getter ignores header information
     * <p/>
     * Note: new possibility: in order to read the data+link parts of each cell: pass TableElement.class as
     * castingClass. Otherwise: the getter will only cast the data content part to castingClass. <br/>
     *
     * @param contentIndex index of selected row. Note: index relative to the content section of this Table, i.e
     *                     this.getContent()
     * @return the selected row values found in this.getContent().
     * @throws IndexOutOfBoundsException unexpected index for this content
     * @throws IkatsException            inconsistency error occurred
     */
    public <T> List<T> getContentRow(int contentIndex, Class<T> castingClass)
            throws IndexOutOfBoundsException, IkatsException {
        if (castingClass == TableElement.class) {
            return (List<T>) this.getContent().getRowDataWithLink(contentIndex, false);
        } else {
            List<Object> col = getContent().getRowData(contentIndex);
            return TableManager.convertList(col, castingClass);
        }

    }

    /**
     * Getter of the content part of the table. Beware: content may not be initialized.
     *
     * @return handled content or null
     */
    TableContent getContent() {
        return this.tableInfo.content;
    }

    /**
     * Internal getter of this.tableInfo.content.cells
     *
     * @return this.tableInfo.content.cells or null if undefined
     */
    public List<List<Object>> getContentData() {
        if (this.tableInfo.content == null)
            return null;

        return this.tableInfo.content.cells;
    }


    /**
     * Enables/Disables the headers on this table: this re-initializes the header management on this Table.
     * <p/>
     * Note: only the header data is enabled by this method. Use enableLinks(...) in order to add links to enabled
     * header(s).
     *
     * @param withColumnsHeader true is initializing a new columns header. false is removing the previous columns header -when needed-
     * @param withRowsHeader    true is initializing a new rows header. false is removing the previous rows header -when needed-
     */
    public void enableHeaders(boolean withColumnsHeader, boolean withRowsHeader) {

        if (withColumnsHeader || withRowsHeader) {
            if (this.tableInfo.headers == null)
                this.tableInfo.headers = new TableHeaders();

            if (withRowsHeader) {
                tableInfo.headers.row = new Header();
                tableInfo.headers.row.data = new ArrayList<>();
            }
            if (withColumnsHeader) {
                tableInfo.headers.col = new Header();
                tableInfo.headers.col.data = new ArrayList<>();
            }
        } else {
            this.tableInfo.headers = null;
        }
    }

    /**
     * Initializes the links configuration, when some links are required.
     *
     * @param enabledOnColHeader       True enables the links on the column header
     * @param defaultPropertyColHeader the DataLink providing default values for column links
     * @param enabledOnRowHeader       enables the links on the row header
     * @param defaultPropertyRowHeader the DataLink providing default values for row links
     * @param enabledOnContent         enables the links on the content
     * @param defaultPropertyContent   the DataLink providing default values for content links
     */
    public void enableLinks(boolean enabledOnColHeader, DataLink defaultPropertyColHeader, boolean enabledOnRowHeader,
                            DataLink defaultPropertyRowHeader, boolean enabledOnContent, DataLink defaultPropertyContent) {
        Header columnsHeader = getColumnsHeader();
        if (columnsHeader != null && enabledOnColHeader)
            columnsHeader.enableLinks(defaultPropertyColHeader);
        Header rowsHeader = getRowsHeader();
        if (rowsHeader != null && enabledOnRowHeader)
            rowsHeader.enableLinks(defaultPropertyRowHeader);
        TableContent content = getContent();
        if (content != null && enabledOnContent)
            content.enableLinks(defaultPropertyContent);
    }

    /**
     * Sets the table description
     *
     * @param description
     */
    public void setDescription(String description) {
        if (tableInfo.table_desc == null) {
            tableInfo.table_desc = new TableInfo.TableDesc();
        }
        tableInfo.table_desc.desc = description;

    }

    /**
     * Clone this Table into a new table. {@inheritDoc}
     */
    public Table clone() {
        TableInfo copyOfTableInfo = new TableInfo(this.tableInfo);
        return new Table(copyOfTableInfo);
    }

    /**
     * Sets the title of the table
     *
     * @param title
     */
    public void setTitle(String title) {
        if (tableInfo.table_desc == null) {
            tableInfo.table_desc = new TableInfo.TableDesc();
        }
        tableInfo.table_desc.title = title;
    }

    /**
     * Sets the table name
     *
     * @param name
     */
    public void setName(String name) {
        if (tableInfo.table_desc == null) {
            tableInfo.table_desc = new TableInfo.TableDesc();
        }
        tableInfo.table_desc.name = name;
    }

    /**
     * Sets the rows Header from a defined header, for instance from another Table.
     *
     * @param rowsHeader
     */
    public void setRowsHeader(Header rowsHeader) {
        if (tableInfo.headers == null)
            tableInfo.headers = new TableHeaders();
        tableInfo.headers.row = rowsHeader;
    }

    /**
     * Sets the columns Header from a defined header, for instance from another Table.
     *
     * @param columnsHeader
     */
    public void setColumnsHeader(Header columnsHeader) {
        if (tableInfo.headers == null)
            tableInfo.headers = new TableHeaders();
        tableInfo.headers.col = columnsHeader;
    }

    /**
     * For internal use: Initializes the Header of columns.
     *
     * @param startWithTopLeftCorner true indicates that the defined headerData and the defined headerLinks begin from the top-left corner
     * @param defaultLink            default definition for the links possibly defined by headerLinks. Optional: null if undefined.
     * @param headerData             defines the data part of the header. null when no data managed. Note: null is discouraged for data
     *                               part. Pass an empty list if you want to manage data, without specifiing them with this initialization.
     * @param headerLinks            defines the links part of the header. null when no links managed. Pass an empty List<DataLink> if you
     *                               want to manage links, without specifiing them with this initialization.
     * @return initialized and attached header, which can be completed later using {@link Header#addItem}
     * @throws IkatsException inconsistency error detected
     */
    Header initColumnsHeader(boolean startWithTopLeftCorner, DataLink defaultLink, List<Object> headerData,
                             List<DataLink> headerLinks) throws IkatsException {
        if (defaultLink != null && headerLinks == null) {
            throw new IkatsException(
                    "Inconsistency: default link cannot be defined if the links are not managed (headerLinks == null)");
        }
        if (this.tableInfo.headers == null)
            this.tableInfo.headers = new TableHeaders();

        this.tableInfo.headers.col = Header.createHeader(headerData, headerLinks, defaultLink, startWithTopLeftCorner);
        return this.tableInfo.headers.col;
    }

    /**
     * For internal use: Initializes the rows header.
     *
     * @param startWithTopLeftCorner true indicates that the defined headerData and the defined headerLinks begin from the top-left corner
     * @param defaultLink            default definition for the links possibly defined by headerLinks. Optional: null if undefined.
     * @param headerData             defines the data part of the header. null when no data managed. Note: null is discouraged for data
     *                               part. Pass an empty list if you want to manage data, without specifiing them with this initialization.
     * @param headerLinks            defines the links part of the header. null when no links managed. Pass an empty List<DataLink> if you
     *                               want to manage links, without specifiing them with this initialization.
     * @return initialized and attached header, which can be completed later using {@link Header#addItem}
     * @throws IkatsException inconsistency error detected
     */
    Header initRowsHeader(boolean startWithTopLeftCorner, DataLink defaultLink, List<Object> headerData,
                          List<DataLink> headerLinks) throws IkatsException {
        if (defaultLink != null && headerLinks == null) {
            throw new IkatsException(
                    "Inconsistency: default link cannot be defined if the links are not manages (headerLinks == null)");
        }
        if (this.tableInfo.headers == null)
            this.tableInfo.headers = new TableHeaders();

        this.tableInfo.headers.row = Header.createHeader(headerData, headerLinks, defaultLink, startWithTopLeftCorner);
        return this.tableInfo.headers.row;
    }

    /**
     * Internal use only: initializes an empty TableContent under this.tableInfo.content.
     *
     * @param manageLinks true in order to manage links under this.tableInfo.content
     * @param defaultLink when not null: the DataLink defining the default properties applicable to content links.
     * @return initialized this.tableInfo.content
     * @throws IkatsException inconsistency error during initialization
     */
    TableContent initContent(boolean manageLinks, DataLink defaultLink) throws IkatsException {

        this.tableInfo.content = TableContent.initContent(new ArrayList<List<Object>>(),
                manageLinks ? new ArrayList<List<DataLink>>() : null, defaultLink);

        return this.tableInfo.content;
    }

    /**
     * Sorts the rows under the column header, according to one column.
     * <p>
     * If a row header is managed, it is sorted the same way.
     * <p>
     * Assumed: the column named columnHeaderName ought to have homogeneous class on items.
     *
     * @param columnHeaderName name of the sorting criterion.
     * @param reverse          reverse is true for descending order.
     * @throws IkatsException            inconsistency detected
     * @throws ResourceNotFoundException the sorting colunm is not found
     */
    public void sortRowsByColumnValues(String columnHeaderName, boolean reverse)
            throws IkatsException, ResourceNotFoundException {
        if (!isHandlingColumnsHeader())
            throw new IkatsException(
                    "Bad usage: sortRowsByColumnValues(String) impossible when Table is not handling columns header.");
        int index = getIndexColumnHeader(columnHeaderName);
        sortRowsByColumnValues(index, reverse);
    }

    /**
     * Sorts the rows under the column header, according to one column.
     * <p>
     * If a row header is managed, it is sorted the same way.
     * <p>
     * Assumed: the column named columnHeaderName ought to have homogeneous class on items.
     *
     * @param index index of the column: retrieved from getColumn service.
     * @throws IkatsException            inconsistency detected
     * @throws ResourceNotFoundException the sorting colunm is not found
     */
    public void sortRowsByColumnValues(int index, boolean reverse) throws IkatsException, ResourceNotFoundException {

        // no sort needed if content data is empty !
        if (getContentData() == null)
            return;

        // we will apply natural sort on values converted to String
        List<String> sortingColumn = getColumn(index, String.class);

        // associates original indexes to their sorting value
        //
        Map<Integer, String> mapIndexToSortingValue = new HashMap<Integer, String>();

        // ... and initializes indexes: the list of indexes, before it is
        // sorted differently ...
        List<Integer> indexes = new ArrayList<>();
        int originalIndex = 0;
        for (String sortingValue : sortingColumn) {
            Integer currentIndex = originalIndex;
            String currentVal = sortingValue == null ? "" : sortingValue;
            mapIndexToSortingValue.put(currentIndex, currentVal);
            indexes.add(currentIndex);
            originalIndex++;
        }

        Comparator<Integer> compareRows = TableManager.getIndexComparator(mapIndexToSortingValue, reverse);

        // ... and apply the reordering on the list indexes
        Collections.sort(indexes, compareRows);

        List<List<Object>> theOriginalRows = tableInfo.content.cells;
        List<List<Object>> theReorderedRows = new ArrayList<>();

        List<List<DataLink>> theOriginalRowLinks = tableInfo.content.links;
        List<List<DataLink>> theReorderedRowLinks = theOriginalRowLinks != null ? new ArrayList<>() : null;

        List<Object> theOriginalRowsHeaderData = isHandlingRowsHeader() ? getRowsHeader().data : null;
        List<Object> theReorderedRowsHeaderData = theOriginalRowsHeaderData != null ? new ArrayList<>() : null;

        List<DataLink> theOriginalRowsHeaderLinks = isHandlingRowsHeader() ? getRowsHeader().links : null;
        List<DataLink> theReorderedRowsHeaderLinks = theOriginalRowsHeaderLinks != null ? new ArrayList<>() : null;

        // first element (data or link) of rows header is not sorted if a
        // columns header exists
        // => save this in integer firstHeaderSorted
        int firstHeaderSorted = isHandlingColumnsHeader() ? 1 : 0;

        // inserts fixed elements of rows header (if required)
        if ((theReorderedRowsHeaderLinks != null) && (firstHeaderSorted == 1))
            theReorderedRowsHeaderLinks.add(theOriginalRowsHeaderLinks.get(0));
        if ((theReorderedRowsHeaderData != null) && (firstHeaderSorted == 1))
            theReorderedRowsHeaderData.add(theOriginalRowsHeaderData.get(0));

        // ... and then rebuild each reordered collection ...
        for (Integer reorderedIndex : indexes) {
            theReorderedRows.add(theOriginalRows.get(reorderedIndex));

            if (theReorderedRowLinks != null)
                theReorderedRowLinks.add(theOriginalRowLinks.get(reorderedIndex));

            if (theReorderedRowsHeaderData != null)
                theReorderedRowsHeaderData.add(theOriginalRowsHeaderData.get(reorderedIndex + firstHeaderSorted));

            if (theReorderedRowsHeaderLinks != null)
                theReorderedRowsHeaderLinks.add(theOriginalRowsHeaderLinks.get(reorderedIndex + firstHeaderSorted));
        }

        // ... done ! => update the internal model
        tableInfo.content.cells = theReorderedRows;
        tableInfo.content.links = theReorderedRowLinks;
        if (theReorderedRowsHeaderData != null)
            tableInfo.headers.row.data = theReorderedRowsHeaderData;
        if (theReorderedRowsHeaderLinks != null)
            tableInfo.headers.row.links = theReorderedRowsHeaderLinks;

    }

    /**
     * Adds a new row in the table, without row header information
     *
     * @param rowData required list of row data values. Accepted types for T: immutable Object (String, Double, Boolean ...)
     *                or TableElement.
     * @return number of rows in content part, i.e ignoring optional columns header.
     * @throws IkatsException consistency/unexpected error occurred
     */
    public <T> int appendRow(List<T> rowData) throws IkatsException {

        return appendRowInternal(null, (List<Object>) rowData);
    }

    /**
     * Adds a new row in the table, with row header value.
     *
     * @param rowHeaderData optional row header data. null implies that row header will be ignored, not completed. Accepted types
     *                      for H: immutable Object (String, Double, Boolean ...) or TableElement.
     * @param rowData       required list of row data values. Accepted types for T: immutable Object (String, Double, Boolean ...)
     *                      or TableElement grouping data+link or else DataLink defining one link, without data info.
     * @return number of rows in content part, i.e ignoring optional columns header.
     * @throws IkatsException consistency/unexpected error occurred
     */
    public <H, T> int appendRow(H rowHeaderData, List<T> rowData) throws IkatsException {

        return appendRowInternal(rowHeaderData, (List<Object>) rowData);
    }

    /**
     * internal implementation of service appending a row
     *
     * @param rowHeaderData the inserted header associated to rowdata: required when this.isHandlingRowsHeader()
     * @param rowData       required list of row data values. Accepted types for T: immutable Object (String, Double, Boolean ...)
     *                      or TableElement grouping data+link or else DataLink defining one link, without data info.
     * @return
     * @throws IkatsException inconsistency error detected.
     */
    private int appendRowInternal(Object rowHeaderData, List<Object> rowData) throws IkatsException {

        if (rowHeaderData != null) {
            if (!isHandlingRowsHeader()) {
                throw new IkatsException(
                        "Cannot add row header: " + rowHeaderData.toString() + "not managing rows header");
            }
            this.getRowsHeader().addItem(rowHeaderData);
        }
        if ((rowData != null) && !rowData.isEmpty())
            this.getContent().addRow(TableElement.encodeElements(rowData));

        return this.getContent().cells.size();
    }

    /**
     * Inserts a new row and associated header, just before the row matched by the header location specified by
     * beforeRowHeader.
     * <p/>
     * Note: the method forbids to insert the row before the columns header, ie the beforeRowHeader should match one row
     * in the content part.
     *
     * @param beforeRowHeader the header value defining the insert location: String value compared to each header item as String.
     * @param rowHeaderData   the inserted row header data. null is not accepted. (when null: use insertRow(int, List<T> ) instead).
     * @param rowData         the inserted row. T type is expected to be an immutable Object (simpler case) like String, Integer,
     *                        Double, (...) or TableElement, or else DataLink.
     * @throws IkatsException            inconsistency error, for instance: no rows header managed, or bad value for one parameter, or
     *                                   insertion forbidden before the columns header.
     * @throws ResourceNotFoundException column location unmatched by beforeColHeaderData
     */
    public <H, T> void insertRow(H beforeRowHeader, H rowHeaderData, List<T> rowData)
            throws IkatsException, ResourceNotFoundException {

        if (beforeRowHeader == null)
            throw new IkatsException("Cannot insert row: locating beforeRowHeader=null, it should not.");

        if (!isHandlingRowsHeader())
            throw new IkatsException("Cannot insert row using rows header: this table is without rows header");

        if (rowHeaderData == null)
            throw new IkatsException("Cannot insert row using rows header when rowHeaderData is null");

        String headerLocationValue = beforeRowHeader.toString();
        int rowLocation = getIndexRowHeader(headerLocationValue);

        if (rowLocation == -1)
            throw new ResourceNotFoundException("Unmatched row for row header=" + headerLocationValue);

        int insertedIndexRowHeader = rowLocation;
        if (isHandlingColumnsHeader()) {
            if (rowLocation == 0)
                throw new IkatsException(
                        "Forbidden insertion: a row before the columns header =>  failed insertRow() for beforeRowHeader="
                                + headerLocationValue);
            else {
                rowLocation--;
            }
        }
        TableElement elemH = TableElement.encodeElement(rowHeaderData);
        getRowsHeader().insertItem(insertedIndexRowHeader, elemH);

        insertRow(rowLocation, rowData);
    }

    /**
     * Inserts a new row just before the content row at specified content index.
     *
     * @param insertionIndex the index of insertion in the content part.
     * @param rowData        list of objects. T type is expected to be TableElement, or DataLink or immutable Object otherwise
     *                       (simple case).
     * @throws IkatsException inconsistency error occured.
     */
    public <T> void insertRow(int insertionIndex, List<T> rowData) throws IkatsException {
        // The cast is needed in order to select the GOOD encodeElements(...)
        this.getContent().insertRow(insertionIndex, TableElement.encodeElements((List<Object>) rowData));
    }

    /**
     * Appends a new column at the right of content part
     *
     * @param colData the appended column as a List of types T: either Object for data, TableElement for data+link, DataLink
     *                for link. Note: in usual simple case of Object for data: please use immutable types (String, Integer,
     *                Double, Boolean, ...)
     * @return the number of columns in table content part -ignoring the table header section -
     * @throws IkatsException
     */
    public <T> int appendColumn(List<T> colData) throws IkatsException {

        return appendColumnInternal(null, (List<Object>) colData);
    }

    /**
     * Appends a column associated with its appended header data
     *
     * @param colHeaderData the appended header: object typed TableElement, or DataLink or another Object. Simple use String is
     *                      recommended, usually.
     * @param colData       the appended column as a List of types T: either Object for data, TableElement for data+link, DataLink
     *                      for link. Note: in usual simple case of Object for data: please use immutable types (String, Integer,
     *                      Double, Boolean, ...)
     * @return the number of columns, afterwards, excluding the optional rows header
     * @throws IkatsException
     */
    public <H, T> int appendColumn(H colHeaderData, List<T> colData) throws IkatsException {

        return appendColumnInternal(colHeaderData, (List<Object>) colData);
    }

    /**
     * internal implementation of service appending a column
     *
     * @param colHeaderData
     * @param colData
     * @return
     * @throws IkatsException
     */
    private int appendColumnInternal(Object colHeaderData, List<Object> colData) throws IkatsException {

        if (colHeaderData == null && isHandlingColumnsHeader()) {
            throw new IkatsException("Required column header: this is handling columns header");
        }
        if (colHeaderData != null) {
            if (!isHandlingColumnsHeader()) {
                throw new IkatsException(
                        "Cannot add column header: " + colHeaderData.toString() + "not managing columns header");
            }
            this.getColumnsHeader().addItem(colHeaderData);
        }
        if ((colData != null) && !colData.isEmpty()) {
            TableContent myContent = getContent();
            if ((myContent.cells == null) || myContent.cells.isEmpty()) {
                // special case: first column added => allocate empty rows
                if (myContent.cells == null)
                    myContent.cells = new ArrayList<>();

                for (int i = 0; i < colData.size(); i++) {
                    myContent.cells.add(new ArrayList<>());
                }
            }
            myContent.addColumn(TableElement.encodeElements(colData));
        }
        // returns the number of columns <=> first row size assuming that
        // Table has a
        // standard size, rectangular.
        return this.getContent().cells.get(0).size();
    }

    /**
     * Inserts a new column and associated header, just before the column matched by the header location specified by
     * beforeColHeaderData.
     * <p/>
     * Note: the method forbids to intert the column before the row header, ie the beforeColHeaderData should match
     * column in the content part.
     *
     * @param beforeColHeader the header value defining the insert location: String value compared to each header item as String.
     * @param colHeaderData   the inserted column header data. null is not accepted. (when null: use insertColumn(int, List<T> )
     *                        instead).
     * @param columnData      the inserted column data. T type is expected to be an immutable Object (simpler case: String, Integer,
     *                        Double, ...) or TableElement, or else DataLink.
     * @throws IkatsException            inconsistency error, for instance: no columns header managed, or bad value for one parameter, or
     *                                   insertion forbidden before the rows header.
     * @throws ResourceNotFoundException column location unmatched by beforeColHeaderData
     */
    public <H, T> void insertColumn(H beforeColHeader, H colHeaderData, List<T> columnData)
            throws IkatsException, ResourceNotFoundException {

        if (beforeColHeader == null)
            throw new IkatsException("Cannot insert column: locating beforeColHeader=null, it should not.");

        if (!isHandlingColumnsHeader())
            throw new IkatsException("Cannot insert column using columns header: this table is without columns header");

        if (colHeaderData == null)
            throw new IkatsException("Cannot insert column using columns header when colHeaderData is null");

        String headerLocationValue = beforeColHeader.toString();
        int columnLocation = getIndexColumnHeader(headerLocationValue);

        if (columnLocation == -1)
            throw new ResourceNotFoundException("Unmatched column for column header=" + headerLocationValue);

        int insertedIndexColHeader = columnLocation;
        if (isHandlingRowsHeader()) {
            if (columnLocation == 0)
                throw new IkatsException(
                        "Forbidden insertion: a column before the rows header =>  failed insertColumn() for beforeColHeader="
                                + headerLocationValue);
            else {
                columnLocation--;
            }
        }
        TableElement elemH = TableElement.encodeElement(colHeaderData);
        getColumnsHeader().insertItem(insertedIndexColHeader, elemH);

        insertColumn(columnLocation, (List<Object>) columnData);
    }

    /**
     * Inserts a new column just before the content column at specified content index.
     *
     * @param insertionIndex the index of insertion in the content part.
     * @param columnData     list of objects. T type is expected to be TableElement, or DataLink or immutable Object otherwise
     *                       (simple case).
     * @throws IkatsException inconsistency error occured.
     */
    public <T> void insertColumn(int insertionIndex, List<T> columnData) throws IkatsException {
        // The cast is needed in order to select the GOOD encodeElements(...)
        this.getContent().insertColumn(insertionIndex, TableElement.encodeElements((List<Object>) columnData));
    }
}

