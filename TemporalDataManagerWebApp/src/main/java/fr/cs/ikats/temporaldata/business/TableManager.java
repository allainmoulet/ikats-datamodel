package fr.cs.ikats.temporaldata.business;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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
import fr.cs.ikats.lang.NaturalOrderComparator;
import fr.cs.ikats.process.data.model.ProcessData;
import fr.cs.ikats.temporaldata.business.ProcessDataManager.ProcessResultTypeEnum;
import fr.cs.ikats.temporaldata.business.TableInfo.DataLink;
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
     * The Table class is the business resource for the 'table' IKATS functional
     * type, it is a wrapper of TableInfo. The Table class provides end-user
     * services in java world. <br/>
     * Note the difference with TableInfo: TableInfo manages the JSON
     * persistence, and can be used by the REST web services, involving JSON
     * media type, grouped in the class TableResource. You can get the TableInfo
     * managed thanks to the getter getTableInfo()
     */
    static public class Table {

        private final TableInfo tableInfo;

        /**
         * Creates the business Table handling TableInfo. Internally used by
         * TableManager.
         *
         * @param handledTable
         *            either from an existing TableInfo, for example, the Table
         *            loaded from a json content or from new TableInfo(), in
         *            order to initialize it.
         */
        Table(TableInfo handledTable) {
            super();
            this.tableInfo = handledTable;
        }

        /**
         * {@inheritDoc}
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
         * Gets the TableInfo object handled by Table: see TableInfo and Table
         * documentation to know more about respective roles of these two
         * classes.
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
            }
            else
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
            }
            else
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
            }
            else
                return null;
        }

        /**
         * Counts the number of rows in the table. Assumed: shape of Table is
         * consistent.
         *
         * @param includeColumnHeader
         *            set to True if the header line shall be counted, false
         *            otherwise
         * @return the number of rows
         */
        public int getRowCount(boolean includeColumnHeader) {
            // Count rows in Content part:
            List<List<Object>> contentData = getContentData();
            int nbRows = contentData != null ? contentData.size() : 0;

            // If required: add 1 for the column header
            if (includeColumnHeader && getColumnsHeader() != null)
                nbRows++;
            return nbRows;
        }

        /**
         * Counts the number of columns in the table. Assumed: shape of Table is
         * consistent: all rows have same number of columns, and the rows header
         * is consistent: its size is equal to number of columns + 1
         *
         * @param includeRowHeader
         *            set to True if the header column shall be counted, false
         *            otherwise
         * @return the number of column
         */
        public int getColumnCount(boolean includeRowHeader) {
            int countCol = 0;
            // Count columns in Content part:
            List<List<Object>> contentData = getContentData();
            if ((contentData != null) && !contentData.isEmpty()) {
                countCol = contentData.get(0).size();
            }

            // If required: add 1 for the column header
            if (includeRowHeader && getRowsHeader() != null)
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
         * @param value
         *            the value to look for
         * @return index matched by value, if exists, or -1
         * @throws IkatsException
         *             when column header is null
         */
        public int getIndexColumnHeader(String value) throws IkatsException {
            return this.getHeaderIndex(this.getColumnsHeader(), value);
        }

        /**
         * Retrieves the header row index matching the value.
         *
         * @param value
         *            the value to look for
         * @return index matched by value, if exists, or -1
         * @throws IkatsException
         *             when row header is null
         */
        public int getIndexRowHeader(String value) throws IkatsException {
            return this.getHeaderIndex(this.getRowsHeader(), value);
        }

        /**
         * Not yet implemented
         *
         * @return
         * @deprecated Unsupported operation.
         */
        public <T> List<T> getColumnsHeaderItems() {
            // todo V2
            throw new UnsupportedOperationException("Not yet implemented");
        }

        /**
         * Not yet implemented
         *
         * @return
         * @deprecated Unsupported operation.
         */
        public <T> List<T> getRowsHeaderItems() {
            // todo V2
            throw new UnsupportedOperationException("Not yet implemented");
        }

        /**
         * Gets the header size. Internal use only.
         *
         * @param theHeader
         *            Header object to get the size for
         * @return size of the theHeader.data, if defined, otherwise -1
         */
        private int getHeaderSize(Header theHeader) {

            if (theHeader == null || theHeader.data == null)
                return -1;

            return theHeader.data.size();
        }

        /**
         * Retrieves the index position of a header name in the header container
         * (theHeader).
         * <p>
         * Note: the value is compared to theHeader.data.get(x).toString(): this
         * enables to manage also header data types different from String.
         *
         * @param theHeader
         *            Header is the header items container, where is searched
         *            the value.
         * @param value
         *            Name of the searched header item (item: column or row) to
         *            get index
         * @return index of header item matching the value. First position is
         *         zero. If value is not found -& is returned.
         * @throws IkatsException
         *             unexpected error. Examples: null value, or theHeader is
         *             null, or theHeader.data is null.
         */
        private int getHeaderIndex(Header theHeader, String value) throws IkatsException {

            if (value == null)
                throw new IkatsException("Unexpected column name is null ");

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
         * Gets the column values from the Table.
         * <p>
         * Note: this getter ignores the links possibly defined on the column.
         *
         * @param columnName
         *            name of the selected column: this criterion is in the
         *            column header.
         * @return the content column below selected column header name.
         * @throws ResourceNotFoundException
         *             when the column is not found
         * @throws IkatsException
         *             unexpected error occured: for exemple ClassCastException
         *             error.
         */
        public <T> List<T> getColumn(String columnName) throws IkatsException, ResourceNotFoundException {

            return (List<T>) getColumn(columnName, String.class);
        }

        /**
         * Gets the column values from the Table.
         * 
         * Note: this getter ignores the links possibly defined on the column.
         * 
         * @param columnName
         *            name of the selected column: this criteria is in the
         *            column header.
         * @return the content column below selected column header name.
         * @throws ResourceNotFoundException
         *             when the column is not found
         * @throws IkatsException
         *             unexpected error occured: for exemple ClassCastException
         *             error.
         */
        public <T> List<T> getColumn(String columnName, Class<T> castingClass) throws IkatsException, ResourceNotFoundException {

            try {

                int matchedIndex = this.getIndexColumnHeader(columnName);
                if (matchedIndex < 0) {
                    throw new ResourceNotFoundException(
                            "Unmatched getColumn(): in Columns header: no column named " + columnName + " of table " + this.toString());
                }
                return (List<T>) getColumn(matchedIndex, castingClass);
            }
            catch (IkatsException e) {
                throw new IkatsException("Failed getColumn(" + columnName + ") in table: 1" + this.toString(), e);
            }
        }

        /**
         * Gets the column values from the Table. Note: this getter ignores the
         * links possibly defined on the column. <br/>
         * For end-users: when possible, it is advised to use getColumn(String),
         * less confusing than getColumn(int).
         * 
         * @param index
         *            of selected column. Note: index relative to the global
         *            table. If rows header exists: 0 points to rows header;
         *            otherwise 0 points to first column of this.getContent().
         * @return the selected column values. Note the columns header part is
         *         not included. And if Rows header is selected: the first rows
         *         header element is not included.
         * @throws IkatsException
         * @throws ResourceNotFoundException
         *             when the column is not found
         */
        public <T> List<T> getColumn(int index, Class<T> castingClass) throws IkatsException, ResourceNotFoundException {

            try {
                if (index < 0) {
                    throw new ResourceNotFoundException(
                            "Unmatched getColumn(): in Columns header: no column at content index=" + index + " of table " + this.toString());
                }

                List<Object> matchedData;
                Header rowsHeader = this.getRowsHeader();
                if ((rowsHeader != null) && index == 0) {
                    // Read the rows header and ignore its first element
                    matchedData = new ArrayList<>(rowsHeader.getData());
                    matchedData.remove(0);

                }
                else {
                    // inside the content part: the column is indexed by
                    int contentIndex = (rowsHeader == null) ? index : index - 1;
                    matchedData = this.getContent().getColumnData(contentIndex);
                }

                return convertList(matchedData, castingClass);
            }
            catch (IkatsException typeError) {
                throw new IkatsException(
                        "Failed getColumn() in table: cast failed on column at content index=" + index + " in table " + this.toString(), typeError);
            }
        }

        /**
         * Gets the row values as String from this table, using the toString()
         * on each object instance.
         * 
         * Note: this getter ignores the links possibly defined in the row.
         * 
         * @param rowName
         *            name of the selected row: this criteria is in the row
         *            header.
         * @return the content row below selected by the rowName parameter.
         *         Note: the selected row does not contain the rows header part.
         * @throws IkatsException
         *             row header is undefined or unexpected error.
         * @throws ResourceNotFoundException
         *             not row is selected by rowName.
         */
        public List<String> getRow(String rowName) throws IkatsException, ResourceNotFoundException {

            try {

                int matchedIndex = this.getIndexRowHeader(rowName);
                if (matchedIndex < 0) {
                    throw new ResourceNotFoundException(
                            "Unmatched getRow(): in Rows header: no row named " + rowName + " of table " + this.toString());
                }
                return getRow(matchedIndex, String.class);
            }
            catch (IkatsException e) {
                throw new IkatsException("Failed getColumn(" + rowName + ") in table: " + this.toString(), e);
            }
        }

        /**
         * Gets the row values from the Table. Note: this getter ignores the
         * links possibly defined on the row. <br/>
         * For end-users: when possible, it is advised to use getColumn(String),
         * less confusing than getColumn(int).
         * 
         * @param rowName
         *            name of the row header item selecting the row values.
         * 
         * @return the selected row values. Note that the rows header part is
         *         not included in the result. And if columns header is selected
         *         by index=0: the first columns header element is not included.
         * @throws IkatsException
         *             inconsistency error detected
         * @throws ResourceNotFoundException
         *             when the row is not found
         */
        public <T> List<T> getRow(String rowName, Class<T> castingClass) throws IkatsException, ResourceNotFoundException {

            try {

                int matchedIndex = this.getIndexRowHeader(rowName);
                if (matchedIndex < 0) {
                    throw new ResourceNotFoundException(
                            "Unmatched getRow(): in Rows header: no row named " + rowName + " of table " + this.toString());
                }
                return getRow(matchedIndex, castingClass);
            }
            catch (IkatsException e) {
                throw new IkatsException("Failed getRow(" + rowName + ") in table: " + this.toString(), e);
            }
        }

        /**
         * Gets the selected row values from this table. Note: this getter
         * ignores the links possibly defined in the row. <br/>
         * For end-users: when possible, it is advised to use getRow(String),
         * less confusing than getRow(int).
         * 
         * @param index
         *            index of selected row. Note: index is relative to the
         *            whole table. If column header exists: 0 points to
         *            columnHeaders; otherwise 0 points to first row of
         *            this.getContent().
         * @param castingClass
         *            class to which the values are converted
         * @return selected row values. Note the row header part is not
         *         included. And if Columns header is selected: first header
         *         element is not included.
         * @throws IkatsException
         *             row header is undefined
         * @throws ResourceNotFoundException
         *             row is not found
         */
        public <T> List<T> getRow(int index, Class<T> castingClass) throws IkatsException, ResourceNotFoundException {
            try {
                if (index < 0) {
                    throw new ResourceNotFoundException(
                            "Unmatched getRow(): in Rows header: no row at content index=" + index + " of table " + this.toString());
                }

                List<Object> matchedData;
                Header columnsHeader = this.getColumnsHeader();
                if ((columnsHeader != null) && index == 0) {
                    // Read the columns header and ignore its first element

                    matchedData = new ArrayList<>(columnsHeader.getData());
                    matchedData.remove(0);

                }
                else {
                    // inside the content part: the row is indexed by
                    // matchedIndex - 1
                    int contentIndex = (columnsHeader == null) ? index : index - 1;
                    matchedData = this.getContent().getRowData(contentIndex);
                }

                // iterate and cast the value to T ...
                return convertList(matchedData, castingClass);
            }
            catch (IkatsException typeError) {
                throw new IkatsException("Failed getRow() in row at content index=" + index + " in table " + this.toString(), typeError);
            }
        }

        /**
         * Getter pf the content part of the table. Beware: content may not be
         * initialized.
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
         List<List<Object>> getContentData() {
            if (this.tableInfo.content == null)
                return null;

            return this.tableInfo.content.cells;
        }

        /**
         * Internal getter of this.tableInfo.content.links
         * 
         * @return this.tableInfo.content.links or null if undefined
         */
        private List<List<DataLink>> getContentDataLinks() {
            if (this.tableInfo.content == null)
                return null;

            return this.tableInfo.content.links;
        }

        /**
         * Disables the columns header on this table. Note: once called:
         * this.isHandlingColumnsHeader() witll return false.
         */
        public void disableColumnsHeader() {
            if (this.tableInfo.headers != null)
                this.tableInfo.headers.col = null;
        }

        /**
         * Disables the header management.
         */
        public void disableRowsHeader() {
            if (this.tableInfo.headers != null)
                this.tableInfo.headers.row = null;
        }

        /**
         * Initializes the links configuration, when some links are required.
         *
         * @param enabledOnColHeader
         *            True enables the links on the column header
         * @param defaultPropertyColHeader
         *            the DataLink providing default values for column links
         * @param enabledOnRowHeader
         *            enables the links on the row header
         * @param defaultPropertyRowHeader
         *            the DataLink providing default values for row links
         * @param enabledOnContent
         *            enables the links on the content
         * @param defaultPropertyContent
         *            the DataLink providing default values for content links
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
         * For internal use: Initializes the Header of columns.
         *
         * @param startWithTopLeftCorner
         *            true indicates that the defined headerData and the defined
         *            headerLinks begin from the top-left corner
         * @param defaultLink
         *            default definition for the links possibly defined by
         *            headerLinks. Optional: null if undefined.
         * @param headerData
         *            defines the data part of the header. null when no data
         *            managed. Note: null is discouraged for data part. Pass an
         *            empty list if you want to manage data, without specifiing
         *            them with this initialization.
         * @param headerLinks
         *            defines the links part of the header. null when no links
         *            managed. Pass an empty List<DataLink> if you want to
         *            manage links, without specifiing them with this
         *            initialization.
         * @return initialized and attached header, which can be completed later
         *         using {@link Header#addItem}
         * @throws IkatsException
         *             inconsistency error detected
         */
        Header initColumnsHeader(boolean startWithTopLeftCorner, DataLink defaultLink, List<Object> headerData, List<DataLink> headerLinks)
                throws IkatsException {
            if (defaultLink != null && headerLinks == null) {
                throw new IkatsException("Inconsistency: default link cannot be defined if the links are not manages (headerLinks == null)");
            }
            if (this.tableInfo.headers == null)
                this.tableInfo.headers = new TableHeaders();

            this.tableInfo.headers.col = Header.createHeader(headerData, headerLinks, defaultLink, startWithTopLeftCorner);
            return this.tableInfo.headers.col;
        }

        /**
         * For internal use: Initializes the rows header.
         *
         * @param startWithTopLeftCorner
         *            true indicates that the defined headerData and the defined
         *            headerLinks begin from the top-left corner
         * @param defaultLink
         *            default definition for the links possibly defined by
         *            headerLinks. Optional: null if undefined.
         * @param headerData
         *            defines the data part of the header. null when no data
         *            managed. Note: null is discouraged for data part. Pass an
         *            empty list if you want to manage data, without specifiing
         *            them with this initialization.
         * @param headerLinks
         *            defines the links part of the header. null when no links
         *            managed. Pass an empty List<DataLink> if you want to
         *            manage links, without specifiing them with this
         *            initialization.
         * @return initialized and attached header, which can be completed later
         *         using {@link Header#addItem}
         * @throws IkatsException
         *             inconsistency error detected
         */
        Header initRowsHeader(boolean startWithTopLeftCorner, DataLink defaultLink, List<Object> headerData, List<DataLink> headerLinks)
                throws IkatsException {
            if (defaultLink != null && headerLinks == null) {
                throw new IkatsException("Inconsistency: default link cannot be defined if the links are not manages (headerLinks == null)");
            }
            if (this.tableInfo.headers == null)
                this.tableInfo.headers = new TableHeaders();

            this.tableInfo.headers.row = Header.createHeader(headerData, headerLinks, defaultLink, startWithTopLeftCorner);
            return this.tableInfo.headers.row;
        }

        /**
         * Internal use only: initializes an empty TableContent under
         * this.tableInfo.content.
         * 
         * @param manageLinks
         *            true in order to manage links under this.tableInfo.content
         * @param defaultLink
         *            when not null: the DataLink defining the default
         *            properties applicable to content links.
         * @return initialized this.tableInfo.content
         * @throws IkatsException
         *             inconsistency error during initialization
         */
        TableContent initContent(boolean manageLinks, DataLink defaultLink) throws IkatsException {

            this.tableInfo.content = TableContent.initContent(new ArrayList<List<Object>>(), manageLinks ? new ArrayList<List<DataLink>>() : null,
                    defaultLink);

            return this.tableInfo.content;
        }

        /**
         * Sorts the rows under the column header, according to one column.
         * <p>
         * If a row header is managed, it is sorted the same way.
         * <p>
         * Assumed: the column named columnHeaderName ought to have homogeneous class on items.
         * @param columnHeaderName
         *            name of the sorting criterion.
         * @param reverse reverse is true for descending order.
         * @throws IkatsException inconsistency detected
         * @throws ResourceNotFoundException the sorting colunm is not found
         */
        public void sortRowsByColumnValues(String columnHeaderName, boolean reverse) throws IkatsException, ResourceNotFoundException {
            if ( ! isHandlingColumnsHeader() ) 
                throw new IkatsException("Bad usage: sortRowsByColumnValues(String) impossible when Table is not handling columns header.");
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
         * @throws IkatsException inconsistency detected
         * @throws ResourceNotFoundException the sorting colunm is not found
         */
        public void sortRowsByColumnValues(int index, boolean reverse) throws IkatsException, ResourceNotFoundException {
            
            // no sort needed if content data is empty !
            if ( getContentData() == null ) return;
            
            // we will apply natural sort on values converted to String
            List<String> sortingColumn = getColumn(index, String.class);
            
            // associates original indexes to their sorting value
            //
            Map<Integer, String> mapIndexToSortingValue = new HashMap<Integer, String>();
            
            // ... and initializes indexes: the list of indexes, before it is sorted differently ...
            List<Integer> indexes= new ArrayList<>();
            int originalIndex = 0;
            for (String sortingValue : sortingColumn) {
                Integer currentIndex= originalIndex;
                mapIndexToSortingValue.put(currentIndex, sortingValue);
                indexes.add(currentIndex);
                originalIndex++;
            }
            
            Comparator<Integer> compareRows = getIndexComparator(mapIndexToSortingValue, reverse);
            
            // ... and apply the reordering on the list indexes 
            Collections.sort( indexes, compareRows );
            
            List<List<Object>> theOriginalRows= tableInfo.content.cells;
            List<List<Object>> theReorderedRows= new ArrayList<>();
            
            List<List<DataLink>> theOriginalRowLinks= tableInfo.content.links;
            List<List<DataLink>> theReorderedRowLinks= theOriginalRowLinks != null ? new ArrayList<>() : null;
            
            List<Object> theOriginalRowsHeaderData = isHandlingRowsHeader() ? getRowsHeader().data : null;
            List<Object> theReorderedRowsHeaderData = theOriginalRowsHeaderData != null ? new ArrayList<>() : null;
            
            List<DataLink> theOriginalRowsHeaderLinks = isHandlingRowsHeader() ? getRowsHeader().links : null;
            List<DataLink> theReorderedRowsHeaderLinks = theOriginalRowsHeaderLinks != null ? new ArrayList<>() : null;
            
            // first element (data or link) of rows header is not sorted if a columns header exists
            // => save this in integer firstHeaderSorted
            int firstHeaderSorted = isHandlingColumnsHeader() ? 1 : 0;
        
            // inserts fixed elements of rows header (if required)
            if ((theReorderedRowsHeaderLinks != null ) && (firstHeaderSorted == 1))
                theReorderedRowsHeaderLinks.add( theOriginalRowsHeaderLinks.get(0) );
            if ((theReorderedRowsHeaderData != null ) && (firstHeaderSorted == 1))
                theReorderedRowsHeaderData.add( theOriginalRowsHeaderData.get(0) );
            
            // ... and the rebuild each reordered collection ...
            for (Integer reorderedIndex : indexes) {
                theReorderedRows.add( theOriginalRows.get( reorderedIndex) );
                
                if ( theReorderedRowLinks != null )
                    theReorderedRowLinks.add( theOriginalRowLinks.get(reorderedIndex));
                
                if ( theReorderedRowsHeaderData != null )
                    theReorderedRowsHeaderData.add( theOriginalRowsHeaderData.get( reorderedIndex + firstHeaderSorted));
                
                if ( theReorderedRowsHeaderLinks != null )
                    theReorderedRowsHeaderLinks.add( theOriginalRowsHeaderLinks.get( reorderedIndex + firstHeaderSorted));
            }
            
            // ... done ! => update the internal model
            tableInfo.content.cells = theReorderedRows;
            tableInfo.content.links = theReorderedRowLinks;
            if (theReorderedRowsHeaderData != null) tableInfo.headers.row.data = theReorderedRowsHeaderData;
            if (theReorderedRowsHeaderLinks != null) tableInfo.headers.row.links = theReorderedRowsHeaderLinks;
            
        }

        /**
         * Adds a new row in the table, without row header information
         * 
         * @param rowData
         *            required list of row data values. Accepted types for T:
         *            immutable Object (String, Double, Boolean ...) or
         *            TableElement.
         * @return number of rows in content part, i.e ignoring optional columns
         *         header.
         * @throws IkatsException
         *             consistency/unexpected error occurred
         */
        public <T> int appendRow(List<T> rowData) throws IkatsException {

            return appendRowInternal(null, (List<Object>) rowData);
        }

        /**
         * Adds a new row in the table, with row header value.
         * 
         * @param rowHeaderData
         *            optional row header data. null implies that row header
         *            will be ignored, not completed. Accepted types for H:
         *            immutable Object (String, Double, Boolean ...) or
         *            TableElement.
         * @param rowData
         *            required list of row data values. Accepted types for T:
         *            immutable Object (String, Double, Boolean ...) or
         *            TableElement grouping data+link or else DataLink defining
         *            one link, without data info.
         * @return number of rows in content part, i.e ignoring optional columns
         *         header.
         * @throws IkatsException
         *             consistency/unexpected error occurred
         */
        public <H, T> int appendRow(H rowHeaderData, List<T> rowData) throws IkatsException {

            return appendRowInternal(rowHeaderData, (List<Object>) rowData);
        }

        /**
         * 
         * @param rowHeaderData
         *            the inserted header associated to rowdata: required when
         *            this.isHandlingRowsHeader()
         * @param rowData
         *            required list of row data values. Accepted types for T:
         *            immutable Object (String, Double, Boolean ...) or
         *            TableElement grouping data+link or else DataLink defining
         *            one link, without data info.
         * @return
         * @throws IkatsException
         *             inconsistency error detected.
         */
        private int appendRowInternal(Object rowHeaderData, List<Object> rowData) throws IkatsException {

            if (rowHeaderData != null) {
                if (!isHandlingRowsHeader()) {
                    throw new IkatsException("Cannot add row header: " + rowHeaderData.toString() + "not managing rows header");
                }
                this.getRowsHeader().addItem(rowHeaderData);
            }
            if ((rowData != null) && !rowData.isEmpty())
                this.getContent().addRow(TableElement.encodeElements(rowData));

            return this.getContent().cells.size();
        }

        public <T> int appendColumn(List<T> colData) throws IkatsException {

            return appendColumnInternal(null, (List<Object>) colData);
        }

        public <H, T> int appendColumn(H colHeaderData, List<T> colData) throws IkatsException {

            return appendColumnInternal(colHeaderData, (List<Object>) colData);
        }

        private int appendColumnInternal(Object colHeaderData, List<Object> colData) throws IkatsException {

            if (colHeaderData == null && isHandlingColumnsHeader()) {
                throw new IkatsException("Required column header: this is handling columns header");
            }
            if (colHeaderData != null) {
                if (!isHandlingColumnsHeader()) {
                    throw new IkatsException("Cannot add column header: " + colHeaderData.toString() + "not managing columns header");
                }
                this.getColumnsHeader().addItem(colHeaderData);
            }
            if ((colData != null) && !colData.isEmpty()){
                TableContent myContent = getContent();
                if ( ( myContent.cells == null) || myContent.cells.isEmpty() )
                {
                    // special case: first column added => allocate empty rows
                    if (myContent.cells == null) myContent.cells = new ArrayList<>();
                    
                    for (Object colDataItem : colData) {
                        myContent.cells.add( new ArrayList<>() );
                    }
                }
                myContent.addColumn(TableElement.encodeElements(colData));
            }
            // returns the number of columns <=> first row size assuming that Table has a
            // standard size, rectangular.
            return this.getContent().cells.get(0).size();
        }

        public <H, T> int insertColumn(H beforeColHeader, H colHeaderData, List<T> columnData) throws IkatsException {
            // todo V2
            return insertColumn((Object) beforeColHeader, (Object) colHeaderData, (List<Object>) columnData);
        }

        private int insertColumnInternal(Object beforeColHeader, Object colHeaderData, List<Object> columnData) throws IkatsException {
            // todo V2
            // handle specific case: beforeColHeader is null => the column is
            // appended as the last column
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }

    /**
     * jsonObjectMapper manages JSON persistence of Table
     */
    private ObjectMapper jsonObjectMapper;

    /**
     * processDataManager manages ProcessData persistence of Table
     */
    private ProcessDataManager processDataManager;

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

        processDataManager = new ProcessDataManager();
    }

    /**
     * Loads a Table object from the json plain text, using configuration from
     * this.jsonObjectMapper.
     *
     * @param jsonContent:
     *            json plain text value.
     * @return the loaded Table
     * @throws IkatsJsonException
     *             in case of parsing error.
     */
    public TableInfo loadFromJson(String jsonContent) throws IkatsJsonException {
        try {
            return jsonObjectMapper.readValue(jsonContent, TableInfo.class);
        }
        catch (Exception e) {
            throw new IkatsJsonException("Failed to load Table business resource from the json content", e);
        }
    }

    /**
     * Serializes the TableInfo into equivalent JSON String, using internal
     * configuration of this.jsonObjectMapper.
     *
     * @param tableInfo
     *            the object serialized
     * @return the JSON content written by serialization of tableInfo.
     * @throws IkatsJsonException
     *             JSON serialization failed.
     */
    public String serializeToJson(TableInfo tableInfo) throws IkatsJsonException {
        try {
            return jsonObjectMapper.writeValueAsString(tableInfo);
        }
        catch (Exception e) {
            throw new IkatsJsonException("Failed to serialize Table business resource to the json content", e);
        }
    }

    /**
     * Creates and initializes the structure of an empty Table,
     * <ul>
     * <li>with columns header enabled when parameter withColumnsHeader is true
     * ,</li>
     * <li>with rows header enabled when parameter withColumnsHeader is true ,
     * </li>
     * </ul>
     * This Table is initialized without links managed: see how to configure
     * links management with enablesLinks() method.
     * 
     * @return created Table, ready to be completed.
     */
    public Table initEmptyTable(boolean withColumnsHeader, boolean withRowsHeader) {
        TableInfo tableJson = new TableInfo();

        tableJson.table_desc = new TableDesc();
        tableJson.headers = new TableHeaders();
        if (withColumnsHeader || withRowsHeader) {
            if (withRowsHeader) {
                tableJson.headers.row = new Header();
                tableJson.headers.row.data = new ArrayList<>();
            }
            if (withColumnsHeader) {
                tableJson.headers.col = new Header();
                tableJson.headers.col.data = new ArrayList<>();
            }
        }

        tableJson.content = new TableContent();
        tableJson.content.cells = new ArrayList<>();

        return new Table(tableJson);
    }

    /**
     * Creates a Table from the JSON content
     * 
     * @param tableJson
     *            the plain text encoding the JSON
     * @return the Table associated to tableJson
     * @throws IkatsJsonException
     */
    public Table initTable(String tableJson) throws IkatsJsonException {
        TableInfo tableInfo = loadFromJson(tableJson);
        return initTable(tableInfo, false);
    }

    /**
     * Initializes a table, with defined columnHeaders, without links, with rows
     * header enabled by parameter withRowHeader.
     *
     * @param columnHeaders
     *            list of columns header values provided as String
     * @param withRowHeader:
     *            true activates the row header management. But the rows header
     *            content is set later.
     * @return initialized Table: ready to use appendRow() for example.
     * @throws IkatsException
     *             initialization error
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
     * Initializes the Table business resource from the TableInfo JSON-mapping
     * resource.
     *
     * @param tableInfo
     *            the JSON-mapping resource.
     * @param copyTableInfo
     *            true demands that returned Table manages a copy of tableInfo.
     * @return initialized Table, wrapping the (copy of) tableInfo
     */
    public Table initTable(TableInfo tableInfo, boolean copyTableInfo) {
        if (copyTableInfo) {
            return new Table(new TableInfo(tableInfo));
        }
        else {
            return new Table(tableInfo);
        }
    }

    /**
     * Gets the JSON resource TableInfo from process data database.
     * 
     * @param tableName
     *            the name of the table is its unique identifier
     * @return read resource TableInfo.
     * @throws IkatsJsonException
     *             failed to read consistent JSON format into TableInfo
     *             structure.
     * @throws IkatsDaoException
     *             unexpected DAO error, from Hibernate, reading the database
     * @throws ResourceNotFoundException
     *             the table name tableName is not matched in the database.
     */
    public TableInfo readFromDatabase(String tableName) throws IkatsJsonException, IkatsDaoException, ResourceNotFoundException {

        try {
            List<ProcessData> dataTables = processDataManager.getProcessData(tableName);

            if (dataTables == null) {
                throw new IkatsDaoException("Unexpected Hibernate error while attempting to read table: name=" + tableName);
            }
            else if (dataTables.isEmpty()) {
                throw new ResourceNotFoundException("No result found for table name=" + tableName);
            }

            ProcessData dataTable = dataTables.get(0);

            // Extract data to json string
            String jsonString = new String(dataTable.getData().getBytes(1, (int) dataTable.getData().length()));

            // Convert to Table type
            TableInfo table = loadFromJson(jsonString);

            // Adds the name to the table:
            // the name is not part of written JSON content presently
            // but it is good to keep it at java level, in order to implement
            // CRUD with unique identifier.
            Table handler = new Table(table);
            handler.setName(tableName);

            LOGGER.trace("Table retrieved from db OK : name=" + tableName);
            return table;
        }
        catch (SQLException sqle) {
            // Why the catch is not on HibernateException ?
            throw new IkatsDaoException("Failed to read Table, reading the BLOB of processData with processId=" + tableName, sqle);
        }

    }

    /**
     * Creates a new Table in process data database.
     * 
     * @param tableName
     *            the unique identifier of the Table is its name
     * @param tableToStore
     *            the TableInfo is required to write the content into the
     *            database. From Table object, use getTableInfo().
     * @return ID of created processData storing the table.
     * @throws IkatsJsonException
     *             error encoding the JSON content
     * @throws IkatsDaoException
     *             error checking if resource already exists
     * @throws IkatsDaoConflictException
     *             error when a resource with processId=tableName exists
     * @throws InvalidValueException
     *             consistency error found in the name of the table: see
     *             TABLE_NAME_PATTERN
     */
    public String createInDatabase(String tableName, TableInfo tableToStore)
            throws IkatsJsonException, IkatsDaoException, IkatsDaoConflictException, InvalidValueException {

        // Initializing potential big table
        if (existsInDatabase(tableName))
            throw new IkatsDaoConflictException("Resource already exists ");

        // Validate the name consistency
        validateTableName(tableName, "Create Table in database");

        // Adds the name to the table:
        // the name is not part of written JSON content presently
        // but it is good to keep it at java level, in order to implement CRUD
        // with unique identifier.
        initTable(tableToStore, false).setName(tableName);

        byte[] data = serializeToJson(tableToStore).getBytes();

        String rid = processDataManager.importProcessData(tableName, tableToStore.table_desc.desc, data, ProcessResultTypeEnum.JSON);
        LOGGER.trace("Table stored Ok in db: " + tableName + " with rid: " + rid);

        return rid;
    }

    /**
     * Deletes the table from the database.
     *
     * @param tableName
     *            the unique identifier of the Table is its name
     */
    public void deleteFromDatabase(String tableName) {
        // The name of the table is in the processId column of table processData
        // => so, we can use directly the removeProcessData(processId) service.

        // No exception raised by this remove
        processDataManager.removeProcessData(tableName);
    }

    /**
     * Basically checks if tableName exists as processId in the database.
     * <p>
     * Note: presently the name of the table is saved in Postgres table
     * process_data in column processId.
     *
     * @param tableName
     *            the name is the identifier of the Table.
     * @return true if table name already exists in database.
     * @throws IkatsDaoException
     *             unexpected Hibernate error.
     */
    public boolean existsInDatabase(String tableName) throws IkatsDaoException {
        List<ProcessData> collectionProcessData = processDataManager.getProcessData(tableName);

        // Temporary solution: check that result is not null
        if (collectionProcessData == null)
            throw new IkatsDaoException("Unexpected Hibernate error: processDataManager.getProcessData(" + tableName + ")");

        return !collectionProcessData.isEmpty();
    }

    /**
     * Gets a table column from a table, reading the table in database.
     * <p>
     * <ul>
     * <li>calls readFromDatabase(tableName)</li>
     * <li>and then getColumnFromTable(table, columnName)</li>
     * </ul>
     * <p>
     * Warning: do not repeat this operation if you have several columns to read
     * from the same table, this will clearly be inefficient! Instead, in that
     * case, use readFromDatabase(), then initTable(TableInfo) and finally use
     * services on Table business resource.
     * 
     * @param tableName
     *            the name of the Table resource is also its unique identifier.
     * @param columnName
     *            column header value identifying the selection.
     * @return the column as list of values, excluding the header value.
     * @throws IkatsException
     *             unexpected error occured. Exemple: internal
     *             ClassCastException when List<T> does not fit the actual data.
     * @throws IkatsDaoException
     *             unexpected hibernate exception reading the table from
     *             database.
     * @throws ResourceNotFoundException
     *             either the resource Table named tableName is not found in the
     *             database, or the column is not found in the table.
     */
    public <T> List<T> getColumnFromTable(String tableName, String columnName) throws IkatsException, IkatsDaoException, ResourceNotFoundException {

        TableInfo table = readFromDatabase(tableName);

        Table tableH = initTable(table, false);

        List<T> column = tableH.getColumn(columnName);

        LOGGER.trace("Column " + columnName + " retrieved from table : " + tableName);

        return column;
    }

    /**
     * Validate that the table name is well encoded.
     *
     * @param name
     *            name of the table, also its unique identifier
     * @param context
     *            information to provide in order to improve error log.
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
     * Internal use: convert a collection of Object data into a collection of
     * type T. For each item from originalList: if type T is String, item is
     * replaced by item.toString() in the new collection else: objects are
     * casted to the expected type T, using castingClass.
     * 
     * @param originalList
     *            the original list of objects
     * @param castingClass
     *            the class specifying the type of T. Technically required, to
     *            call the cast operation.
     * @return the new converted list from the originalList
     * @throws IkatsException
     *             error wrapping the ClassCastException
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
                }
                else {
                    result.add(castingClass.cast(dataItem));
                }

                posCastItem++;
            }
            return result;
        }
        catch (ClassCastException e) {
            throw new IkatsException("TableManager::convertList() failed: on item at position=" + posCastItem, e);
        }

    }

    /**
     * Internal use only: definition of Integer comparator driven by sorting value, typed T.
     * @param mapIndexToSortingValue the map associating each indexe to its sorting value.
     * @return this comparator
     */
    private static Comparator<Integer> getIndexComparator(Map<Integer, String> mapIndexToSortingValue, boolean reverse) {
        // magic comparator: reorder the indexes according to the order of sorting values
        Comparator internalComparator = new NaturalOrderComparator();
        Comparator<Integer> compareRows = new Comparator<Integer>() {

            @Override
            public int compare(Integer index, Integer anotherIndex ) {
                String sortingVal = mapIndexToSortingValue.get(index);
                String anotherSortingVal = mapIndexToSortingValue.get(anotherIndex);
                int res = internalComparator.compare(sortingVal, anotherSortingVal);
                if ( reverse ) res = - res;
                return res;
            }
        };
        return compareRows;
    }
}
