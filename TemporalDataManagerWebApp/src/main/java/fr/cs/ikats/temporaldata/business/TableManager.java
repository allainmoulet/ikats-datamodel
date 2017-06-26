package fr.cs.ikats.temporaldata.business;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;


import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.process.data.model.ProcessData;
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
 * <li>persistence services (java/JSON)</li>
 * <li>data selection services</li>
 * <li>data modification services</li>
 * </ul>
 */
public class TableManager {

    static Logger logger = Logger.getLogger(TableManager.class);

    /**
     * Pattern used in the consistency check of Table name.
     */
    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9_-]+");

    /**
     * The Table class is the business resource for the 'table' IKATS functional type, it is a wrapper of TableInfo. 
     * The Table class provides end-user services in java world.
     * <br/>
     * Note the difference with TableInfo: TableInfo manages the JSON persistence, and can be used by the REST web services, 
     * involving JSON media type, grouped in the class TableResource. You can get the TableInfo managed thanks to the getter getTableInfo()
     */
    static public class Table {

        private final TableInfo tableInfo;

        /**
         * Create the business Table handling TableInfo.
         * 
         * @param handledTable  either from an
         * existing TableInfo, for exemple, the Table loaded from a json content
         * or from new TableInfo(), in order to initialize it.
         */
        Table(TableInfo handledTable) {
            super();
            this.tableInfo = handledTable;
        }

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
         * Getter
         * 
         * @return the table info
         * @deprecated to be replaced by getTableInfo()
         */
        public TableInfo getTable() {
            return tableInfo;
        }
        
        /**
         * Getter
         * 
         * @return the table  info
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
         * @return
         */
        public int getRowCount(boolean includeColumnHeader) {
            // count rows in Content part:
            List<List<Object>> contentData = getContentData();
            int nbRows = contentData != null ? contentData.size() : 0;
            // if required: add 1 for the column header
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
         * @return
         */
        public int getColumnCount(boolean includeRowHeader) {
            int countCol = 0;
            // count columns in Content part:
            List<List<Object>> contentData = getContentData();
            if ((contentData != null) && !contentData.isEmpty()) {
                countCol = contentData.get(0).size();
            }

            // if required: add 1 for the column header
            if (includeRowHeader && getRowsHeader() != null)
                countCol++;
            return countCol;
        }

        /**
         * Returns the columns Header , if defined, or null
         * 
         * @return
         */
        public Header getColumnsHeader() {
            if (this.tableInfo.headers != null) {
                return this.tableInfo.headers.col;
            }
            else
                return null;
        }

        /**
         * Returns the rows Header , if defined, or null
         * 
         * @return
         */
        public TableInfo.Header getRowsHeader() {
            if (this.tableInfo.headers != null) {
                return this.tableInfo.headers.row;
            }
            else
                return null;
        }

        /**
         * Retrieves the header column index matching the value.
         * 
         * @param value
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
         */
        public <T> List<T> getColumnsHeaderItems() {
            // To do ...
            throw new Error("Not yet implemented");
        }

        /**
         * Not yet implemented
         * 
         * @return
         */
        public <T> List<T> getRowsHeaderItems() {
            throw new Error("Not yet implemented");
        }

        /**
         * Gets the header size. Internal use only.
         * 
         * @param theHeader
         * @return size of the theHeader.data, if defined, otherwise -1
         */
        private int getHeaderSize(Header theHeader) {

            if (theHeader == null || theHeader.data == null)
                return -1;

            return theHeader.data.size();
        }

        /**
         * 
         * @param theHeader
         * @param value
         * @return index of header item matching the value
         * @throws IkatsException
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
         * 
         * Note: this getter ignores the links possibly defined on the column.
         * 
         * @param columnName name of the selected column: this criteria is in the column header.
         * @return the content column below selected column header name.
         * @throws ResourceNotFoundException
         *             when the column is not found
         * @throws IkatsException
         *             unexpected error occured: for exemple ClassCastException
         *             error.
         */
        public <T> List<T> getColumn(String columnName) throws IkatsException, ResourceNotFoundException {
         
            try {

                int matchedIndex = this.getIndexColumnHeader(columnName);
                if (matchedIndex < 0) {
                    throw new ResourceNotFoundException(
                            "Unmatched getColumn(): in Columns header: no column named " + columnName + " of table " + this.toString());
                }
                return getColumn(matchedIndex);
            }
            catch (IkatsException e) {
                throw new IkatsException("Failed getColumn("+ columnName +") in table: " + this.toString(), e);
            }
        }

        /**
         * Gets the column values from the Table.
         * 
         * Note: this getter ignores the links possibly defined on the column.
         * 
         * @param index of selected column. Note: index relative to the global table, including headers, when defined.
         * @return the selected column values. Excluding column optional header value.
         * @throws IkatsException 
         * @throws ResourceNotFoundException when the column is not found
         */
        public <T> List<T> getColumn(int index) throws IkatsException, ResourceNotFoundException {
            int posCastItem = 0;
            try {
                if (index < 0) {
                    throw new ResourceNotFoundException(
                            "Unmatched getColumn(): in Columns header: no column at content index=" + index + " of table " + this.toString());
                }
                
                List<T> result = new ArrayList<>();
                List<Object> matchedData;
                Header rowsHeader = this.getRowsHeader();
                if ((rowsHeader != null) && index == 0) {
                    // Read the rows header and ignore its first element 
                    matchedData = new ArrayList<>(rowsHeader.getData());
                    matchedData.remove(0);

                }
                else {
                    // inside the content part: the column is indexed by 
                    int contentIndex = ( rowsHeader == null ) ? index : index -1;
                    matchedData = this.getContent().getColumnData(contentIndex);
                }

                // iterate and cast the value to T ...
                for (Object dataItem : matchedData) {
                    result.add((T) dataItem);
                    posCastItem++;
                }
                return result;
            }
            catch (ClassCastException typeError) {
                throw new IkatsException("Failed getColumn() in table: cast failed on item at index=" + posCastItem + " in column at content index="
                        + index + " in table " + this.toString(), typeError);
            }
            catch (IkatsException e) {
                throw new IkatsException("Failed getColumn() in table: " + this.toString(), e);
            }
        }
        
        /**
         * Gets the row values from this table/
         * 
         * Note: this getter ignores the links possibly defined on the row.
         * 
         * @param rowName name of the selected row: this criteria is in the row header.
         * @return
         * @throws IkatsException row header is undefined
         * @throws ResourceNotFoundException row is not found
         */
        public <T> List<T> getRow(String rowName) throws IkatsException, ResourceNotFoundException {
            
            try {

                int matchedIndex = this.getIndexRowHeader(rowName);
                if (matchedIndex < 0) {
                    throw new ResourceNotFoundException(
                            "Unmatched getRow(): in Rows header: no row named " + rowName + " of table " + this.toString());
                }
                return getRow(matchedIndex);
            }
            catch (IkatsException e) {
                throw new IkatsException("Failed getColumn("+ rowName +") in table: " + this.toString(), e);
            }
        }
        
        /**
         * Gets the selected row values from this table.
         * 
         * Note: this getter ignores the links possibly defined on the row.
         *
         * @param index content index of selected row. Note: index relative to the whole table.
         * If column header exists: 0 points to columnHeaders; otherwise 0 points to first row of this.getContent().
         * @return selected row values. Note the row header part is not included. And if Columns header is selected: 
         * first header element is not included.
         * @throws IkatsException row header is undefined
         * @throws ResourceNotFoundException row is not found
         */
        public <T> List<T> getRow(int index) throws IkatsException, ResourceNotFoundException {
            int posCastItem = 0;
            try {
                if (index < 0) {
                    throw new ResourceNotFoundException(
                            "Unmatched getRow(): in Rows header: no row at content index=" + index + " of table " + this.toString());
                }
                
                List<T> result = new ArrayList<>();
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
                    int contentIndex = ( columnsHeader == null ) ? index : index -1; 
                    matchedData = this.getContent().getRowData(contentIndex);
                }

                // iterate and cast the value to T ...
                for (Object dataItem : matchedData) {
                    result.add((T) dataItem);
                    posCastItem++;
                }
                return result;
            }
            catch (ClassCastException typeError) {
                throw new IkatsException("Failed getRow() in table: cast failed on item at index=" + posCastItem + " in row at content index="
                        + index + " in table " + this.toString(), typeError);
            }
            catch (IkatsException e) {
                throw new IkatsException("Failed getColumn() in table: " + this.toString(), e);
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

        private List<List<Object>> getContentData() {
            if (this.tableInfo.content == null)
                return null;

            return this.tableInfo.content.cells;
        }

        private List<List<DataLink>> getContentDataLinks() {
            if (this.tableInfo.content == null)
                return null;

            return this.tableInfo.content.links;
        }

        public void disableColumnsHeader() {
            if (this.tableInfo.headers != null)
                this.tableInfo.headers.col = null;
        }

        public void disableRowsHeader() {
            if (this.tableInfo.headers != null)
                this.tableInfo.headers.row = null;
        }

        /**
         * Initializes the links configuration, when some links are required.
         * 
         * @param enabledOnColHeader
         *            enables the links on the column header
         * @param defaultPropertyColHeader
         * @param enabledOnRowHeader
         *            enables the links on the row header
         * @param defaultPropertyRowHeader
         * @param enabledOnContent
         *            enables the links on the content
         * @param defaultPropertyContent
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

        public void setDescription(String description) {
            if (tableInfo.table_desc == null) {
                tableInfo.table_desc = new TableInfo.TableDesc();
            }
            tableInfo.table_desc.desc = description;

        }

        public void setTitle(String title) {
            if (tableInfo.table_desc == null) {
                tableInfo.table_desc = new TableInfo.TableDesc();
            }
            tableInfo.table_desc.title = title;
        }

        public void setName(String name) {
            if (tableInfo.table_desc == null) {
                tableInfo.table_desc = new TableInfo.TableDesc();
            }
            tableInfo.table_desc.name = name;
        }

        /**
         * Simply initializes the Header of columns builder: when Object and
         * DataLinks are added later, using {@link Header#addItem}
         * 
         * @param startWithTopLeftCorner
         * @param defaultLink
         * @param manageLinks
         * @return initialized and attached header, which can be completed using
         *         {@link Header#addItem}
         */
        Header initColumnsHeader(boolean startWithTopLeftCorner, DataLink defaultLink, boolean manageLinks) throws IkatsException {
            List<DataLink> headerLinks = manageLinks ? new ArrayList<>() : null;

            return this.initColumnsHeader(startWithTopLeftCorner, defaultLink, new ArrayList<>(), headerLinks);
        }

        /**
         * Initializes the Header of columns builder: when Object and DataLinks
         * are directly set
         * 
         * @param startWithTopLeftCorner
         * @param defaultLink
         * @param headerData
         * @param headerLinks
         * @return initialized and attached header, which can be completed using
         *         {@link Header#addItem}
         * @throws IkatsException
         */
         Header initColumnsHeader(boolean startWithTopLeftCorner, DataLink defaultLink, List<Object> headerData, List<DataLink> headerLinks)
                throws IkatsException {
            if (defaultLink != null && headerLinks == null) {
                throw new IkatsException("Inconsistency: default link cannot be defined if the links are not manages (headerLinks == null)");
            }
            if (this.tableInfo.headers == null)
                this.tableInfo.headers = new TableHeaders();

            this.tableInfo.headers.col = createHeader(headerData, headerLinks, defaultLink, startWithTopLeftCorner);
            return this.tableInfo.headers.col;
        }

        /**
         * Initialize the column header. Internal purpose.
         * 
         * @param startWithTopLeftCorner false when a null value is starting the header on the top left corner.
         * @param defaultLink
         * @param headerData
         * @param headerLinks
         * @return
         * @throws IkatsException
         */
        Header initRowsHeader(boolean startWithTopLeftCorner, DataLink defaultLink, List<Object> headerData, List<DataLink> headerLinks)
                throws IkatsException {
            if (defaultLink != null && headerLinks == null) {
                throw new IkatsException("Inconsistency: default link cannot be defined if the links are not manages (headerLinks == null)");
            }
            if (this.tableInfo.headers == null)
                this.tableInfo.headers = new TableHeaders();
        
            this.tableInfo.headers.row = createHeader(headerData, headerLinks, defaultLink, startWithTopLeftCorner);
            return this.tableInfo.headers.row;
        }

        /**
         * Initialize the column header.  Internal purpose.
         * 
         * @param startWithTopLeftCorner false when a null value is starting the header on the top left corner.
         * @param defaultLink
         * @param manageLinks
         * @return
         * @throws IkatsException
         */
        Header initRowsHeader(boolean startWithTopLeftCorner, DataLink defaultLink, boolean manageLinks) throws IkatsException {
            List<DataLink> headerLinks = manageLinks ? new ArrayList<>() : null;

            return this.initRowsHeader(startWithTopLeftCorner, defaultLink, new ArrayList<>(), headerLinks);
        }

        private Header createHeader(List<Object> headerData, List<DataLink> headerLinks, DataLink defaultLink, boolean startWithTopLeftCorner) {

            Header initHeader = new Header();
            if (headerData != null) {
                initHeader.data = headerData;
                if (!startWithTopLeftCorner)
                    initHeader.data.add(0, null);
            }
            if (headerLinks != null) {
                initHeader.links = headerLinks;
                if (!startWithTopLeftCorner)
                    initHeader.links.add(0, null);
            }

            initHeader.default_links = defaultLink;

            return initHeader;
        }

        /**
         * Simple init ... todo doc
         * 
         * @param manageLinks
         * @param defaultLink
         * @return
         * @throws IkatsException
         */
        public TableContent initContent(boolean manageLinks, DataLink defaultLink) throws IkatsException {

            return this.tableInfo.content = initContent(new ArrayList<List<Object>>(), manageLinks ? new ArrayList<List<DataLink>>() : null,
                    defaultLink);
        }

        /**
         * 
         * @param cellData
         * @param links
         * @param defaultLink
         * @return
         * @throws IkatsException
         */
        private TableContent initContent(List<List<Object>> cellData, List<List<DataLink>> links, DataLink defaultLink) throws IkatsException {

            if (links == null && defaultLink != null)
                throw new IkatsException("Inconsistency: content cannot have defined default link if links are not managed");

            if (this.tableInfo.content == null) {
                this.tableInfo.content = new TableContent();
            }
            this.tableInfo.content.cells = cellData;
            this.tableInfo.content.links = links;
            this.tableInfo.content.default_links = defaultLink;
            return this.tableInfo.content;

        }

        /**
         * Sorts the rows under the column header, according to one column.
         * 
         * If a row header is managed, it is sorted the same way.
         * 
         * @param columnName
         *            name of the sorting criteria.
         */
        public void sortRowsByColumnValues(String columnName) {
            throw new Error("Not yet implemented");
        }

        /**
         * Adds a new row in the table, without row header information
         * 
         * @param rowData
         *            required list of row data values. Accepted types for T:
         *            immutable Object (String, Double, Boolean ...) or
         *            TableElement.
         * @return
         * @throws IkatsException
         */
        public <T> int appendRow(List<T> rowData) throws IkatsException {

            return appendRowInternal(null, (List<Object>) rowData);
        }

        /**
         * Adds a new row in the table.
         * 
         * @param rowHeaderData
         *            optional row header data. null implies that row header
         *            will be ignored, not completed. Accepted types for H:
         *            immutable Object (String, Double, Boolean ...) or
         *            TableElement.
         * @param rowData
         *            required list of row data values. Accepted types for T:
         *            immutable Object (String, Double, Boolean ...) or
         *            TableElement.
         * @return current number of rows (this.getContent().cells.size())
         * @throws IkatsException
         */
        public <H, T> int appendRow(H rowHeaderData, List<T> rowData) throws IkatsException {

            return appendRowInternal((Object) rowHeaderData, (List<Object>) rowData);
        }

        private int appendRowInternal(Object rowHeaderData, List<Object> rowData) throws IkatsException {

            if (rowHeaderData != null) {
                if (rowHeaderData instanceof TableElement) {
                    this.getRowsHeader().addItem(((TableElement) rowHeaderData).data, ((TableElement) rowHeaderData).link);
                }
                else {
                    this.getRowsHeader().addItem(rowHeaderData, null);
                }
            }
            if ((rowData != null) && rowData.size() > 0)
                this.getContent().addRow(TableElement.encodeElements(rowData));

            return this.getContent().cells.size();
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
     * Loads a Table object from the json plain text.
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
     * 
     * @param businessResource
     * @return
     * @throws IkatsJsonException
     */
    public String serializeToJson(TableInfo businessResource) throws IkatsJsonException {
        try {
            return jsonObjectMapper.writeValueAsString(businessResource);
        }
        catch (Exception e) {
            throw new IkatsJsonException("Failed to serialize Table business resource to the json content", e);
        }
    }

    /**
     * Create and intialize the whole structure of a Table, without links
     */
    public Table initEmptyTable()
    {
        TableInfo tableJson = new TableInfo();
        Table table = new Table( tableJson );
        
        tableJson.table_desc = new TableDesc();
        tableJson.headers = new TableHeaders();
        tableJson.headers.row = new Header();
        tableJson.headers.row.data = new ArrayList<>();
    
        tableJson.headers.col = new Header();
        tableJson.headers.col.data = new ArrayList<>();
       
        tableJson.content = new TableContent();
        tableJson.content.cells = new ArrayList<>();
      
        return new Table(tableJson);
    }
    
    /**
     * Initialize empty table enhanced with links configuration.
     * 
     * @param enabledOnColHeader
     * @param defaultPropertyColHeader
     * @param enabledOnRowHeader
     * @param defaultPropertyRowHeader
     * @param enabledOnContent
     * @param defaultPropertyContent
     * @return
     */
    public Table initEmptyTable(boolean enabledOnColHeader, DataLink defaultPropertyColHeader, boolean enabledOnRowHeader,
            DataLink defaultPropertyRowHeader, boolean enabledOnContent, DataLink defaultPropertyContent) {

        Table emptyTable = initEmptyTable();
        emptyTable.enableLinks(enabledOnColHeader, defaultPropertyColHeader,
                               enabledOnRowHeader, defaultPropertyRowHeader, 
                               enabledOnContent, defaultPropertyContent);

        return emptyTable;
    }
    
    /**
     * Initialize a table, simple, without links, without row header.
     * 
     * @param columnHeaders
     * @return the handler of the table
     * @throws IkatsException
     */
    public Table initTable(List<String> columnHeaders) throws IkatsException {
        return initTable(columnHeaders, false);
    }

    /**
     * Initialize a table, without links.
     * 
     * @param columnHeaders defines the column header cells
     * @param withRowHeader: true activates the row header management. But header cells are defined later
     * @return the handler of the table: ready to use appendRow() for example.
     */
    public Table initTable(List<String> columnHeaders, boolean withRowHeader) throws IkatsException {
        Table csvLikeTableH = initTable(new TableInfo(), false);

        // 1: Init Column header
        // 2: optional Init Row Header
        // 3: Init Content

        csvLikeTableH.initColumnsHeader(true, null, new ArrayList<Object>(columnHeaders), null);

        if (withRowHeader)
            csvLikeTableH.initRowsHeader(false, null, false);

        csvLikeTableH.initContent(false, null);

        return csvLikeTableH;
    }
 
    /**
     * Get the Table business view of a TableInfo. 
     * 
     * Table and TableManager propose public services for end-users
     * 
     * @param tableInfo the tableinfo basic structure is jsonifiable resource.
     * @param copyTableInfo true demands that returned Table manages a copy of tableInfo.
     * @return
     */
    public Table initTable(TableInfo tableInfo, boolean copyTableInfo) {
        if ( copyTableInfo )
        {
            return new Table(new TableInfo( tableInfo));
        }
        else
        {
            return new Table(tableInfo);
        }
    }

    /**
     * Get a Table from process data database
     * 
     * @param tableName
     *            name of the table
     * @return
     * @throws IkatsJsonException
     * @throws IkatsDaoException
     * @throws ResourceNotFoundException
     * @throws SQLException
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

            // extract data to json string
            String jsonString = new String(dataTable.getData().getBytes(1, (int) dataTable.getData().length()));
            // convert to Table type
            TableInfo table = loadFromJson(jsonString);

            logger.info("Table retrieved from db OK : name=" + tableName);
            return table;
        }
        catch (SQLException sqle) {
            throw new IkatsDaoException("Failed to read Table, reading the BLOB of processData with procesId=" + tableName, sqle);
        }

    }

    /**
     * Create a new Table in process data database.
     * 
     * @param tableName
     * @param tableToStore
     * @return ID of created processData
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

        // validate the name consistency
        validateTableName(tableName, "Create Table in database");

        // record the name also inside the Table, so that it will be visible
        // from json.
        initTable(tableToStore, false).setName(tableName);

        byte[] data = serializeToJson(tableToStore).getBytes();

        if (existsInDatabase(tableName))
            throw new IkatsDaoConflictException("Resource already exists ");

        String rid = processDataManager.importProcessData(tableName, tableToStore.table_desc.desc, data);
        logger.info("Table stored Ok in db : " + tableName);

        return rid;
    }
    
    /**
     * Deletes the table from the database.
     * @param tableName the name of the table
     */
    public void deteteFromDatabase(String tableName)
    {
        // The name of the table is in the processId column of table processData
        // => so, we can use directly the removeProcessData(processId) service. 
        
        // No exception raised by this remove
        processDataManager.removeProcessData(tableName);
    }
    
    /**
     * Basically checks if tableName exists as processId in the database.
     * 
     * @param tableName
     * @return
     * @throws IkatsDaoException
     */
    public boolean existsInDatabase(String tableName) throws IkatsDaoException {
        List<ProcessData> collectionProcessData = processDataManager.getProcessData(tableName);

        // temporary solution: check that result is not null
        if (collectionProcessData == null)
            throw new IkatsDaoException("Unexpected Hibernate error: processDataManager.getProcessData(" + tableName + ")");

        return !collectionProcessData.isEmpty();
    }

    /**
     * Gets a table column from a table, reading the table in database.
     * 
     * <ul>
     * <li>calls readFromDatabase(tableName)</li>
     * <li>and then getColumnFromTable(table, columnName)</li>
     * </ul>
     * 
     * Warning: do not repeat this operation if you have several columns to read
     * from 1 table, this would not be efficient.
     * 
     * @param tableName
     * @param columnName
     *            column header value identifying the selection.
     * @return the column as list of values, excluding the header value.
     * @throws IkatsException
     * @throws IkatsDaoException
     * @throws ResourceNotFoundException
     * @throws SQLException
     */
    public <T> List<T> getColumnFromTable(String tableName, String columnName)
            throws IkatsException, IkatsDaoException, ResourceNotFoundException, SQLException {

        TableInfo table = readFromDatabase(tableName);

        Table tableH = initTable(table, false);

        List<T> column = tableH.getColumn(columnName);

        logger.info("Column " + columnName + " retrieved from table : " + tableName);

        return column;
    }

    /**
     * Validate that the table name is well encoded.
     * 
     * @param name
     * @param context
     * @param propertyName
     * @throws InvalidValueException
     */
    public final void validateTableName(String name, String context) throws InvalidValueException {
        String nameStr = name == null ? "null" : name;
        if ((name == null) || !TableManager.TABLE_NAME_PATTERN.matcher(name).matches()) {
            String msg = context + ": invalid name of table resource: " + nameStr;
            logger.error(msg);
            throw new InvalidValueException("Table", "name", TableManager.TABLE_NAME_PATTERN.pattern(), nameStr, null);
        }
    }
}
