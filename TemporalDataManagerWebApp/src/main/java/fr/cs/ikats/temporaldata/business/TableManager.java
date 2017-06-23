package fr.cs.ikats.temporaldata.business;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.hsqldb.persist.TableSpaceManagerBlocks;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.process.data.model.ProcessData;
import fr.cs.ikats.temporaldata.business.Table.DataLink;
import fr.cs.ikats.temporaldata.business.Table.Header;
import fr.cs.ikats.temporaldata.business.Table.TableContent;
import fr.cs.ikats.temporaldata.business.Table.TableHeaders;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.IkatsJsonException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.resource.TableResource;

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
     * Wrapper of object Table: this handler provides services.
     * 
     */
    static public class TableHandler {

        private final Table table;

        /**
         * Create the handler - either from an existing Table, for exemple, the
         * Table loaded from a json content - or from new Table(), in order to
         * initialize it.
         * 
         * @param handledTable
         */
        TableHandler(Table handledTable) {
            super();
            this.table = handledTable;
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
         * @return the table
         */
        public Table getTable() {
            return table;
        }

        /**
         * Gets description
         * 
         * @return the title of the Table. null when undefined
         */
        public String getDescription() {
            if (table.table_desc != null) {
                return table.table_desc.desc;
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
            if (table.table_desc != null) {
                return table.table_desc.title;
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
            if (table.table_desc != null) {
                return table.table_desc.name;
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
            if (this.table.headers != null) {
                return this.table.headers.col;
            }
            else
                return null;
        }

        /**
         * Returns the rows Header , if defined, or null
         * 
         * @return
         */
        public Table.Header getRowsHeader() {
            if (this.table.headers != null) {
                return this.table.headers.row;
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
         * @param columnName
         * @return
         * @throws ResourceNotFoundException
         * @throws IkatsException
         */
        public <T> List<T> getColumnFromTable(String columnName) throws IkatsException {
            int posCastItem = 0;
            try {

                int matchedIndex = this.getIndexColumnHeader(columnName);
                if (matchedIndex < 0) {
                    logger.warn("Unmatched getColumnFromTable(): in Columns header: no column named " + columnName + " of table " + this.toString());
                }

                List<T> result = new ArrayList<>();
                List<Object> matchedData;
                if (matchedIndex == 0) {
                    // Read the rows header and ignore its first element
                    matchedData = new ArrayList<Object>();
                    matchedData = new ArrayList<>(this.getRowsHeader().getSimpleElements());
                    matchedData.remove(0);

                }
                else {
                    // inside the content part: the column is indexed by
                    // matchedIndex - 1
                    matchedData = this.getContent().getSimpleDataColumn(matchedIndex - 1);
                }

                // iterate and cast the value to T ...
                for (Object dataItem : matchedData) {
                    result.add((T) dataItem);
                    posCastItem++;
                }
                return result;
            }
            catch (ClassCastException typeError) {
                throw new IkatsException("Failed getColumnFromTable() in table: cast failed on item at index=" + posCastItem + " in column named "
                        + columnName + " in table " + this.toString(), typeError);
            }
            catch (IkatsException e) {
                throw new IkatsException("Failed getColumnFromTable() in table: " + this.toString(), e);
            }
        }

        /**
         * Getter pf the content part of the table. Beware: content may not be
         * initialized.
         * 
         * @return handled content or null
         */
        TableContent getContent() {
            return this.table.content;
        }

        private List<List<Object>> getContentData() {
            if (this.table.content == null)
                return null;

            return this.table.content.cells;
        }

        private List<List<DataLink>> getContentDataLinks() {
            if (this.table.content == null)
                return null;

            return this.table.content.links;
        }

        public void disableColumnsHeader() {
            if (this.table.headers != null)
                this.table.headers.col = null;
        }

        public void disableRowsHeader() {
            if (this.table.headers != null)
                this.table.headers.row = null;
        }
        
        /**
         * Initializes the links configuration, when some links are required.
         * @param enabledOnColHeader enables the links on the column header
         * @param defaultPropertyColHeader
         * @param enabledOnRowHeader enables the links on the row header
         * @param defaultPropertyRowHeader
         * @param enabledOnContent enables the links on the content
         * @param defaultPropertyContent
         */
        public void enableLinks( boolean enabledOnColHeader,
                                 DataLink defaultPropertyColHeader,
                                 boolean enabledOnRowHeader,
                                 DataLink defaultPropertyRowHeader,
                                 boolean enabledOnContent,
                                 DataLink defaultPropertyContent
                                 )
        {
            Header columnsHeader = getColumnsHeader();
            if ( columnsHeader != null && enabledOnColHeader) columnsHeader.enableLinks(defaultPropertyColHeader);
            Header rowsHeader = getRowsHeader();
            if ( rowsHeader != null && enabledOnRowHeader) rowsHeader.enableLinks(defaultPropertyRowHeader);
            TableContent content = getContent();
            if ( content != null && enabledOnContent) content.enableLinks(defaultPropertyContent );
        }

        public void setDescription(String description) {
            if (table.table_desc == null) {
                table.table_desc = new Table.TableDesc();
            }
            table.table_desc.desc = description;

        }

        public void setTitle(String title) {
            if (table.table_desc == null) {
                table.table_desc = new Table.TableDesc();
            }
            table.table_desc.title = title;
        }

        public void setName(String name) {
            if (table.table_desc == null) {
                table.table_desc = new Table.TableDesc();
            }
            table.table_desc.name = name;
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
        public Header initColumnsHeader(boolean startWithTopLeftCorner, DataLink defaultLink, boolean manageLinks) throws IkatsException {
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
        public Header initColumnsHeader(boolean startWithTopLeftCorner, DataLink defaultLink, List<Object> headerData, List<DataLink> headerLinks)
                throws IkatsException {
            if (defaultLink != null && headerLinks == null) {
                throw new IkatsException("Inconsistency: default link cannot be defined if the links are not manages (headerLinks == null)");
            }
            if (this.table.headers == null)
                this.table.headers = new TableHeaders();

            this.table.headers.col = createHeader(headerData, headerLinks, defaultLink, startWithTopLeftCorner);
            return this.table.headers.col;
        }

        /**
         * TODO doc
         * 
         * @param startWithTopLeftCorner
         * @param defaultLink
         * @param manageLinks
         * @return
         * @throws IkatsException
         */
        public Header initRowsHeader(boolean startWithTopLeftCorner, DataLink defaultLink, boolean manageLinks) throws IkatsException {
            List<DataLink> headerLinks = manageLinks ? new ArrayList<>() : null;

            return this.initRowsHeader(startWithTopLeftCorner, defaultLink, new ArrayList<>(), headerLinks);
        }

        /**
         * TODO doc
         * 
         * @param startWithTopLeftCorner
         * @param defaultLink
         * @param headerData
         * @param headerLinks
         * @return
         * @throws IkatsException
         */
        public Header initRowsHeader(boolean startWithTopLeftCorner, DataLink defaultLink, List<Object> headerData, List<DataLink> headerLinks)
                throws IkatsException {
            if (defaultLink != null && headerLinks == null) {
                throw new IkatsException("Inconsistency: default link cannot be defined if the links are not manages (headerLinks == null)");
            }
            if (this.table.headers == null)
                this.table.headers = new TableHeaders();

            this.table.headers.row = createHeader(headerData, headerLinks, defaultLink, startWithTopLeftCorner);
            return this.table.headers.row;
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

            return this.table.content = initContent(new ArrayList<List<Object>>(), manageLinks ? new ArrayList<List<DataLink>>() : null, defaultLink);
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

            if (this.table.content == null) {
                this.table.content = new TableContent();
            }
            this.table.content.cells = cellData;
            this.table.content.links = links;
            this.table.content.default_links = defaultLink;
            return this.table.content;

        }

        /**
         * Sorts the rows under the column header, according to one column.
         * 
         * If a row header is managed, it is sorted the same way.
         * @param columnName name of the sorting criteria.
         */
        public void sortRowsByColumnValues(String columnName)
        {
            throw new Error("Not yet implemented");
        }
        
        /**
         * Adds a new row in the table, without row header information
         * 
         * @param rowData required list of row data values. Accepted types for T: immutable Object (String, Double, Boolean ...) or TableElement.
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
         *            will be ignored, not completed. Accepted types for H: immutable Object (String, Double, Boolean ...) or TableElement.
         * @param rowData
         *            required list of row data values. Accepted types for T: immutable Object (String, Double, Boolean ...) or TableElement.
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
            if ((rowData != null) && rowData.size() > 0) this.getContent().addRow(TableElement.encodeElements(rowData));

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
    public Table loadFromJson(String jsonContent) throws IkatsJsonException {
        try {
            return jsonObjectMapper.readValue(jsonContent, Table.class);
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
    public String serializeToJson(Table businessResource) throws IkatsJsonException {
        try {
            return jsonObjectMapper.writeValueAsString(businessResource);
        }
        catch (Exception e) {
            throw new IkatsJsonException("Failed to serialize Table business resource to the json content", e);
        }
    }

    /**
     * Retrieve the data part of the column matching the columnName.
     * 
     * 
     * Note: data part is not including the links: in next version: use the
     * getColumnWithLinksFromTable()
     * 
     * @param table
     * @param columnName:
     *            criterion used to retrieve good column, with expected name in
     *            the header.
     * @return found List or null when no such column
     * @throws IkatsException
     * @deprecated method: please use directly the method from TableHandler.
     */
    public <T> List<T> getColumnFromTable(Table table, String columnName) throws IkatsException {
        TableHandler myT = new TableHandler(table);
        return myT.getColumnFromTable(columnName);
    }

    /**
     * Initializer of a table CSV-like, simple, without links, without row
     * header.
     * 
     * @param columnHeaders
     * @return the handler of the table
     * @throws IkatsException
     */
    public TableHandler initCsvLikeTable(List<String> columnHeaders) throws IkatsException {
        return initCsvLikeTable(columnHeaders, false);

    }

    /**
     * Initializer of a table CSV-like, without links.
     * 
     * @param columnHeaders
     * @param withRowHeader: true activates the row header management.
     * @return the handler of the table: ready to use appendRow() for example.
     */
    public TableHandler initCsvLikeTable(List<String> columnHeaders, boolean withRowHeader) throws IkatsException {
        TableHandler csvLikeTableH = getHandler(new Table());

        // 1: Init Column header
        // 2: optional Init Row Header
        // 3: Init Content

        csvLikeTableH.initColumnsHeader(withRowHeader, null, new ArrayList<Object>(columnHeaders), null);

        if (withRowHeader)
            csvLikeTableH.initRowsHeader(false, null, false);

        csvLikeTableH.initContent(false, null);

        return csvLikeTableH;
    }

    /**
     * Get the handler, which proposes public services for end-user, or other
     * services for the TableManager.
     * 
     * Note: before using the handler: please check the end-user services of
     * TableManager, with public visibility.
     * 
     * @param table
     * @return the handler of the table: ready to use appendRow() for example
     */
    public TableHandler getHandler(Table table) {
        return new TableHandler(table);
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
    public Table readFromDatabase(String tableName) throws IkatsJsonException, IkatsDaoException, ResourceNotFoundException {

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
            Table table = loadFromJson(jsonString);

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
    public String createInDatabase(String tableName, Table tableToStore)
            throws IkatsJsonException, IkatsDaoException, IkatsDaoConflictException, InvalidValueException {

        // validate the name consistency
        validateTableName(tableName, "Create Table in database");

        // record the name also inside the Table, so that it will be visible
        // from json.
        getHandler(tableToStore).setName(tableName);

        byte[] data = serializeToJson(tableToStore).getBytes();

        if (existsInDatabase(tableName))
            throw new IkatsDaoConflictException("Resource already exists ");

        String rid = processDataManager.importProcessData(tableName, tableToStore.table_desc.desc, data);
        logger.info("Table stored Ok in db : " + tableName);

        return rid;
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

        Table table = readFromDatabase(tableName);

        TableHandler tableH = getHandler(table);
        
        List<T> column = tableH.getColumnFromTable(columnName);

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
