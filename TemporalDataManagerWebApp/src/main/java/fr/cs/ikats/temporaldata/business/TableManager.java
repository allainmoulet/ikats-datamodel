package fr.cs.ikats.temporaldata.business;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import fr.cs.ikats.temporaldata.business.Table.DataLink;
import fr.cs.ikats.temporaldata.business.Table.Header;
import fr.cs.ikats.temporaldata.business.Table.TableContent;
import fr.cs.ikats.temporaldata.business.Table.TableHeaders;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.IkatsJsonException;
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

        public String toString()
        {
            String titleStr = getTitle();
            titleStr = titleStr == null ? "null" : titleStr;
            String descStr = getDescription();
            descStr = descStr == null ? "null" : descStr;
            
            return "Table title=" + titleStr + " desc=" + descStr;
        }
        
        /**
         * Getter
         * @return the table
         */
        public Table getTable() {
            return table;
        }

        public String getDescription() {
            if (table.table_desc != null) {
                return table.table_desc.desc;
            }
            else
                return null;
        }

        public String getTitle() {
            if (table.table_desc != null) {
                return table.table_desc.title;
            }
            else
                return null;
        }

        /**
         * Counts the number of rows in the table.
         * Assumed: shape of Table is consistent.
         * @param includeColumnHeader
         * @return
         */
        public int getRowCount(boolean includeColumnHeader)
        {
            // count rows in Content part:
            List<List<Object>> contentData = getContentData();
            int nbRows = contentData != null ? contentData.size() : 0;
            // if required: add 1 for the column header
            if ( includeColumnHeader && getColumnsHeader() != null ) nbRows++;
            return nbRows;
        }
        
        /**
         * Counts the number of columns in the table.
         * Assumed: shape of Table is consistent: all rows have same number of columns,
         * and the rows header is consistent: its size is equal to  number of columns + 1
         * @param includeRowHeader
         * @return
         */
        public int getColumnCount(boolean includeRowHeader)
        {
            int countCol=0;
            // count columns in Content part:
            List<List<Object>> contentData = getContentData();
            if ( (contentData != null) && ! contentData.isEmpty() )
            {
                countCol= contentData.get(0).size();
            }
            
            // if required: add 1 for the column header
            if ( includeRowHeader && getRowsHeader() != null ) countCol++;
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
         * @param value
         * @return index matched by value, if exists, or -1
         * @throws IkatsException when column header is null
         */
        public int getIndexColumnHeader(String value) throws IkatsException {
            return this.getHeaderIndex(this.getColumnsHeader(), value);
        }
 
        /**
         * Retrieves the header row index matching the value. 
         * @param value
         * @return index matched by value, if exists, or -1
         * @throws IkatsException when row header is null
         */
        public int getIndexRowHeader(String value) throws IkatsException {
            return this.getHeaderIndex(this.getRowsHeader(), value);
        }

        /**
         * Not yet implemented
         * @return
         */
        public <T> List<T> getColumnsHeaderItems()
        {
            throw new Error("Not yet implemented");
        }
        
        /**
         * Not yet implemented
         * @return
         */
        public <T> List<T> getRowsHeaderItems()
        {
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
            
            if ( value == null ) 
                throw new IkatsException("Unexpected column name is null ");
            
            if ( theHeader == null || theHeader.data == null ) 
                throw new IkatsException("Undefined header: impossible to search the index matching value=" + value);
            int position=0;
            
            for (Object headerValue : theHeader.data ) {
                if ( headerValue != null && headerValue.toString().equals( value ) )
                {
                    return position;
                }
                position++;
            }
            return -1;
        }
       
        /**
         * @param columnName
         * @return
         * @throws ResourceNotFoundException 
         * @throws IkatsException 
         */
        protected <T> List<T> getColumnFromTable(String columnName) throws ResourceNotFoundException, IkatsException {
            int posCastItem=0;
            try {
            
                
                int matchedIndex = this.getIndexColumnHeader(columnName);
                if ( matchedIndex < 0 )
                    throw new ResourceNotFoundException("Unmatched getColumnFromTable(): in Columns header: no column named " + columnName );
                
                List<T> result = new ArrayList<>();
                List<Object> matchedData;
                if ( matchedIndex == 0 )
                {
                    // Read the rows header and ignore its first element
                    matchedData = new ArrayList<Object>();
                    matchedData = new ArrayList<>( this.getRowsHeader().getSimpleElements());
                    matchedData.remove(0);
                   
                }
                else
                {
                    // inside the content part: the column is indexed by matchedIndex - 1
                    matchedData = this.getContent().getSimpleDataColumn( matchedIndex -1 );
                }
                
                // iterate and cast the value to T ...
                for (Object dataItem : matchedData) {
                    result.add( (T) dataItem );
                    posCastItem++;
                }
                return result;
            }
            catch(ClassCastException typeError)
            {
                throw new IkatsException("Failed getColumnFromTable() in table: cast failed on item at index=" 
                                         + posCastItem + " in column named " + columnName + " in table "+ this.toString(), typeError);
            }
            catch (IkatsException e) {
                throw new IkatsException("Failed getColumnFromTable() in table: " + this.toString(), e);
            }
        }
        
        /**
         * Getter pf the content part of the table.
         * Beware: content may not be initialized.
         * @return handled content or null
         */
        TableContent getContent()
        {
            return this.table.content;
        }
        
        private List<List<Object>> getContentData()
        {
            if ( this.table.content == null ) return null;
            
            return this.table.content.cells;
        }
        
        private List<List<DataLink>> getContentDataLinks()
        {
            if ( this.table.content == null ) return null;
            
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
        
        private Header createHeader(List<Object> headerData, 
                                    List<DataLink> headerLinks, 
                                    DataLink defaultLink, boolean startWithTopLeftCorner) {
        
            
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
        
            return this.table.content = initContent(new ArrayList<List<Object>>(), manageLinks ? new ArrayList<List<DataLink>>() : null,
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
        
            if (this.table.content == null) {
                this.table.content = new TableContent();
            }
            this.table.content.cells = cellData;
            this.table.content.links = links;
            this.table.content.default_links = defaultLink;
            return this.table.content;
        
        }

        /**
         * 
         * @param rowData
         * @param rowDataLinks
         * @return
         * @throws IkatsException
         */
        public <T> int appendRow(List<T> rowData, List<DataLink> rowDataLinks) throws IkatsException {
            
            // TODO check ... 
            return appendRowInternal(null, null, (List<Object>) rowData, rowDataLinks);
        }
        
        /**
         * 
         * @param rowHeaderData
         * @param rowHeaderLink
         * @param rowData
         * @param rowDataLinks
         * @return
         * @throws IkatsException
         */
        public <T> int  appendRow(String rowHeaderData, DataLink rowHeaderLink, 
                                  List<T> rowData, List<DataLink> rowDataLinks) throws IkatsException {
            
            // TODO check ... 
            return appendRowInternal(rowHeaderData, rowHeaderLink, (List<Object>) rowData, rowDataLinks);
        }

        private int appendRowInternal(String rowHeaderData, 
                             DataLink rowHeaderLink, 
                             List<Object> rowData, List<DataLink> rowDataLinks) throws IkatsException {
            
            if ( rowHeaderData != null) this.getRowsHeader().addItem(rowHeaderData, rowHeaderLink);
            this.getContent().addRow( Table.encodeElements(rowData, rowDataLinks));
            return this.getContent().cells.size();
        }
    
    }

    private ObjectMapper jsonObjectMapper;

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
     * Retrieve the data part of the column matching the columnName.
     * 
     * Note: data part is not including the links: in next version: use the getColumnWithLinksFromTable()
     * @param table
     * @param columnName: criterion used to retrieve good column, with expected name in the header.
     * @return
     * @throws ResourceNotFoundException
     * @throws IkatsException
     */
    public <T> List<T> getColumnFromTable(Table table, String columnName) throws ResourceNotFoundException, IkatsException
    {
        TableHandler myT = new TableHandler( table );
        return myT.getColumnFromTable(columnName);
        
        
    }
    
    /**
     * Initializer of a table CSV-like, simple, without links, without row header.
     * @param columnHeaders
     * @return the handler of the table
     * @throws IkatsException
     */
    public TableHandler initCsvLikeTable( List<String> columnHeaders ) throws IkatsException
    {
        return initCsvLikeTable( columnHeaders, false);
       
    }
 
    /**
     * Initializer of a table CSV-like, without links.
     * @param columnHeaders
     * @param withRowHeader 
     * @return the handler of the table: ready to use appendRow() for example.
     */
    public TableHandler initCsvLikeTable(List<String> columnHeaders, boolean withRowHeader)  throws IkatsException {
        TableHandler csvLikeTableH = getHandler(new Table());
        
        Header colHeader = csvLikeTableH.initColumnsHeader(withRowHeader, null, 
                new ArrayList<Object>(columnHeaders), null);
       
        csvLikeTableH.initContent(false, null);
        
        return csvLikeTableH;
    }

    /**
     * Get the handler, which proposes public services for end-user, or other services for the TableManager.
     * 
     * Note: before using the handler: please check the end-user services of TableManager, with public visibility.
     * @param table
     * @return the handler of the table: ready to use appendRow() for example
     */
    public TableHandler getHandler(Table table) {
        return new TableHandler(table);
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
    
}
