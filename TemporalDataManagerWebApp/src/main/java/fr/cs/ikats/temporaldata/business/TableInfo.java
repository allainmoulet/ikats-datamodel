package fr.cs.ikats.temporaldata.business;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import fr.cs.ikats.temporaldata.exception.IkatsException;

/**
 * The TableInfo is mapping the functional IKATS type 'table', as a JSON resource.
 * TableInfo class can be used as a JSON resource in the Rest services, to be developed in TableResource.
 * <br/>
 * The TableInfo is composed of one TableDesc section, one TableHeaders section and finally one TableContent section. 
 * Each section is detailed below.
 * <br/> 
 * Note the difference with Table: the business resource Table is a wrapper of TableInfo, managed by TableManager, 
 * and providing end-user services in java world.
 * 
 */
public class TableInfo {

    /**
     * The TableContent is the central part of the table, aside TableHeaders,
     * TableDesc.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static public class TableContent {
        /**
         * The data directly contained by this table: cells structured as a list
         * of rows. A row is a list of cell. A cell is an Object, whose
         * effective type is not fixed.
         *
         * Optional: may be missing if links are defined.
         */
        public List<List<Object>> cells;
        /**
         * The default_links just provides mutual properties, shared by all
         * this.links, in order to sum the link information, optimizing the json
         * serialized from Table class. <br/>
         * Example: if all this.links points to timeseries, you may define
         * <ul>
         * <li>default_links.type = 'ts_list'</li>
         * <li>and default_links.context = 'ts'</li>
         * </ul>
         * 
         * <br/>
         * Optional: if there is no links, or no mutual information shared by
         * DataLink objects. <br/>
         * Note: if one DataLink redefines default_links.type
         */
        public DataLink default_links;

        /**
         * The data linked by this table: links structured as a list of rows. A
         * row is a list of links. A link is an DataLink.
         *
         * Optional: may be missing, when there is no deeper content to be
         * explored by link.
         */
        public List<List<DataLink>> links;

        /**
         * The public contructor required by jackson ObjectMapper
         */
        public TableContent() {
            super();
        }

        /**
         * Copy constructor: copies all objects defined by content.
         * 
         * Restriction: it is assumed that the defined objects under
         * content.cells are immutable: String, Number, BigDecimal, BigInteger,
         * Boolean (...), otherwise you may have side-effects concerning the
         * copy of content.cells.
         * 
         * @param content
         */
        public TableContent(TableContent content) {

            if (content.cells != null) {
                List<List<Object>> copyCells = new ArrayList<List<Object>>();
                for (List<Object> rowOfCells : content.cells) {

                    // Assumed: cells are immutable. See javadoc.
                    copyCells.add(new ArrayList<Object>(rowOfCells));
                }
                this.cells = copyCells;
            }

            if (content.links != null) {
                List<List<DataLink>> copyLinks = new ArrayList<List<TableInfo.DataLink>>();
                for (List<DataLink> rowOfLinks : content.links) {
                    List<DataLink> copyRow = copyListOfLinks(rowOfLinks);
                    copyLinks.add(copyRow);
                }
                this.links = copyLinks;
            }
            if (default_links != null)
                this.default_links = content.default_links;

        }

        /**
         * Gets from this content the row data, at index, without links 
         * @param index
         * @return the row data at index
         * @throws IkatsException when this.cells is null
         * @throws IndexOutOfBoundsException when index is out of bound of this.cells
         */
        @JsonIgnore
        public List<Object> getRowData(int index) throws IkatsException, IndexOutOfBoundsException {
            if (cells == null)
                throw new IkatsException("Failed: getSimpleDataRow at index=" + index + " undefined cells");
            return cells.get(index);
        }

        /**
         * Gets from this content the row as list of TableElement, at index, wit optional links.
         * Use this method if TableContent is managing links, otherwise it will throw exception.
         * @param index
         * @return the row
         * @throws IkatsException when this.cells is null or this.links is null or when cells.size() != links.size()
         * @throws IndexOutOfBoundsException when index is out of bound of this.cells
         */
        @JsonIgnore
        public List<TableElement> getRowDataWithLink(int index) throws IkatsException, IndexOutOfBoundsException {
            if (cells == null)
                throw new IkatsException("Failed: getRow at index=" + index + " undefined cells");
            if (links == null)
                throw new IkatsException("Failed: getRowWithLinks at index=" + index + " undefined links");
            if (cells.size() != links.size())
                throw new IkatsException("Failed: getRowWithLinks at index=" + index + " size() different from cells size");

            List<TableElement> result = new ArrayList<>();
            Iterator<DataLink> iterLinks = links.get(index).iterator();
            for (Object data : cells.get(index)) {
                DataLink link = iterLinks.next();
                TableElement item = new TableElement(data, link);
                result.add(item);
            }
            return result;
        }

        /**
         * Gets from this content the column data, at index, without links 
         * @param index
         * @return the column data at index
         * @throws IkatsException when this.cells is null
         * @throws IndexOutOfBoundsException when index is out of bound of at least one of the rows
         */
        @JsonIgnore
        public List<Object> getColumnData(int index) throws IkatsException, IndexOutOfBoundsException {
            List<Object> simpleColumn = new ArrayList<>();

            if (cells == null)
                throw new IkatsException("Failed: getSimpleDataColumn at index=" + index + " undefined cells");
            int posRow = 0;
            for (List<Object> row : cells) {
                if (row == null)
                    throw new IndexOutOfBoundsException("Failed: getSimpleDataColumn at row=" + posRow + " : row is null");

                if (index + 1 > row.size())
                    throw new IndexOutOfBoundsException("Failed: getSimpleDataColumn at row=" + posRow + " : row size < (index + 1) with index=" + index);

                simpleColumn.add(row.get(index));

                posRow++;
            }
            return simpleColumn;
        }
 
        /**
         * Gets from this content the column as list of TableElement, at index, wit optional links.
         * Use this method if TableContent is managing links, otherwise it will throw exception.
         * @param index 
         * @param requiresLinksOrdie true activates the check requiring the links.
         * @return the selected column at index
         * @throws IkatsException inconsistency error. For instance undefined cells, unexpected undefined links.
         * @throws IndexOutOfBoundsException when index is out of bound of at least one of the rows
         */
        @JsonIgnore
        public List<TableElement> getColumnDataWithLink(int index, boolean requiresLinksOrdie) throws IkatsException, IndexOutOfBoundsException {
            int posRow = 0;
            try {
                String message = "Failed: getColumn at index=" + index;
                checkLinks(requiresLinksOrdie, message);

                List<TableElement> resultColumn = new ArrayList<>();
                Iterator<List<DataLink>> iterLinks = (links != null) ? links.iterator() : null;

                for (List<Object> rowOfData : cells) {

                    if (rowOfData == null)
                        throw new IkatsException("Failed: getColumn at row=" + posRow + " : row is null");

                    DataLink addedLink = null;
                    if (links != null) {
                        List<DataLink> rowOfLinks = iterLinks.next();
                        if (rowOfLinks == null)
                            throw new IkatsException("Failed: getColumn at row=" + posRow + " : row of links is null");
                        addedLink = rowOfLinks.get(index);
                    }
                    TableElement item = new TableElement(rowOfData.get(index), addedLink);
                    resultColumn.add(item);

                    posRow++;
                }
                return resultColumn;
            }
            catch (IndexOutOfBoundsException iob) {

                throw new IkatsException("Failed: getColumn at row=" + posRow + " : list size < (index + 1) with index=" + index, iob);
            }
        }

        /**
         * Internal method to be generalized with links management.
         * @param requiresLinksOrdie
         * @param message
         * @throws IkatsException
         */
        private void checkLinks(boolean requiresLinksOrdie, String message) throws IkatsException {
            if (cells == null)
                throw new IkatsException(message + " undefined cells");

            if (links == null && requiresLinksOrdie)
                throw new IkatsException(message + " undefined links");

            if (requiresLinksOrdie && cells.size() != links.size())
                throw new IkatsException(message + " links and cells have different sizes");
        }

        /**
         * Activates the links management on TableContent
         * @param defaultProperties configuration of the links is providing default values in order to reduce the volume of JSON.
         */
        public void enableLinks(DataLink defaultProperties )
        {
            if ( links == null )
            {
                links = new ArrayList<List<DataLink>>();
                default_links = defaultProperties;
            }
        }
        
        /**
         * Adds a row using TableElement list: wrapping data values and optional links
         * @param elements
         * @return
         * @throws IkatsException
         */
        public TableContent addRow(List<TableElement> elements) throws IkatsException {
            int posCol = 0;
            int posRow = cells.size();
            List<Object> rowData = new ArrayList<>();
            boolean manageLinks = links != null;
            List<DataLink> rowLinks = manageLinks ? new ArrayList<>() : null;

            for (TableElement elem : elements) {
                rowData.add(elem.data);
                if (manageLinks) {
                    rowLinks.add(elem.link);
                }
                else if (elem.link != null) {
                    throw new IkatsException("Failed to add new row at " + posRow + " column=" + posCol + ": links not managed but got " + elem.link);
                }
                posCol++;
            }
            this.cells.add(rowData);
            if (manageLinks)
                this.links.add(rowLinks);

            return this;
        }

        /**
         * Not yet implemented
         * @return
         */
        public TableContent insertRow(int beforeIndex, List<TableElement> elements) throws IkatsException { 
            throw new Error("Not yet implemented");
        }
        
        /**
         * Not yet implemented
         * @return
         */
        public TableContent replaceRow(int index, List<TableElement> elements) throws IkatsException { 
            throw new Error("Not yet implemented");
        }
        
        /**
         * Not yet implemented
         * @return
         */
        public TableContent deleteRow(int index) throws IkatsException { 
            throw new Error("Not yet implemented");
        }
        
        /**
         * Not yet implemented
         * @return
         */
        public TableContent addColumn(List<TableElement> elements) throws IkatsException {
            throw new Error("Not yet implemented");
        }

        /**
         * Not yet implemented
         * @return
         */
        public TableContent insertColumn(int beforeIndex, List<TableElement> elements) throws IkatsException {
            throw new Error("Not yet implemented");
        }
        
        /**
         * Not yet implemented
         * @return
         */
        public TableContent replaceColumn(int index, List<TableElement> elements) throws IkatsException { 
            throw new Error("Not yet implemented");
        }
        
        /**
         * Not yet implemented
         * @return
         */
        public TableContent deleteColumn(int index) throws IkatsException { 
            throw new Error("Not yet implemented");
        }

    }

    /**
     * DataLink describes how to get linked data, at a deeper level.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static public class DataLink {

        /**
         * Functional type of the linked data.
         */
        public String type;
        
        /**
         * The parameter value defining the link to the data. For example: the
         * ID value of linked data (processdata, TS, ...). Note: this can be a complexe Object.
         */
        public Object val;
        /**
         * The context defines how to retrieve the linked data. Non exhaustive
         * exemples:
         * <ul>
         * <li>'processdata': when the linked content data is in the processdata
         * database</li>
         * <li>'ts' when the linked data is retrieved from timeseries database
         * </li>
         * <li>'metadata' when the linked data is retrieved from the metadata
         * database</li>
         * </ul>
         */
        public String context;

        /**
         * The public contructor required by jackson ObjectMapper
         */
        public DataLink() {
            super();
        }

        /**
         * @param dataLink
         */
        public DataLink(DataLink dataLink) {
            this.context = dataLink.context;
            this.type = dataLink.type;
            this.val = dataLink.val;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "DataLink [type=" + type + ", val=" + val + ", context=" + context + "]";
        }

    }

    /**
     * The Header class is instanciated for the column header, or the row
     * header. Header cells are separated for table content cells.
     * 
     * One Header groups one vector of header cells.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static public class Header {

        /**
         * The vector of header cells: list of values. There is no pre-defined
         * type. Usually, String is expected in order to define labels.
         */
        public List<Object> data;

        /**
         * default_link contains the default link properties, for the DataLink
         * defined in this.links. See {@link TableContent#default_links}
         */
        public DataLink default_links;

        /**
         * See {@link TableContent#links}
         */
        public List<DataLink> links;

        /**
         * The public contructor required by jackson ObjectMapper
         */
        public Header() {
            super();
        }

        /**
         * Copy constructor: copies all objects defined by theHeader
         * 
         * Restriction: it is assumed that the defined objects under
         * theHeader.data are immutable: OK with String, Number, BigDecimal,
         * BigInteger, Boolean (...), otherwise you may have side-effects
         * concerning the copy of theHeader.data.
         * 
         * @param theHeader
         */ 
        public Header(Header theHeader) {

            if (theHeader.data != null) {
                // Assumed: cells are immutable. See javadoc.
                this.data = new ArrayList<>(theHeader.data);
            }
            if (theHeader.links != null) {
                this.links = TableInfo.copyListOfLinks(theHeader.links);
            }
            this.default_links = theHeader.default_links;

        }

        /**
         * Adds new Header data in the table
         * @param data: header data can be TableElement -including optional link-, or immutable object.
         * @return
         * @throws IkatsException
         */
        public Header addItem(Object data) throws IkatsException {
            if ( data == null ) return addItem( null, null);
            if ( data instanceof TableElement) return addItem( ((TableElement) data).data, ((TableElement) data).link);
            return addItem(data, null);
        }
        /**
         * 
         * @param data
         *            the information added as data: immutable object or null.
         * @param link
         *            the link associated to data: DataLink or null
         * @return this: convenient for chained calls.
         * @throws IkatsException
         */
        public Header addItem(Object data, DataLink link) throws IkatsException {
            if (link != null && this.links == null)
                throw new IkatsException("Inconsistency: add item with link, on a header not managing the links");
            
            this.data.add(data);
            // links are managed <=> this.links is not null
            // And if links are managed: it is yet possible and required to add null link.
            if ( this.links != null ) this.links.add(link);
            
            return this;
        }
        
        /**
         * Gets the data
         * @return
         */
        public List<Object> getData()
        {
            return this.data;
        }
        /**
         * @return
         */
        @JsonIgnore
        public List<TableElement> getDataWithLink() throws IkatsException {
            return TableElement.encodeElements(this.data, this.links);
        }
        
        public void enableLinks(DataLink defaultProperties )
        {
            if ( links == null )
            {
                links = new ArrayList<DataLink>();
                default_links = defaultProperties;
            }
            // else: ignored !
        }
    }

    /**
     * The header section of the table. The headers are separated from the
     * content.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static public class TableHeaders {

        /**
         * The Columns header
         */
        public Header col;
        
        /**
         * The Rows header
         */
        public Header row;

        /**
         * The public contructor required by jackson ObjectMapper
         */
        public TableHeaders() {
            super();
        }

        /**
         * Copy constructor: copies all objects defined by headers
         * 
         * @param headers
         */
        public TableHeaders(TableHeaders headers) {
            if (headers.col != null)
                this.col = new Header(headers.col);
            if (headers.row != null)
                this.row = new Header(headers.row);
        }
    }

    /**
     * The description section of the table. Contains different meta
     * informations.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static public class TableDesc {
        /**
         * The title of the table
         */
        public String title;

        /**
         * The text describing the table
         */
        public String desc;

        /**
         * The name of the table, used as unique identifier.
         * 
         *  Specifically used in database storage. Not written in JSON.
         */
        @JsonIgnore
        public String name;
        
        /**
         * The public contructor required by jackson ObjectMapper.
         */
        public TableDesc() {
            super();
        }

        /**
         * Copy constructor: copies all objects defined by source
         * 
         * @param table_desc
         */
        public TableDesc(TableDesc source) {
            this.title = source.title;
            this.desc = source.desc;
        }

    }

    /**
     * See {@link TableDesc}
     */
    public TableDesc table_desc;

    /**
     * See {@link TableHeaders}
     */
    public TableHeaders headers;

    /**
     * See {@link TableContent}
     */
    public TableContent content;

    /**
     * The public contructor required by jackson ObjectMapper
     */
    public TableInfo() {
        super();
    }

    /**
     * @param links
     * @return
     */
    public static List<DataLink> copyListOfLinks(List<DataLink> links) {
        ArrayList<DataLink> copyLinks = new ArrayList<>();
        for (DataLink dataLink : links) {
            copyLinks.add(new DataLink(dataLink));
        }
        return copyLinks;
    }

    /**
     * Copy constructor: copies all objects defined by source Table.
     * 
     * @param source
     */
    public TableInfo(TableInfo source) {
        this();
        if (source.table_desc != null)
            this.table_desc = new TableDesc(source.table_desc);
        if (source.content != null)
            this.content = new TableContent(source.content);
        if (source.headers != null)
            this.headers = new TableHeaders(source.headers);
    }
}
