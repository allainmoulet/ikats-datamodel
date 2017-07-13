package fr.cs.ikats.temporaldata.business;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import fr.cs.ikats.temporaldata.business.TableInfo.DataLink;
import fr.cs.ikats.temporaldata.business.TableInfo.Header;
import fr.cs.ikats.temporaldata.business.TableInfo.TableContent;
import fr.cs.ikats.temporaldata.exception.IkatsException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * The TableInfo is mapping the functional IKATS type 'table', as a JSON
 * resource. TableInfo class can be used as a JSON resource in the Rest
 * services, to be developed in TableResource. <br/>
 * The TableInfo is composed of one TableDesc section, one TableHeaders section
 * and finally one TableContent section. Each section is detailed below. <br/>
 * Note the difference with Table: the business resource Table is a wrapper of
 * TableInfo, managed by TableManager, and providing end-user services in java
 * world.
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
         * <p>
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
         * <p>
         * <br/>
         * Optional: if there is no links, or no mutual information shared by
         * DataLink objects. <br/>
         * Note: if one DataLink redefines default_links.type
         */
        public DataLink default_links;

        /**
         * The data linked by this table: links structured as a list of rows. A
         * row is a list of links. A link is an DataLink.
         * <p>
         * Optional: may be missing, when there is no deeper content to be
         * explored by link.
         */
        public List<List<DataLink>> links;

        /**
         * The public constructor required by jackson ObjectMapper
         */
        public TableContent() {
            super();
        }

        /**
         * Copy constructor: copies all objects defined by content.
         * <p>
         * Restriction: it is assumed that the defined objects under
         * content.cells are immutable: String, Number, BigDecimal, BigInteger,
         * Boolean (...), otherwise you may have side-effects concerning the
         * copy of content.cells.
         *
         * @param content
         *            content to copy
         */
        public TableContent(TableContent content) {

            // Copying cells
            if (content.cells != null) {
                List<List<Object>> copyCells = new ArrayList<List<Object>>();
                for (List<Object> rowOfCells : content.cells) {

                    // Assumed: cells are immutable. See javadoc.
                    copyCells.add(new ArrayList<Object>(rowOfCells));
                }
                this.cells = copyCells;
            }

            // Copying links
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
         *
         * @param index
         *            row index to get
         * @return the row data at index
         * @throws IkatsException
         *             when this.cells is null
         * @throws IndexOutOfBoundsException
         *             when index is out of bound of this.cells
         */
        @JsonIgnore
        public List<Object> getRowData(int index) throws IkatsException, IndexOutOfBoundsException {
            if (cells == null)
                throw new IkatsException("Failed: getSimpleDataRow at index=" + index + " undefined cells");
            return cells.get(index);
        }

        /**
         * Gets from this content the row as list of TableElement, at index,
         * with optional links. Use this method if TableContent is managing
         * links, otherwise it will throw exception.
         *
         * @param index
         *            row index to get
         * @param requiresLinksOrDie
         *            true will activate additional checks on links consistency:
         *            this.links must be defined, and have same size than
         *            this.cells
         * @return the row
         * @throws IkatsException
         *             when this.cells is null or this.links is null or when
         *             cells.size() != links.size()
         * @throws IndexOutOfBoundsException
         *             when index is out of bound of this.cells
         */
        @JsonIgnore
        public List<TableElement> getRowDataWithLink(int index, boolean requiresLinksOrDie) throws IkatsException, IndexOutOfBoundsException {

            String message = "Failed: getRowDataWithLink index=" + index;
            checkLinks(requiresLinksOrDie, message);

            List<TableElement> result = new ArrayList<>();

            List<DataLink> rowDefinedLinks = null;
            int sizeRowDefinedLinks = 0;
            if (links != null && links.size() > index) {
                rowDefinedLinks = links.get(index);
                if (rowDefinedLinks != null)
                    sizeRowDefinedLinks = rowDefinedLinks.size();
            }

            List<Object> rowData = cells.get(index);
            for (int cellPos = 0; cellPos < rowData.size(); cellPos++) {
                DataLink currentLink = null;
                if ((rowDefinedLinks != null) && (cellPos < sizeRowDefinedLinks))
                    currentLink = rowDefinedLinks.get(cellPos);
                TableElement item = new TableElement(rowData.get(cellPos), currentLink);
                result.add(item);
            }
            return result;
        }

        /**
         * Gets from this content the column data, at index, without links
         *
         * @param index
         *            index to get
         * @return the column data at index
         * @throws IkatsException
         *             when this.cells is null
         * @throws IndexOutOfBoundsException
         *             when index is out of bound of at least one of the rows
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
                    throw new IndexOutOfBoundsException(
                            "Failed: getSimpleDataColumn at row=" + posRow + " : row size < (index + 1) with index=" + index);

                simpleColumn.add(row.get(index));

                posRow++;
            }
            return simpleColumn;
        }

        /**
         * Gets from this content the column as list of TableElement, at index,
         * wit optional links. Use this method if TableContent is managing
         * links, otherwise it will throw exception.
         *
         * @param index
         * @param requiresLinksOrdie
         *            true activates the check requiring the links.
         * @return the selected column at index
         * @throws IkatsException
         *             inconsistency error. For instance undefined cells,
         *             unexpected undefined links.
         * @throws IndexOutOfBoundsException
         *             when index is out of bound of at least one of the rows
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
         *
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
         * Activates the links management on TableContent.
         * 
         * Note: if this.cells is not null, also allocates undefined links to
         * null in order to have same dimensions on this.cells and this.links.
         *
         * @param defaultProperties
         *            configuration of the links is providing default values in
         *            order to reduce the volume of JSON.
         */
        public void enableLinks(DataLink defaultProperties) {
            if (links == null) {
                links = new ArrayList<List<DataLink>>();
                if (cells != null && cells.size() > 0) {
                    int nbColumns = 0;
                    int nbRows = cells.size();

                    // assumed: all the rows must have the same size
                    // assumed: this.cells is defined => rows size >=1
                    List<Object> rowOne = cells.get(0);
                    if (rowOne != null && rowOne.size() > 0) {
                        nbColumns = rowOne.size();

                        for (int indexRow = 0; indexRow < nbRows; indexRow++) {
                            List<DataLink> rowLinks = new ArrayList<>();
                            for (int indexCol = 0; indexCol < nbColumns; indexCol++) {
                                rowLinks.add(null);
                            }
                            links.add(rowLinks);
                        }
                    }
                }
                default_links = defaultProperties;
            }
        }

        /**
         * Adds a row using TableElement list: wrapping data values and optional
         * links
         *
         * @param elements
         *            list of elements: wrappers of data and link
         * @return this TableContent: this convenient to chain the modifiers:
         *         this.addRow(...).addRow(...)
         * @throws IkatsException
         *             inconsistency error. Example: trying to add a link in
         *             TableContent which is not managing links.
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
         * Insert the row at index specified by beforeIndex.
         *
         * @param beforeIndex
         * @param elements
         * @return this
         * @throws IkatsException
         *             faied to insert the row
         */
        public TableContent insertRow(int beforeIndex, List<TableElement> elements) throws IkatsException {
            int posRow = 0;
            try {
                List<Object> rowData = new ArrayList<>();
                List<DataLink> rowLinks = links != null ? new ArrayList<>() : null;
                for (TableElement tableElement : elements) {
                    rowData.add(tableElement.data);

                    if (rowLinks != null)
                        rowLinks.add(tableElement.link);
                    posRow++;
                }

                cells.add(beforeIndex, rowData);
                if (rowLinks != null)
                    links.add(beforeIndex, rowLinks);

                return this;
            }
            catch (NullPointerException | IndexOutOfBoundsException | NoSuchElementException e) {
                throw new IkatsException("Failed to insert row at index=" + beforeIndex + " near index=" + posRow, e);
            }
        }

        /**
         * Not yet implemented
         *
         * @return
         * @deprecated Unsupported operation.
         */
        public TableContent deleteRow(int index) throws IkatsException {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        /**
         * Not yet implemented
         *
         * @return
         */
        public TableContent addColumn(List<TableElement> elements) throws IkatsException {

            if (elements.size() != cells.size())
                throw new IkatsException(
                        "Inconsistency error number of rows=" + cells.size() + " is different from the size of added column=" + elements.size());
            boolean manageLinks = links != null;
            if (manageLinks && (elements.size() != links.size()))
                throw new IkatsException("Inconsistency error number of row links=" + links.size()
                        + " is different from the size of added column links=" + elements.size());

            int posRow = 0;
            int posCol = elements.size();
            Iterator<List<Object>> iterateOnRows = cells.iterator();
            Iterator<List<DataLink>> iterateOnRowLinks = manageLinks ? links.iterator() : null;
            for (TableElement tableElement : elements) {
                List<Object> row = iterateOnRows.next();
                row.add(tableElement.data);
                if (manageLinks) {
                    List<DataLink> rowLinks = iterateOnRowLinks.next();
                    rowLinks.add(tableElement.link);
                }
                else if (tableElement.link != null) {
                    throw new IkatsException(
                            "Failed to add new row at " + posRow + " column=" + posCol + ": links not managed but got " + tableElement.link);
                }
                posRow++;
            }

            return this;
        }

        /**
         * Inserts the specified column at the specified position in this list
         * (optional operation). Shifts the columns currently at that position
         * (if any) and any subsequent columns to the right (adds one to their
         * indices).
         * <p>
         * This operation updates as well this.cells and -if not null-
         * this.links
         * 
         * @param beforeIndex
         *            the index defining the location inserted column.
         * @param elements
         *            the list of TableElement defining the inserted column
         * @return this TabkeContent
         * @throws IkatsException
         */
        public TableContent insertColumn(int beforeIndex, List<TableElement> elements) throws IkatsException {
            int posRow = 0;
            try {
                Iterator<List<Object>> iterData = cells.iterator();
                Iterator<List<DataLink>> iterLinks = links != null ? links.iterator() : null;

                for (TableElement tableElement : elements) {
                    List<Object> currentRow = iterData.next();
                    currentRow.add(beforeIndex, tableElement.data);

                    if (iterLinks != null) {
                        List<DataLink> currentRowLinks = iterLinks.next();
                        currentRowLinks.add(beforeIndex, tableElement.link);
                    }
                    posRow++;
                }
                return this;
            }
            catch (NullPointerException | IndexOutOfBoundsException | NoSuchElementException e) {
                throw new IkatsException("Failed to insert column at index=" + beforeIndex + " near row index=" + posRow, e);
            }

        }

        /**
         * Not yet implemented
         *
         * @return
         * @deprecated Unsupported operation.
         */
        public TableContent deleteColumn(int index) throws IkatsException {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        /**
         * Internal use only: creates a new TableContent.
         * 
         * @param cellData
         *            the defined data item.
         * @param links
         *            the defined links items.
         * @param defaultLink
         *            when not null: the DataLink defining the default
         *            properties applicable to content links.
         * @return the created TableContent
         * @throws IkatsException
         *             inconsistency error during initialization.
         */
        static TableContent initContent(List<List<Object>> cellData, List<List<DataLink>> links, DataLink defaultLink) throws IkatsException {

            TableContent content = new TableContent();
            if (links == null && defaultLink != null)
                throw new IkatsException("Inconsistency: content cannot have defined default link if links are not managed");

            content.cells = cellData;
            content.links = links;
            content.default_links = defaultLink;
            return content;

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
         * ID value of linked data (processdata, TS, ...). Note: this can be a
         * complex Object.
         */
        public Object val;
        /**
         * The context defines how to retrieve the linked data. Non exhaustive
         * examples:
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
         * The public constructor required by jackson ObjectMapper
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

        /**
         * Build a new POJO DataLink
         * 
         * @param type
         *            type of DataLink
         * @param val
         *            value of DataLink
         * @param context
         *            context of DataLink
         * @return created DataLink
         */
        final static public DataLink buildLink(String type, Object val, String context) {
            DataLink link = new DataLink();
            link.type = type;
            link.val = val;
            link.context = context;

            return link;

        }
    }

    /**
     * The Header class is instantiated for the column header, or the row
     * header. Header cells are separated for table content cells.
     * <p>
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
         * The public constructor required by jackson ObjectMapper
         */
        public Header() {
            super();
        }

        /**
         * Copy constructor: copies all objects defined by theHeader
         * <p>
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
         *
         * @param data:
         *            header data can be TableElement -including optional link-,
         *            or immutable object.
         * @return
         * @throws IkatsException
         */
        public Header addItem(Object data) throws IkatsException {
            if (data == null) {
                return addItem(null, null);
            }
            else if (data instanceof TableElement) {
                return addItem(((TableElement) data).data, ((TableElement) data).link);
            }
            else if (data instanceof DataLink) {
                return addItem(null, (DataLink) data);
            }
            else {
                return addItem(data, null);
            }
        }

        /**
         * Adds a list of items. For each data item, calls this.addItem(item)
         * 
         * @param data
         * @return
         * @throws IkatsException
         */
        public <T> Header addItems(T... data) throws IkatsException {
            for (int i = 0; i < data.length; i++) {
                this.addItem(data[i]);
            }
            return this;
        }

        /**
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
            // And if links are managed: it is yet possible and required to add
            // null link.
            if (this.links != null)
                this.links.add(link);

            return this;
        }

        /**
         * Gets the this.data: this service is internal to TableManager / Table
         * implementation. You can use getItems(), for external use.
         *
         * @return
         */
        List<Object> getData() {
            return this.data;
        }

        /**
         * Gets the header data as String: each data item is converted to String
         * 
         * @return the computed collection from this.data
         * @throws IkatsException
         *             converting error
         */
        @JsonIgnore
        public List<String> getItems() throws IkatsException {
            return getItems(String.class);
        }

        /**
         * Gets the header data: each data item is converted to T, using
         * toString when T is String or a cast otherwise.
         * 
         * @param castingClass
         * @return the computed collection from this.data
         * @throws IkatsException
         *             converting error
         */
        @JsonIgnore
        public <T> List<T> getItems(Class<T> castingClass) throws IkatsException {
            return TableManager.convertList(this.data, castingClass);
        }

        /**
         * @return
         */
        @JsonIgnore
        public List<TableElement> getDataWithLink() throws IkatsException {
            return TableElement.encodeElements(this.data, this.links);
        }

        /**
         * Activates the links management on this header
         * 
         * @param defaultProperties
         */
        public void enableLinks(DataLink defaultProperties) {
            if (links == null) {

                links = new ArrayList<DataLink>();
                if (data != null && data.size() > 0) {
                    for (int i = 0; i < data.size(); i++) {
                        links.add(null);
                    }
                }
                default_links = defaultProperties;
            }
            // else: ignored !
        }

        /**
         * Internally used by TableManager: initializes the specified Header.
         * 
         * @param headerData
         *            the data part of the header
         * @param headerLinks
         *            the links part of the header
         * @param defaultLink
         *            this link specifies the default link properties, when they
         *            are missing in headerLinks items.
         * @param startWithTopLeftCorner
         *            true when provided values are defined from the top-left
         *            corner.
         * @return created Header.
         */
        static Header createHeader(List<Object> headerData, List<DataLink> headerLinks, DataLink defaultLink, boolean startWithTopLeftCorner) {

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
         * Insert new element in Header: data + optional link
         * 
         * @param insertedIndexColHeader
         *            the position of insertion
         * @param elemH
         *            the table element (data+link) inserted int this Header.
         * @throws IkatsException
         *             inconsistency error detected. Ex. when elemH defines a
         *             link while this.links == null. Ex. when index is out of
         *             bound.
         */
        public void insertItem(int insertedIndexColHeader, TableElement elemH) throws IkatsException {
            try {
                if (elemH.link != null && this.links == null)
                    throw new IkatsException("Inconsistency: insert header item with link, on a header not managing the links");

                data.add(insertedIndexColHeader, elemH.data);
                if (links != null)
                    links.add(insertedIndexColHeader, elemH.link);
            }
            catch (IkatsException | NullPointerException | IndexOutOfBoundsException e) {
                throw new IkatsException("Failed to insert " + elemH.toString() + " at header position=" + insertedIndexColHeader, e);
            }

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
         * The public constructor required by jackson ObjectMapper
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
     * information.
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
         * <p>
         * Specifically used in database storage. Not written in JSON.
         */
        @JsonIgnore
        public String name;

        /**
         * The public constructor required by jackson ObjectMapper.
         */
        public TableDesc() {
            super();
        }

        /**
         * Copy constructor: copies all objects defined by source
         *
         * @param source
         */
        public TableDesc(TableDesc source) {
            this.title = source.title;
            this.desc = source.desc;
            // the copy shall not have same identifier:
            // => do not copy source.name
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
     * The public constructor required by jackson ObjectMapper
     */
    public TableInfo() {
        super();
    }

    /**
     * Internal use: copy a collection of links
     * 
     * @param links
     * @return
     */
    static List<DataLink> copyListOfLinks(List<DataLink> links) {
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
