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
     * Pattern used in the consistency check of Table name.
     */
    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9_-]+");

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
         * @param handledTable either from an existing TableInfo, for example, the Table
         *                     loaded from a json content or from new TableInfo(), in
         *                     order to initialize it.
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
         * Counts the number of rows in the table. Assumed: shape of Table is
         * consistent.
         *
         * @param includeColumnHeader set to True if the header line shall be counted, false
         *                            otherwise
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
         * @param includeRowHeader set to True if the header column shall be counted, false
         *                         otherwise
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
         * Not yet implemented
         *
         * @return
         * @deprecated Unsupported operation.
         */
        public <T> List<T> getColumnsHeaderItems() {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        /**
         * Not yet implemented
         *
         * @return
         * @deprecated Unsupported operation.
         */
        public <T> List<T> getRowsHeaderItems() {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        /**
         * Gets the header size. Internal use only.
         *
         * @param theHeader Header object to get the size for
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
         * @param theHeader Header is the header items container, where is searched
         *                  the value.
         * @param value     Name of the searched header item (item: column or row) to
         *                  get index
         * @return index of header item matching the value. First position is
         * zero. If value is not found -& is returned.
         * @throws IkatsException unexpected error. Examples: null value, or theHeader is
         *                        null, or theHeader.data is null.
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
         * @param columnName name of the selected column: this criterion is in the
         *                   column header.
         * @return the content column below selected column header name.
         * @throws ResourceNotFoundException
         *             when the column is not found
         * @throws IkatsException
         *             unexpected error occured: for exemple ClassCastException
         *             error.
         */
        public <T> List<T> getColumn2(String columnName) throws IkatsException, ResourceNotFoundException {

            return (List<T>) getColumn2(columnName, String.class);
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
        public <T> List<T> getColumn2(String columnName, Class<T> castingClass) throws IkatsException, ResourceNotFoundException {

            try {

                int matchedIndex = this.getIndexColumnHeader(columnName);
                if (matchedIndex < 0) {
                    throw new ResourceNotFoundException(
                            "Unmatched getColumn(): in Columns header: no column named " + columnName + " of table " + this.toString());
                }
                return (List<T>) getColumn2(matchedIndex, castingClass);
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
        public <T> List<T> getColumn2(int index, Class<T> castingClass) throws IkatsException, ResourceNotFoundException {
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
                    int contentIndex = (rowsHeader == null) ? index : index - 1;
                    matchedData = this.getContent().getColumnData(contentIndex);
                }

                boolean isConvertingToString = castingClass == String.class;
                // iterate and cast the value to T ...
                for (Object dataItem : matchedData) {
                   
                    if (isConvertingToString) {
                        result.add(castingClass.cast(dataItem.toString()));
                    }
                    else {
                        result.add(castingClass.cast(dataItem));
                    }

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
         * Gets the column values from the Table.
         * 
         * Note: this getter ignores the links possibly defined on the column.
         * 
         * @param columnName
         *            name of the selected column: this criteria is in the
         *            column header.
         * @return the content column below selected column header name.
         * @throws ResourceNotFoundException when the column is not found
         * @throws IkatsException            unexpected error occurred: for example ClassCastException
         *                                   error.
         */
        public <T> List<T> getColumn(String columnName) throws IkatsException, ResourceNotFoundException {

            try {
                int matchedIndex = this.getIndexColumnHeader(columnName);
                if (matchedIndex < 0) {
                    throw new ResourceNotFoundException(
                            "Unmatched getColumn(): in Columns header: no column named " + columnName + " of table " + this.toString());
                }

                return (List<T>) getColumn(matchedIndex);
            }
            catch (IkatsException e) {
                throw new IkatsException("Failed getColumn(" + columnName + ") in table: " + this.toString(), e);
            }
        }

        /**
         * Gets the column values from the Table. Note: this getter ignores the
         * links possibly defined on the column. <br/>
         * For end-users: when possible, it is advised to use getColumn(String),
         * less confusing than getColumn(int).
         *
         * @param index of selected column. Note: index relative to the global
         *              table. If rows header exists: 0 points to rows header;
         *              otherwise 0 points to first column of this.getContent().
         * @return the selected column values. Note the columns header part is
         * not included. And if Rows header is selected: the first rows
         * header element is not included.
         * @throws IkatsException            row header is undefined or unexpected internal error like
         *                                   ClassCastException, or IndexOutOfBoundException.
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

                } else {
                    // Inside the content part: the column is indexed by
                    int contentIndex = (rowsHeader == null) ? index : index - 1;
                    matchedData = this.getContent().getColumnData(contentIndex);
                }

                // Iterate and cast the value to T ...
                for (Object dataItem : matchedData) {
                    result.add((T) dataItem);
                    posCastItem++;
                }
                return result;
            } catch (IndexOutOfBoundsException iobError) {
                throw new IkatsException("Failed getColumn() in table: IndexOutOfBoundsException: at item position=" + posCastItem
                        + " in column at content index=" + index + " in table " + this.toString(), iobError);
            } catch (ClassCastException typeError) {
                throw new IkatsException("Failed getColumn() in table: cast failed on item at index=" + posCastItem + " in column at content index="
                        + index + " in table " + this.toString(), typeError);
            } catch (IkatsException e) {
                throw new IkatsException("Failed getColumn() in table: " + this.toString(), e);
            }
        }

        /**
         * Gets the row values from this table.
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
        public <T> List<T> getRow2(String rowName) throws IkatsException, ResourceNotFoundException {

            try {

                int matchedIndex = this.getIndexRowHeader(rowName);
                if (matchedIndex < 0) {
                    throw new ResourceNotFoundException(
                            "Unmatched getRow(): in Rows header: no row named " + rowName + " of table " + this.toString());
                }
                return (List<T>) getRow2(matchedIndex, String.class );
            }
            catch (IkatsException e) {
                throw new IkatsException("Failed getColumn(" + rowName + ") in table: " + this.toString(), e);
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
         * @param castingClass class to which the values are converted
         * @return selected row values. Note the row header part is not
         *         included. And if Columns header is selected: first header
         *         element is not included.
         * @throws IkatsException
         *             row header is undefined
         * @throws ResourceNotFoundException
         *             row is not found
         */
        public <T> List<T> getRow2(int index, Class<T> castingClass) throws IkatsException, ResourceNotFoundException {
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
                    int contentIndex = (columnsHeader == null) ? index : index - 1;
                    matchedData = this.getContent().getRowData(contentIndex);
                }

                boolean isConvertingToString = castingClass == String.class;
                // iterate and cast the value to T ...
                for (Object dataItem : matchedData) {
                   
                    if (isConvertingToString) {
                        result.add(castingClass.cast(dataItem.toString()));
                    }
                    else {
                        result.add(castingClass.cast(dataItem));
                    }

                    posCastItem++;
                }
                
                return result;
            }
            catch (ClassCastException typeError) {
                throw new IkatsException("Failed getRow() in table: cast failed on item at index=" + posCastItem + " in row at content index=" + index
                        + " in table " + this.toString(), typeError);
            }
            catch (IkatsException e) {
                throw new IkatsException("Failed getColumn() in table: " + this.toString(), e);
            }
        }

        /**
         * Gets the row values from this table.
         * <p>
         * Note: this getter ignores the links possibly defined in the row.
         *
         * @param rowName name of the selected row: this criterion is in the row
         *                header.
         * @return the content row below selected by the rowName parameter.
         * Note: the selected row does not contain the rows header part.
         * @throws IkatsException            row header is undefined or unexpected internal error like
         *                                   ClassCastException, or IndexOutOfBoundException.
         * @throws ResourceNotFoundException not row is selected by rowName.
         */
        public <T> List<T> getRow(String rowName) throws IkatsException, ResourceNotFoundException {

            try {
                int matchedIndex = this.getIndexRowHeader(rowName);
                if (matchedIndex < 0) {
                    throw new ResourceNotFoundException("No row named " + rowName + " for table " + this.getName());
                }
                return getRow(matchedIndex);
            } catch (IkatsException e) {
                throw new IkatsException("Failed getRow(" + rowName + ") for table: " + this.getName(), e);
            }
        }

        /**
         * Gets the selected row values from this table. Note: this getter
         * ignores the links possibly defined in the row. <br/>
         * For end-users: when possible, it is advised to use getRow(String),
         * less confusing than getRow(int).
         *
         * @param index index of selected row. Note: index is relative to the
         *              whole table. If column header exists: 0 points to
         *              columnHeaders; otherwise 0 points to first row of
         *              this.getContent().
         * @return selected row values. Note the row header part is not
         * included. And if Columns header is selected: first header
         * element is not included.
         * @throws IkatsException            row header is undefined
         * @throws ResourceNotFoundException row is not found
         */
        public <T> List<T> getRow(int index) throws IkatsException, ResourceNotFoundException {
            int posCastItem = 0;
            try {
                if (index < 0) {
                    throw new ResourceNotFoundException("No row content at index=" + index + " for table " + this.getName());
                }

                List<T> result = new ArrayList<>();
                List<Object> matchedData;
                // Review#158227 Because the combo
                // "getColumnsHeader"+"columnsHeader != null" is often used, I
                // suggest to create "this.hasColumnsHeader()"
                // Review#158227 Resp. MBD begin
                // OK keep this => todo with final delivery
                // Review#158227 Resp. MBD end
                Header columnsHeader = this.getColumnsHeader();
                if ((columnsHeader != null) && index == 0) {
                    // Read the columns header and ignore its first element
                    matchedData = new ArrayList<>(columnsHeader.getData());
                    matchedData.remove(0);

                } else {
                    // Inside the content part: the row is indexed by
                    // matchedIndex - 1
                    int contentIndex = (columnsHeader == null) ? index : index - 1;
                    matchedData = this.getContent().getRowData(contentIndex);
                }

                // Iterate and cast the value to T ...
                for (Object dataItem : matchedData) {
                    result.add((T) dataItem);
                    posCastItem++;
                }
                return result;
            } catch (IndexOutOfBoundsException iobError) {
                throw new IkatsException("Failed getRow() in table: IndexOutOfBoundsException: at item position=" + posCastItem
                        + " in row at content index=" + index + " in table " + this.toString(), iobError);
            } catch (ClassCastException typeError) {
                throw new IkatsException("Failed getRow() in table: cast failed on item at index=" + posCastItem + " in row at content index=" + index
                        + " in table " + this.getName(), typeError);
            } catch (IkatsException e) {
                throw new IkatsException("Failed getRow() in table: " + this.getName(), e);
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

        // Review#158227 No description
        // Review#158227 Resp. MBD begin
        // no javadoc required for private (test plan?) or in V2
        // Review#158227 Resp. MBD end
        private List<List<Object>> getContentData() {
            if (this.tableInfo.content == null)
                return null;

            return this.tableInfo.content.cells;
        }

        // Review#158227 No description
        // Review#158227 Resp. MBD begin
        // no javadoc required for private (test plan?) or in V2
        // Review#158227 Resp. MBD end
        private List<List<DataLink>> getContentDataLinks() {
            if (this.tableInfo.content == null)
                return null;

            return this.tableInfo.content.links;
        }

        // Review#158227 No description
        // Review#158227 Resp. MBD begin
        // no javadoc required for private (test plan?) or in V2
        // Review#158227 Resp. MBD end
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
         * @param enabledOnColHeader       enables the links on the column header
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
         * Simply initializes the Header of columns builder: when Object and
         * DataLinks are added later, using {@link Header#addItem}
         *
         * @param startWithTopLeftCorner false will insert null value for the left column header.
         * @param defaultLink            the DataLink providing default values for column links
         * @param manageLinks            true activate the link management
         * @return initialized and attached header, which can be completed using
         * {@link Header#addItem}
         */
        Header initColumnsHeader(boolean startWithTopLeftCorner, DataLink defaultLink, boolean manageLinks) throws IkatsException {
            List<DataLink> headerLinks = manageLinks ? new ArrayList<>() : null;

            return this.initColumnsHeader(startWithTopLeftCorner, defaultLink, new ArrayList<>(), headerLinks);
        }

        // Review#158227 Missing javadoc for param + throw
        // Review#158227 Resp. MBD begin
        // todo V2
        // Review#158227 Resp. MBD end

        /**
         * Initializes the Header of columns builder: when Object and DataLinks
         * are directly set
         *
         * @param startWithTopLeftCorner
         * @param defaultLink
         * @param headerData
         * @param headerLinks
         * @return initialized and attached header, which can be completed using
         * {@link Header#addItem}
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

        // Review#158227 Missing javadoc for param + throw + return
        // Review#158227 Resp. MBD begin
        // todo V2
        // Review#158227 Resp. MBD end

        /**
         * Initialize the column header. Internal purpose.
         * 
         * @param startWithTopLeftCorner
         *            false when a null value is starting the header on the top
         *            left corner.
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

        // Review#158227 Missing javadoc for param + throw
        // Review#158227 Resp. MBD begin
        // todo V2
        // Review#158227 Resp. MBD end

        /**
         * Initialize the column header. Internal purpose.
         * 
         * @param startWithTopLeftCorner
         *            false when a null value is starting the header on the top
         *            left corner.
         * @param defaultLink
         * @param manageLinks
         * @return
         * @throws IkatsException
         */
        Header initRowsHeader(boolean startWithTopLeftCorner, DataLink defaultLink, boolean manageLinks) throws IkatsException {
            List<DataLink> headerLinks = manageLinks ? new ArrayList<>() : null;

            return this.initRowsHeader(startWithTopLeftCorner, defaultLink, new ArrayList<>(), headerLinks);
        }

        // Review#158227 Missing javadoc
        // Review#158227 No description
        // Review#158227 Resp. MBD begin
        // no javadoc required for private (test plan?) or in V2
        // Review#158227 Resp. MBD end
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

        // Review#158227 Missing javadoc for param + throw + description
        // Review#158227 Resp. MBD begin
        // todo V2
        // Review#158227 Resp. MBD end

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

        // Review#158227 Missing javadoc for param + throw + description
        // Review#158227 Resp. MBD begin
        // todo V2
        // Review#158227 Resp. MBD end

        /**
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
         * <p>
         * If a row header is managed, it is sorted the same way.
         *
         * @param columnName name of the sorting criterion.
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
         *            TableElement.
         * @return number of rows in content part, i.e ignoring optional columns
         *         header.
         * @throws IkatsException
         *             consistency/unexpected error occurred
         */
        public <H, T> int appendRow(H rowHeaderData, List<T> rowData) throws IkatsException {

            // Review#158227 Casting to Object seems useless. Can you explain ?
            // Review#158227 Resp. MBD begin
            // required for (List<Object>) => I prefer to leave it, more
            // readable
            // Review#158227 Resp. MBD end
            return appendRowInternal((Object) rowHeaderData, (List<Object>) rowData);
        }

        // Review#158227 Missing javadoc
        // Review#158227 Resp. MBD begin
        // todo V2
        // Review#158227 Resp. MBD end
        private int appendRowInternal(Object rowHeaderData, List<Object> rowData) throws IkatsException {

            if (rowHeaderData != null) {
                if (rowHeaderData instanceof TableElement) {
                    this.getRowsHeader().addItem(((TableElement) rowHeaderData).data, ((TableElement) rowHeaderData).link);
                } else {
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
     * Loads a Table object from the json plain text, using configuration from
     * this.jsonObjectMapper.
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
     * Serializes the TableInfo into equivalent JSON String, using internal
     * configuration of this.jsonObjectMapper.
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
     * Creates and intializes the structure of an empty Table, with columns
     * header enabled, with rows header enabled, without links.
     * 
     * @return created Table, ready to be completed.
     */
    public Table initEmptyTable() {
        TableInfo tableJson = new TableInfo();

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

    // Review#158227 Missing javadoc for param
    // Review#158227 Resp. MBD begin
    // todo V2: link management
    // Review#158227 Resp. MBD end

    /**
     * Creates and initializes the structure of an empty Table,
     * <ul>
     * <li>with columns header enabled,</li>
     * <li>with rows header enabled,</li>
     * <li>with columns header links configured by parameters
     * enabledOnColHeader, defaultPropertyColHeader,</li>
     * <li>with rows header links configured by parameters enabledOnRowHeader,
     * defaultPropertyRowHeader,</li>
     * <li>with content links configured by parameters enabledOnContent,
     * defaultPropertyContent,</li>
     * </ul>
     * 
     * @param enabledOnColHeader
     *            enables the links in column header management
     * @param defaultPropertyColHeader
     * @param enabledOnRowHeader
     * @param defaultPropertyRowHeader
     * @param enabledOnContent
     * @param defaultPropertyContent
     * @return created Table, ready to be completed.
     */
    public Table initEmptyTable(boolean enabledOnColHeader, DataLink defaultPropertyColHeader, boolean enabledOnRowHeader,
                                DataLink defaultPropertyRowHeader, boolean enabledOnContent, DataLink defaultPropertyContent) {

        Table emptyTable = initEmptyTable();
        emptyTable.enableLinks(enabledOnColHeader, defaultPropertyColHeader, enabledOnRowHeader, defaultPropertyRowHeader, enabledOnContent,
                defaultPropertyContent);

        return emptyTable;
    }

    // Review#158227 This method just allow to gain 1 bool setup. Seems not
    // useful. To be deleted
    // Review#158227 Resp. MBD begin
    //    ok why not ... todo V2
    // Review#158227 Resp. MBD end

    /**
     * Initializes a table with defined columnHeaders, without row header,
     * without links.
     *
     * @param columnHeaders list of column headers provided as String
     * @return initialized Table
     * @throws IkatsException
     *             initialization error
     */
    public Table initTable(List<String> columnHeaders) throws IkatsException {
        return initTable(columnHeaders, false);
    }

    /**
     * Initializes a table, with defined columnHeaders, without links, with rows
     * header enabled by parameter withRowHeader.
     *
     * @param columnHeaders  list of columns header values provided as String
     * @param withRowHeader: true activates the row header management. But the rows header
     *                       content is set later.
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
            csvLikeTableH.initRowsHeader(false, null, false);

        // 3: Init Content
        csvLikeTableH.initContent(false, null);

        return csvLikeTableH;
    }

    /**
     * Initializes the Table business resource from the TableInfo JSON-mapping
     * resource.
     *
     * @param tableInfo     the JSON-mapping resource.
     * @param copyTableInfo true demands that returned Table manages a copy of tableInfo.
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
            } else if (dataTables.isEmpty()) {
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
        } catch (SQLException sqle) {
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
     * @throws IkatsJsonException        error encoding the JSON content
     * @throws IkatsDaoException         error checking if resource already exists
     * @throws IkatsDaoConflictException error when a resource with processId=tableName exists
     * @throws InvalidValueException     consistency error found in the name of the table: see
     *                                   TABLE_NAME_PATTERN
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
     * @param tableName the unique identifier of the Table is its name
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
     * @param tableName the name is the identifier of the Table.
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
}
