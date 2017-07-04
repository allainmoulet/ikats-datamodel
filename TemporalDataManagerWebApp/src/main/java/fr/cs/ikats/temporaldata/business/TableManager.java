package fr.cs.ikats.temporaldata.business;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Comparator;
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
     * Randomly split table in 2 tables according to repartition rate
     * ex : repartitionRate = 0.6
     * => table1 = 60% of input table
     * => table2 = 40% of input table
     * output = [table1 ; table2]
     *
     * @param table
     * @param repartitionRate
     * @throws
     */
    public List<Table> randomSplitTable(Table table, Float repartitionRate) throws ResourceNotFoundException, IkatsException {
        List<List<Object>> tableContent = table.getContentData();
        if (tableContent == null) {
            throw new IkatsException("Table content is null (" + table.toString() + ")");
        }
        Collections.shuffle(tableContent);

        Table table1 = initEmptyTable();
        Table table2 = initEmptyTable();
        List<Table> result = new ArrayList<>();
        result.add(table1);
        result.add(table2);

        int nbLines = tableContent.size();
        int indexSplit = Math.round(nbLines * repartitionRate);

        Table tableToAppend;
        for (int i = 0; i < nbLines; i++) {
            if (i < indexSplit) {
                tableToAppend = table1;
            } else {
                tableToAppend = table2;
            }
            tableToAppend.appendRow(table.getRow(i));
        }

        return result;

    }

    /**
     * Original input table is randomly splitted into 2 tables according to repartition rate
     * Here values from targetColumnName are equally distributed in each new table
     * <p>
     * ex :
     * 2 classes A, B
     * table : 10 elts => 3 elts A 7 elts B
     * repartitionRate = 0.6
     * => table1 = 60% of input table (6 elts => 4 elts A, 2 elts B)
     * => table2 = 40% of input table (4 elts => 2 elts A, 2 elts B)
     * output = [table1 ; table2]
     *
     * @param table
     * @param targetColumnName
     * @throws
     */
    public List<Table> trainTestSplitTable(Table table, String targetColumnName, Float repartitionRate) throws ResourceNotFoundException, IkatsException {

        // sort table by column 'target'
        table.sortRowsByColumnValues(targetColumnName);

        // extract classes column
        List<Object> classColumnContent = table.getColumn(targetColumnName);

        // building list of indexes where classes change
        List<Integer> indexList = new ArrayList<>();
        Object lastClassValue = classColumnContent.get(0);
        for (int i = 1; i < classColumnContent.size(); i++) {
            if (classColumnContent.get(i) != lastClassValue) {
                indexList.add(i);
                lastClassValue = classColumnContent.get(i);
            }
        }

        // creating tables by class
        int nbLines = classColumnContent.size();
        List<Table> tablesByClass = new ArrayList<>();
        Iterator<Integer> currentIndexClass = indexList.iterator();

        Table tableToAppend = initEmptyTable();
        int nextIndex = currentIndexClass.next();
        for (int i = 0; i < nbLines; i++) {
            if (i >= nextIndex) {
                tablesByClass.add(randomSplitTable(tableToAppend, repartitionRate);
                tableToAppend = initEmptyTable();
                nextIndex = currentIndexClass.next();
            }
            tableToAppend.appendRow(table.getRow(i));
        }

        for (Table tab : tablesByClass) {

        }

        return tablesByClass;
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
     * Internal use only: definition of Integer comparator driven by sorting
     * value, typed T.
     *
     * @param mapIndexToSortingValue
     *            the map associating each indexe to its sorting value.
     * @return this comparator
     */
    static Comparator<Integer> getIndexComparator(Map<Integer, String> mapIndexToSortingValue, boolean reverse) {
        // magic comparator: reorder the indexes according to the order of
        // sorting values
        Comparator internalComparator = new NaturalOrderComparator();
        Comparator<Integer> compareRows = new Comparator<Integer>() {

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
        return compareRows;
    }
}
