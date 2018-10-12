package fr.cs.ikats.operators;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.stream.Collectors;

import fr.cs.ikats.common.dao.exception.IkatsDaoMissingResource;
import fr.cs.ikats.table.TableEntity;
import fr.cs.ikats.temporaldata.business.table.TableManager;
import fr.cs.ikats.temporaldata.exception.IkatsException;

import org.apache.log4j.Logger;

import java.util.*;


/**
 * Class for ExportTable functionality
 * Get IKATS table content and export it as a csv file
 */
public class ExportTable {

    /**
     * First step :
     * Define information to be provided to the {@link ExportTable} operator
     */
    public static class Request {

        private String tableName;

        public Request() {
            // default constructor
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }


        public String getTableName() {
            return this.tableName;
        }

    }

    /**
     * Define attributes for the main class
     * Request : Contains information to be provided to the new operator
     * TableManager : Get and manage values of table
     */
    private Request request;
    private TableManager tableManager;

    // Have a connection with IKATS
    static private final Logger logger = Logger.getLogger(ExportTable.class);

    /**
     * Define the Operator Class : ExportTable
     *
     * @param request : Contains table name and CSV file name
     * @throws IkatsOperatorException : Names need to be not empty
     */
    public ExportTable(Request request) throws IkatsOperatorException {

        // Check the inputs : Must have an output file name
        if (request.tableName == null || request.tableName.length() == 0) {
            throw new IkatsOperatorException("There should be a name for the table you want to export : " + request.tableName);
        }

        this.request = request;
        this.tableManager = new TableManager();
    }

    /**
     * Package private method to be used in tests
     */
    ExportTable() {
        this.tableManager = new TableManager();
    }


    /**
     * Method to call outside
     *
     * @throws IkatsOperatorException
     * @throws IkatsException
     */
    public StringBuffer apply() throws IkatsOperatorException, IkatsException {

        // Retrieve the tables from database
        String tableNameToExtract = null;
        TableEntity tableDataToExport;
        try {
            //Read the table we want to store
            tableNameToExtract = this.request.tableName;
            tableDataToExport = tableManager.readRawFromDatabase(tableNameToExtract);

        } catch (IkatsDaoMissingResource e) {
            String msg = "Table " + tableNameToExtract + " not found in database";
            throw new IkatsOperatorException(msg, e);
        }

        // do the job : Adapt format of TableInfo
        StringBuffer FormatResult = doExport(tableDataToExport);

        // then, return it
        logger.info("Table '" + tableNameToExtract + "' is ready to be exported");

        return FormatResult;
    }


    /**
     * Transform TableInfo to a StringBuffer containing data (CSV format)
     *
     * @param tableToExport : tableInfo we want to save as CSV file
     * @return StringBuffer : Content stored in TableInfo
     */
    public StringBuffer doExport(TableEntity tableToExport) throws IkatsException {

        List<List<Object>> rawData;
        ObjectInputStream ois;
        try {
            ois = new ObjectInputStream(new ByteArrayInputStream(tableToExport.getRawValues()));
            rawData = (List<List<Object>>) ois.readObject();
        } catch (ClassNotFoundException | IOException e) {
            throw new IkatsException("Error raised during table deserialization of raw values. Message: " + e.getMessage(), e);
        }

        //We just have to parse it into String and add a comma separator + \n at the end of lines
        StringBuffer FormatResult = ListToString(rawData);

        return FormatResult;
    }


    /**
     * Convert Array of Array to a stringBuffer like ["Line1\n  ...  \n....Linek...."]
     *
     * @param contentsCells : All contents : Data (+ Row Header + Column Header if necessary)
     * @return StringBuffer containing data stored in ArrayList<ArrayList> as CSV
     */
    private StringBuffer ListToString(List<List<Object>> contentsCells) {

        //Create a StringBuffer to store result
        StringBuffer FormatResult = new StringBuffer();

        //For loop to get all rows and add it to result
        for (int i = 0; i < contentsCells.size(); i++) {
            //Get ith row
            List<Object> ithList = Arrays.asList(contentsCells.get(i)).get(0);
            //Transform all elements into string (also for DataLink)
            List<String> strings = ithList.stream().map(object -> Objects.toString(object, null)).collect(Collectors.toList());
            //Convert all elements to String, separated by comma with spaces before/after
            String ithListStringCommaSep = String.join(",", strings);
            //Add a \n to begin a new line
            ithListStringCommaSep += "\n";
            //Add row to final result
            FormatResult.append(ithListStringCommaSep);
        }
        return FormatResult;
    }

}
