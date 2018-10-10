package fr.cs.ikats.operators;

import java.util.stream.Collectors;

import fr.cs.ikats.common.dao.exception.IkatsDaoMissingResource;
import fr.cs.ikats.temporaldata.business.table.TableInfo;
import fr.cs.ikats.temporaldata.business.table.Table;
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
        Table tableToExtract;
        String tableNameToExtract = null;
        try { ;
            //Read the table we want to store
            tableNameToExtract = this.request.tableName;
            tableToExtract = tableManager.initTable(tableManager.readFromDatabase(tableNameToExtract), false);

        } catch (IkatsDaoMissingResource e) {
            String msg = "Table " + tableNameToExtract + " not found in database";
            throw new IkatsOperatorException(msg, e);
        }

        // do the job : Adapt format of TableInfo
        StringBuffer CSVOutputBuffer = doExport(tableToExtract);

        // then, return it
        logger.info("Table '" + tableNameToExtract + "' is ready to be exported");

        return CSVOutputBuffer;
    }


    /**
     * Transform TableInfo to a StringBuffer containing data (CSV format)
     *
     * @param tableToExport : tableInfo we want to save as CSV file
     * @return StringBuffer : Content stored in TableInfo
     */
    public StringBuffer doExport(Table tableToExport) throws IkatsException {

        //Check if there are headers for row and col
        boolean isRowHeader = tableToExport.isHandlingRowsHeader();
        boolean isColumnHeader = tableToExport.isHandlingColumnsHeader();

        //Build CSV Format


        // Add Row and Columns headers into contents if necessary
        if (isColumnHeader) {
            tableToExport.insertRow(0,tableToExport.getColumnsHeader().getData());
        }
        //Add Row Header if there is one
        if (isRowHeader) {
            //There is a row header
            tableToExport.insertColumn(0,tableToExport.getRowsHeader().getData());
        }

        //3) Now we have all the data in tableInfoContents
        List<List<Object>> tableInfoContents = tableToExport.getContentData();

        //If there are columns and rows headers : There is a null element in position (0,0) -> remove it
        if(isColumnHeader && isRowHeader){
            tableInfoContents.get(0).remove(0);
        }

        //We just have to parse it into String and add a comma separator + \n at the end of lines
        StringBuffer FormatResult = ListToString(tableInfoContents);

        return FormatResult;
    }

    //////////////////////////////// Additional Methods ////////////////////////////////////////

    /**
     * Convert Array of Array to a stringBuffer like ["Line1\n  ...  \n....Linek...."]
     *
     * @param contentsCells : All contents : Data (+ Row Header + Column Header if necessary)
     * @return StringBuffer containing data stored in ArrayList<ArrayList> as CSV
     */
    public StringBuffer ListToString(List<List<Object>> contentsCells) {

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
