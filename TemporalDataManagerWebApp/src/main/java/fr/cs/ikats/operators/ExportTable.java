package fr.cs.ikats.operators;

import java.util.stream.Collectors;

import fr.cs.ikats.common.dao.exception.IkatsDaoMissingResource;
import fr.cs.ikats.temporaldata.business.table.TableInfo;
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
        TableInfo tableToExtract;
        String outputFileName = null;
        String tableNameToExtract = null;
        try {
            //review#826 il faut travailler avec des types Table et non TableInfo afin d'utiliser les accesseurs appropriés
            //review#826 Table tableToExtract = tableManager.initTable(tableManager.readFromDatabase(tableNameToExtract), false);
            //Read the table we want to store
            tableNameToExtract = this.request.tableName;
            tableToExtract = tableManager.readFromDatabase(tableNameToExtract);

        } catch (IkatsDaoMissingResource e) {
            String msg = "Table " + tableNameToExtract + " not found in database";
            throw new IkatsOperatorException(msg, e);
        }

        // do the job : Adapt format of TableInfo
        StringBuffer CSVOutputBuffer = doExport(tableToExtract);

        // then, return it
        logger.info("Table '" + tableNameToExtract + "' is ready to be exported to '" + outputFileName + ".csv");

        return CSVOutputBuffer;
    }


    /**
     * Transform TableInfo to a StringBuffer containing data (CSV format)
     *
     * @param tableToExport : tableInfo we want to save as CSV file
     * @return StringBuffer : Content stored in TableInfo
     */
    public StringBuffer doExport(TableInfo tableToExport) {

        //Check if there are headers for row and col
        boolean isRowHeader = isRowHeader(tableToExport);
        boolean isColumnHeader = isColHeader(tableToExport);

        //Build CSV Format
        //1) Get contents data
        //review#826 Ici tu dupliques les données en mémoire
        //review#826 il faudrait utiliser les accesseurs dédiés pour accéder au contenu de la table
        List<List<Object>> tableInfoContents = new ArrayList(tableToExport.content.cells);
        if (tableInfoContents.size() > 0) { //There is a result
            //2) Add Row and Columns headers if necessary
            if (isColumnHeader) {
                addColumnHeader(tableToExport, tableInfoContents);
            }
            //Add Row Header if there is one
            if (isRowHeader) {
                //There is a row header
                addRowHeader(tableToExport, tableInfoContents, isColumnHeader);
            }
        }
        //3) Now we have all the data in tableInfoContents
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
            //review#826 que fais tu des datalinks ? ils devraient être ignorés
            //review#826si c'est le cas c'est bon (pas le temps d'analyser le code)
            List<String> strings = ithList.stream().map(object -> Objects.toString(object, null)).collect(Collectors.toList());
            //Convert all elements to String, separated by comma with spaces before/after
            //review#826 supprimer les espaces autour du séparateur csv
            String ithListStringCommaSep = String.join(" , ", strings);
            //Add a \n to begin a new line
            ithListStringCommaSep += "\n";
            //Add row to final result
            FormatResult.append(ithListStringCommaSep);
        }
        return FormatResult;
    }


    /**
     * Add column header to the content
     *
     * @param tableInfo     : Used here to get the column Header and add it to the content
     * @param contentsCells : Content data
     */
    //review#826 cet accesseur existe déjà sur le type Table => à supprimer et il faut travailler sur le type Table
    public void addColumnHeader(TableInfo tableInfo, List<List<Object>> contentsCells) {
        //There is a column header -> Get it
        List<Object> columnsHeadersData = new ArrayList(tableInfo.headers.col.data);

        //Then Add it to content
        contentsCells.add(0, columnsHeadersData);
    }


    /**
     * Add row header to the content
     *
     * @param tableInfo      Used here to get the row Header and add it to the content
     * @param contentsCells  Content data
     * @param isColumnHeader : Is there a column header ?
     */
    //review#826 cet accesseur existe déjà sur le type Table => à supprimer et il faut travailler sur le type Table
    public void addRowHeader(TableInfo tableInfo, List<List<Object>> contentsCells, boolean isColumnHeader) {

        //Get row header data
        List<Object> rowsHeadersData = new ArrayList(tableInfo.headers.row.data);

        int begin = 0;
        if (isColumnHeader) {
            //There is a column header : Top Left Corner is stored in Columns Header
            //-> We skip the first element
            begin++;
        }

        //Then Add it to content
        for (int i = begin; i < (rowsHeadersData).size(); i++) {
            //Get ith row  to modify it
            List newRow = new ArrayList(contentsCells.get(i));
            //Add element at the beginning
            newRow.add(0, rowsHeadersData.get(i));
            //Set new row in contents
            contentsCells.set(i, newRow);
        }
    }

    /**
     * Check if there is a row header
     *
     * @param tableInfo : TableInfo which contains all data
     * @return boolean : True is there is a row header
     */
    //review#826 cet accesseur existe déjà sur le type Table => à supprimer et il faut travailler sur le type Table
    public boolean isRowHeader(TableInfo tableInfo) {
        return (tableInfo.headers.row != null);
    }

    /**
     * Check if there is a column header
     *
     * @param tableInfo : TableInfo which contains all data
     * @return boolean : True is there is a row header
     */
    //review#826 cet accesseur existe déjà sur le type Table => à supprimer et il faut travailler sur le type Table
    public boolean isColHeader(TableInfo tableInfo) {
        return (tableInfo.headers.col != null);
    }


    /**
     * Set a request on object
     *
     * @param request
     */
    //review#826 non utilisé => à supprimer
    public void setRequest(Request request) {
        this.request = request;
    }

    /**
     * Set table Manager (New TableManager() in general)
     *
     * @param tableManager
     */
    //review#826 non utilisé => à supprimer
    public void setTableManager(TableManager tableManager) {
        this.tableManager = tableManager;
    }
}
