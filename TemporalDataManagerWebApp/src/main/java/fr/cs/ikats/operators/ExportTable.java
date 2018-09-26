package fr.cs.ikats.operators;

import java.util.stream.Collectors;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingResource;
import fr.cs.ikats.temporaldata.business.table.TableInfo;
import fr.cs.ikats.temporaldata.business.table.TableManager;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.IkatsJsonException;
import org.apache.log4j.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;


/**
 * Class for operator ExportTable
 * Get IKATS table content and stored it as a csv file
 */
public class ExportTable {

    /**
     * First step :
     * Define information to be provided to the {@link ExportTable} operator
     */
    public static class Request {

        private String tableName;
        private String outputCSVFileName;

        public Request() {
            // default constructor
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public void setOutputTableName(String outputCSVFileName) {
            this.outputCSVFileName = outputCSVFileName;
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
     * @param request
     * @throws IkatsOperatorException
     */
    public ExportTable(Request request) throws IkatsOperatorException {

        // Check the inputs : Must have an output file name
        if (request.outputCSVFileName.length() == 0) {
            throw new IkatsOperatorException("There should be a name for the new CSV file");
        }
        if (request.tableName.length() == 0) {
            throw new IkatsOperatorException("There should be a name for the table you want to save");
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
     * @throws IkatsOperatorException
     * @throws IkatsException
     * @throws java.io.IOException
     */
    public void apply() throws IkatsOperatorException, IkatsException ,java.io.IOException{

        // Retrieve the tables from database
        TableInfo tableToExtract;
        String outputFileName = null;
        String tableNameToExtract = null;
        try {
            //Read the table we want to store
            tableNameToExtract= this.request.tableName;
            tableToExtract = tableManager.readFromDatabase(tableNameToExtract);
            outputFileName = this.request.outputCSVFileName;
        } catch (IkatsDaoMissingResource e) {
            String msg = "Table " + tableNameToExtract+ " not found in database";
            throw new IkatsOperatorException(msg, e);
        }

        // do the job : Adapt format of TableInfo
        StringBuffer CSVOutputBuffer = doExport(tableManager, tableToExtract);

        // then, download it
        /// Launch Navigator Download Function
        logger.info("Table '" + tableNameToExtract + "' is ready to be stored in '" + outputFileName + ".csv");
    }


    /**
     * Transform TableInfo to a StringBuffer containing data (CSV format)
     * @return StringBuffer : Content stored in TableInfo
     * @throws IkatsDaoMissingResource
     * @throws IkatsException
     */
    public StringBuffer doExport(TableManager tableManager, TableInfo tableToExport) throws  IkatsException,java.io.IOException{

        //Build HashMap thanks to JSon contents
        HashMap<String,Object> jSonMap = parseTableInfoToHashMap(tableManager,tableToExport);

        //Check if there is header for row and col
        boolean isRowHeader = isRowHeader(jSonMap);
        boolean isColHeader = isColHeader(jSonMap);

        //Build CSV Format
        //1) Get contents data
        HashMap<String,Object> jSonMapContents = (HashMap<String,Object> ) jSonMap.get("content");
        ArrayList<ArrayList<Object>> jSonMapContentsCells = (ArrayList<ArrayList<Object>>) jSonMapContents.get("cells");

        //2) Add Row and Columns headers if necessary
        if (isColHeader){
            adaptColumnHeader(jSonMap, jSonMapContentsCells);
        }
        if (isRowHeader){
            //There is a row header
            adaptRowHeader(jSonMap, jSonMapContentsCells,isColHeader);
        }

        //3) Now we have all the data in jSonMapContentsCells
        //We just have to parse it into String and add a comma separator + \n at the end of lines
        StringBuffer FormatResult = ArrayListToString(jSonMapContentsCells );

        return FormatResult;
    }

    //////////////////////////////// Additional Methods ////////////////////////////////////////
    /**
     * Convert Array of Array to a stringBuffer like ["Line1\n  ...  \n....Linek...."]
     * @param jSonMapContentsCells
     * @return StringBuffer containing data stored in ArrayList<ArrayList> as CSV
     */
    public StringBuffer ArrayListToString(ArrayList<ArrayList<Object>> jSonMapContentsCells ){

        //Create a StringBuffer to store result
        StringBuffer FormatResult = new StringBuffer();

        //For loop to get all rows and add it to result
        for(int i=0;i<jSonMapContentsCells.size();i++){
            //Get ith row
            List<Object> ithList = Arrays.asList(jSonMapContentsCells.get(i)).get(0);
            //Transform all elements into string
            List<String> strings = ithList.stream().map(object -> Objects.toString(object, null)).collect(Collectors.toList());
            //Convert all elements to String, separated by comma
            String ithListStringCommaSep = String.join(" , ",strings);
            //Add a \n to begin a new line
            ithListStringCommaSep += "\n";
            //Add row to final result
            FormatResult.append(ithListStringCommaSep);
        }
        return FormatResult;
    }



    /**
     * Add column header to the content
     * @param jSonMap
     * @param jSonMapContentsCells
     */
    public void adaptColumnHeader(HashMap<String,Object> jSonMap, ArrayList<ArrayList<Object>> jSonMapContentsCells){
        //There is a column header -> Get it
        ArrayList<Object> columnsHeadersData = getColHeader(jSonMap);

        //Then Add it to content
        jSonMapContentsCells.add(0,columnsHeadersData);
    }


    /**
     * Add row header to the content
     * @param jSonMap
     * @param jSonMapContentsCells
     * @param isColHeader
     */
    public void adaptRowHeader(HashMap<String,Object> jSonMap, ArrayList<ArrayList<Object>> jSonMapContentsCells,boolean isColHeader){

        ArrayList<Object> rowsHeadersData = getRowHeader(jSonMap);

        int begin = 0;
        if(isColHeader){
            begin++;
        }

        //Then Add it to content
        for (int i=begin;i<(rowsHeadersData).size();i++){
            //Get ith row  to modify
            ArrayList newRow = jSonMapContentsCells.get(i);
            //Add element at the beginning
            newRow.add(0,rowsHeadersData.get(i));
            //Set new row in contents
            jSonMapContentsCells.set(i,newRow);
        }

    }

    /**
     * Get the column header
     * Precondition : There is a column header
     * @param jSonMap
     * @return ArrayList containing columns header
     */
    public ArrayList getColHeader(HashMap<String,Object> jSonMap){
        HashMap jSonMapHeader = (HashMap<String,Object> ) jSonMap.get("headers");
        HashMap jSoncolumnsHeaders = (HashMap<String,Object> ) jSonMapHeader.get("col");
        ArrayList columnsHeadersData = (ArrayList<Object> ) jSoncolumnsHeaders.get("data");
        return  columnsHeadersData;
    }

    /**
     * Get the rows header
     * Precondition : There is a row header
     * @param jSonMap
     * @return ArrayList containing rows header
     */
    public ArrayList getRowHeader(HashMap<String,Object> jSonMap){
        //There is a column header -> Get it
        HashMap jSonMapHeader = (HashMap<String,Object>) jSonMap.get("headers");
        HashMap rowsHeaders = (HashMap) jSonMapHeader.get("row");
        ArrayList rowsHeadersData = (ArrayList<Object> ) rowsHeaders.get("data");
        return  rowsHeadersData;
    }

    /**
     * Take tableInfo, extract JSon contents and parse it with HashMap
     * @param tableManager
     * @param tableToExport
     * @return HashMap containing jSon data
     * @throws IkatsException
     * @throws java.io.IOException
     */
    public HashMap<String,Object> parseTableInfoToHashMap(TableManager tableManager,TableInfo tableToExport) throws IkatsJsonException,java.io.IOException{
        //Serialize table content to a json string
        try{
            String jsonTable = tableManager.serializeToJson(tableToExport);
            //Parse JSon string to hashmap (~dictionary)
            HashMap<String,Object> jSonMap = new ObjectMapper().readValue(jsonTable, HashMap.class);
            return jSonMap;
        }catch (IkatsException e){
            throw new IkatsJsonException("Failed to serialize Table business resource to the json content", e);
        }catch (java.io.IOException e){
            throw new java.io.IOException("Failed to parse jSon String to HashMap");
        }
    }

    /**
     * Check if there is a row header
     * @param jsonMap
     * @return boolean : True is there is a row header
     */
    public boolean isRowHeader (HashMap<String,Object> jsonMap){

        boolean isRowHeader = false;
        //If there is col header, look at the key header
        Object jSonMapHeaders = (HashMap<String,Object>)jsonMap.get("headers");
        if (((HashMap) jSonMapHeaders).containsKey("row")){
            isRowHeader = true;
        }
        return isRowHeader;
    }

    /**
     * Check if there is a col header
     * @param jsonMap
     * @return boolean : True is there is a row header
     */
    public boolean isColHeader (HashMap<String,Object> jsonMap){

        boolean isColHeader = false;
        //If there is col header, look at the key header
        Object jSonMapHeaders = (HashMap<String,Object>)jsonMap.get("headers");
        if (((HashMap) jSonMapHeaders).containsKey("col")){
            isColHeader = true;
        }
                return isColHeader;
    }

    /**
     * Set a request on object
     * @param request
     */

    public void setRequest(Request request) {
        this.request = request;
    }

    /**
     * Set table Manager (New in general)
     * @param tableManager
     */
    public void setTableManager(TableManager tableManager) {
        this.tableManager = tableManager;
    }
}
