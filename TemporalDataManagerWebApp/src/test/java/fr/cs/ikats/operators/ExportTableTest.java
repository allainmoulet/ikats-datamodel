package fr.cs.ikats.operators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.cs.ikats.operators.ExportTable;
import fr.cs.ikats.operators.ExportTable.Request;
import fr.cs.ikats.table.TableEntity;
import fr.cs.ikats.temporaldata.business.table.Table;
import fr.cs.ikats.temporaldata.business.table.TableInfo;
import fr.cs.ikats.temporaldata.business.table.TableManager;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.IkatsJsonException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ExportTableTest {

    private static final Logger logger = Logger.getLogger(TablesMergeTest.class);

    private static final String TABLE1_CSV = "H1-1;H1-2;H1-3;H1-4;H1-5\n"
            + "H;eight;08;8;1000\n"
            + "E;five;05;5;0101\n"
            + "D;four;04;4;0100\n"
            + "I;nine;09;9;1001\n"
            + "A;one;01;1;0001\n"
            + "G;seven;07;7;0111\n"
            + "F;six;06;6;0110\n"
            + "J;ten;10;10;1010\n"
            + "C;three;03;3;0011\n"
            + "B;two;02;2;0010";

    private static final String TABLE1_SMALLER_CSV = "H1-1;H1-2;H1-3;H1-4;H1-5\n"
            + "H;eight;08;8;1000\n"
            + "E;five;05;5;0101\n"
            + "I;nine;09;9;1001\n"
            + "A;un;01;1;0001\n"
            + "G;seven;07;7;0111\n"
            + "J;ten;10;10;1010\n"
            + "C;three;03;3;0011\n"
            + "B;two;02;2;0010";

    private static final String TABLE2_CSV = "H2-1;H2-2;H2-3;H1-1;H1-2\n"
            + "0;3,14;6,28;F;six\n"
            + "3,14;9,42;6,28;A;one\n"
            + "3,14;15,71;12,57;D;four\n"
            + "3,14;0;0;H;eight\n"
            + "6,28;9,42;9,42;G;seven\n"
            + "9,42;12,57;6,28;B;two\n"
            + "9,42;0;12,57;C;three\n"
            + "9,42;6,28;15,71;I;nine\n"
            + "15,71;15,71;6,28;J;ten\n"
            + "15,71;3,14;0;E;five\n";

    private static final String TABLE2_SMALLER_CSV = "H2-1;H2-2;H2-3;H1-1;H1-2\n"
            + "3,14;9,42;6,28;A;un\n"
            + "3,14;0;0;H;eight\n"
            + "6,28;9,42;9,42;G;seven\n"
            + "9,42;12,57;6,28;B;two\n"
            + "9,42;0;12,57;C;three\n"
            + "9,42;6,28;15,71;I;nine\n"
            + "15,71;15,71;6,28;J;ten\n"
            + "15,71;3,14;0;E;five\n";

    private static final String TABLE3_CSV = "H;eight;08;8;1000\n"
            + "E;five;05;5;0101\n"
            + "D;four;04;4;0100\n"
            + "I;nine;09;9;1001\n"
            + "A;one;01;1;0001\n"
            + "G;seven;07;7;0111\n"
            + "F;six;06;6;0110\n"
            + "J;ten;10;10;1010\n"
            + "C;three;03;3;0011\n"
            + "B;two;02;2;0010\n";

    private static final String TABLE4_CSV = "F;six;0;3,14;6,28\n"
            + "H;eight;3,14;0;0\n"
            + "D;four;3,14;15,71;12,57\n"
            + "A;one;3,14;9,42;6,28\n"
            + "G;seven;6,28;9,42;9,42\n"
            + "I;nine;9,42;6,28;15,71\n"
            + "C;three;9,42;0;12,57\n"
            + "B;two;9,42;12,57;6,28\n"
            + "E;five;15,71;3,14;0\n"
            + "J;ten;15,71;15,71;6,28\n";

    private static final String TABLE5_CSV = "H1-1;H5-2;H5-3;H5-4;H5-5\n"
            + "W;eight;08;8;1000\n"
            + "X;five;05;5;0101\n"
            + "Z;four;04;4;0100\n"
            + "P;nine;09;9;1001\n"
            + "V;one;01;1;0001\n"
            + "S;seven;07;7;0111\n";

    private static TableInfo table1 = null;
    private static TableInfo table1WithRow = null;
    private static TableInfo table2 = null;
    private static TableInfo table2WithRow = null;
    private static TableInfo table1Smaller = null;
    private static TableInfo table2Smaller = null;
    private static TableInfo table3 = null;
    private static TableInfo table4 = null;
    private static TableInfo table5 = null;
    private static TableManager tableManager = new TableManager();

    @BeforeClass
    public static void setUpBeforClass() throws Exception {
        table1 = buildTableFromCSVString("table1", TABLE1_CSV, true, false);
        table1WithRow = buildTableFromCSVString("table1WithRow", TABLE1_CSV, true, true);
        table2 = buildTableFromCSVString("table2", TABLE2_CSV, true, false);
        table2WithRow = buildTableFromCSVString("table2WithRow", TABLE2_CSV, true, true);
        table1Smaller = buildTableFromCSVString("table1Smaller", TABLE1_SMALLER_CSV, true, false);
        table2Smaller = buildTableFromCSVString("table2Smaller", TABLE2_SMALLER_CSV, true, false);
        table3 = buildTableFromCSVString("table3", TABLE3_CSV, false, true);
        table4 = buildTableFromCSVString("table4", TABLE4_CSV, false, false);
        table5 = buildTableFromCSVString("table5", TABLE5_CSV, true, false);
    }


    /**
     * jUnit Test on constructor : Must have a table name
     */
    @Test
    public final void testExportTableConstructorNominal_1() {

        // Build the nominal request
        Request ExportTableRequest = new Request();
        ExportTableRequest.setTableName("");
        ExportTableRequest.setOutputTableName("coco");


        try {
            // Pass it to the constructor
            new ExportTable(ExportTableRequest);
        } catch (IkatsOperatorException e) {
            fail("Error initializing Export Table operator : Need to have a table name");
        }
    }

    /**
     * jUnit Test on constructor : Must have an output csv file name
     */
    @Test
    public final void testExportTableConstructorNominal_2() {

        // Build the nominal request
        Request ExportTableRequest = new Request();
        ExportTableRequest.setTableName("table_1");
        ExportTableRequest.setOutputTableName("");

        try {
            // Pass it to the constructor
            new ExportTable(ExportTableRequest);
        } catch (IkatsOperatorException e) {
            fail("Error initializing Export Table operator : Need to have an output csv file name");
        }
    }

    /**
     * jUnit Test on constructor : Everything is OK
     */
    @Test
    public final void testExportTableConstructorNominal_OK() {

        // Build the nominal request
        Request ExportTableRequest = new Request();
        ExportTableRequest.setTableName("table_1");
        ExportTableRequest.setOutputTableName("output_table_1");


        try {
            // Pass it to the constructor
            new ExportTable(ExportTableRequest);
        } catch (IkatsOperatorException e) {
            fail("Error initializing Export Table operator ");
        }
    }


    /////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Tests jUnit on methods isHeaders
     */

    @Test
    public final void testIsRowHeaderFalse(){
        TableManager tm = new TableManager();
        try {
            // Pass it to the constructor
            HashMap<String,Object> jsonMap = new ExportTable().parseTableInfoToHashMap(tm,table4);
            boolean isRowHeader = new ExportTable().isRowHeader(jsonMap);
            assertEquals(isRowHeader,false);
        } catch (IkatsException e) {
            fail("Error initializing Export Table operator : Need to have an output csv file name");
        }
        catch (java.io.IOException e){
            fail("IO Exception");
        }
    }

    @Test
    public final void testIsRowHeaderTrue(){
        TableManager tm = new TableManager();
        try {
            // Pass it to the constructor
            HashMap<String,Object> jsonMap = new ExportTable().parseTableInfoToHashMap(tm,table2WithRow);
            boolean isRowHeader = new ExportTable().isRowHeader(jsonMap);
            assertEquals(isRowHeader,true);
        } catch (IkatsException e) {
            fail("Error initializing Export Table operator : Need to have an output csv file name");
        }
        catch (java.io.IOException e){
            fail("IO Exception");
        }
    }


    @Test
    public final void testIsColHeaderFalse(){
        TableManager tm = new TableManager();
        try {
            // Pass it to the constructor
            HashMap<String,Object> jsonMap = new ExportTable().parseTableInfoToHashMap(tm,table4);
            boolean isColHeader = new ExportTable().isColHeader(jsonMap);
            assertEquals(isColHeader,false);
        } catch (IkatsException e) {
            fail("Error initializing Export Table operator : Need to have an output csv file name");
        }
        catch (java.io.IOException e){
            fail("IO Exception");
        }
    }

    @Test
    public final void testIsColHeaderTrue(){
        TableManager tm = new TableManager();
        try {
            // Pass it to the constructor
            HashMap<String,Object> jsonMap = new ExportTable().parseTableInfoToHashMap(tm,table2WithRow);
            boolean isColHeader = new ExportTable().isColHeader(jsonMap);
            assertEquals(isColHeader,true);
        } catch (IkatsException e) {
            fail("Error initializing Export Table operator : Need to have an output csv file name");
        }
        catch (java.io.IOException e){
            fail("IO Exception");
        }
    }

    @Test
    public final void testGetContents(){
        TableManager tm = new TableManager();
        try {
            // Pass it to the constructor
            HashMap<String,Object> jSonMap = new ExportTable().parseTableInfoToHashMap(tm,table2WithRow);
            HashMap<String,Object> jSonMapContents = (HashMap<String,Object> ) jSonMap.get("content");
            ArrayList<ArrayList<Object>> jSonMapContentsCells = (ArrayList<ArrayList<Object>>) jSonMapContents.get("cells");

            //Build arrayList of content manually
            String  contentTrue ="3,14;6,28;F;six\n"
                    + "9,42;6,28;A;one\n"
                    + "15,71;12,57;D;four\n"
                    + "0;0;H;eight\n"
                    + "9,42;9,42;G;seven\n"
                    + "12,57;6,28;B;two\n"
                    + "0;12,57;C;three\n"
                    + "6,28;15,71;I;nine\n"
                    + "15,71;6,28;J;ten\n"
                    + "3,14;0;E;five\n";
            TableInfo tabletest = buildTableFromCSVString("tabletest", contentTrue, false, false);
            HashMap<String,Object> jSonTrue = new ExportTable().parseTableInfoToHashMap(tm,tabletest);
            HashMap<String,Object> jSonTrueContents = (HashMap<String,Object> ) jSonTrue.get("content");
            ArrayList<ArrayList<Object>> jSonTrueContentsCells = (ArrayList<ArrayList<Object>>) jSonTrueContents.get("cells");
            //Compare get content and reality
            assertEquals(jSonMapContentsCells,jSonTrueContentsCells);

        } catch (IkatsException e) {
            fail("Error initializing Export Table operator : Need to have an output csv file name");
        }
        catch (java.io.IOException e){
            fail("IO Exception");
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * Add header info
     */

    @Test
    public final void testAdaptHeaderColWithoutRowHeader(){
        TableManager tm = new TableManager();
        try {
            // Pass it to the constructor
            HashMap<String,Object> jSonMap = new ExportTable().parseTableInfoToHashMap(tm,table5);
            HashMap<String,Object> jSonMapContents = (HashMap<String,Object> ) jSonMap.get("content");
            ArrayList<ArrayList<Object>> jSonMapContentsCells = (ArrayList<ArrayList<Object>>) jSonMapContents.get("cells");
            new ExportTable().adaptColumnHeader(jSonMap, jSonMapContentsCells);

            //Build arrayList of content manually
            String  contentTrue = "H1-1;H5-2;H5-3;H5-4;H5-5";
            String[] trueHeaderTab = contentTrue.split(";");
            List<Object> trueHeader = new ArrayList<Object>();
            trueHeader = Arrays.asList(trueHeaderTab);


            //Compare get content and reality
            assertEquals(jSonMapContentsCells.get(0),trueHeader);
            System.out.println(jSonMapContentsCells);

        } catch (IkatsException e) {
            fail("Error initializing Export Table operator : Need to have an output csv file name");
        }
        catch (java.io.IOException e){
            fail("IO Exception");
        }
    }


    @Test
    public final void testAdaptHeaderColWithRowHeader(){
        TableManager tm = new TableManager();
        try {
            // Pass it to the constructor
            HashMap<String,Object> jSonMap = new ExportTable().parseTableInfoToHashMap(tm,table2WithRow);
            HashMap<String,Object> jSonMapContents = (HashMap<String,Object> ) jSonMap.get("content");
            ArrayList<ArrayList<Object>> jSonMapContentsCells = (ArrayList<ArrayList<Object>>) jSonMapContents.get("cells");
            new ExportTable().adaptColumnHeader(jSonMap, jSonMapContentsCells);

            //Build arrayList of content manually
            String  contentTrue = "H2-1;H2-2;H2-3;H1-1;H1-2";
            String[] trueHeaderTab = contentTrue.split(";");
            List<Object> trueHeader = new ArrayList<Object>();
            trueHeader = Arrays.asList(trueHeaderTab);


            //Compare get content and reality
            assertEquals(jSonMapContentsCells.get(0),trueHeader);
            System.out.println(jSonMapContentsCells);

        } catch (IkatsException e) {
            fail("Error initializing Export Table operator : Need to have an output csv file name");
        }
        catch (java.io.IOException e){
            fail("IO Exception");
        }
    }


    @Test
    public final void testAdaptHeaderRowWithoutColHeader(){
        TableManager tm = new TableManager();
        try {
            // Pass it to the constructor
            HashMap<String,Object> jSonMap = new ExportTable().parseTableInfoToHashMap(tm,table3);
            System.out.println(new ExportTable().getRowHeader(jSonMap));

            //Get cell contents
            HashMap<String,Object> jSonMapContents = (HashMap<String,Object> ) jSonMap.get("content");
            ArrayList<ArrayList<Object>> jSonMapContentsCells = (ArrayList<ArrayList<Object>>) jSonMapContents.get("cells");

            //Adapt rows header
            new ExportTable().adaptRowHeader(jSonMap, jSonMapContentsCells,false);
            ArrayList<Object> PutRowHeaders = new ArrayList<Object>();

            //Store header in list
            for (int i =0;i<jSonMapContentsCells.size();i++){
                PutRowHeaders.add(jSonMapContentsCells.get(i).get(0));
            }

            //Row Header reality
            String rowStg = "H;E;D;I;A;G;F;J;C;B";
            String[] trueHeaderTab = rowStg.split(";");
            List<Object> trueHeader = new ArrayList<Object>();
            trueHeader = Arrays.asList(trueHeaderTab);

            //Compare get content and reality
            assertEquals(PutRowHeaders,trueHeader);
            System.out.println(jSonMapContentsCells);

        } catch (IkatsException e) {
            fail("Error initializing Export Table operator : Need to have an output csv file name");
        }
        catch (java.io.IOException e){
            fail("IO Exception");
        }
    }

    @Test
    public final void testAdaptHeaderRowWithColHeader(){
        TableManager tm = new TableManager();
        try {
            // Pass it to the constructor
            HashMap<String,Object> jSonMap = new ExportTable().parseTableInfoToHashMap(tm,table2WithRow);

            //Get cell contents
            HashMap<String,Object> jSonMapContents = (HashMap<String,Object> ) jSonMap.get("content");
            ArrayList<ArrayList<Object>> jSonMapContentsCells = (ArrayList<ArrayList<Object>>) jSonMapContents.get("cells");

            //Add col header
            new ExportTable().adaptColumnHeader(jSonMap, jSonMapContentsCells);


            //Add rows header
            new ExportTable().adaptRowHeader(jSonMap, jSonMapContentsCells,true);

            //Store it in a list
            ArrayList<Object> PutRowHeaders = new ArrayList<Object>();
            for (int i =1;i<jSonMapContentsCells.size();i++){
                PutRowHeaders.add(jSonMapContentsCells.get(i).get(0));
            }

            //Row Header reality
            String rowStg = "0;3,14;3,14;3,14;6,28;9,42;9,42;9,42;15,71;15,71";
            String[] trueHeaderTab = rowStg.split(";");
            List<Object> trueHeader = new ArrayList<Object>();
            trueHeader = Arrays.asList(trueHeaderTab);

            //Compare get row header and reality
            assertEquals(PutRowHeaders,trueHeader);
            System.out.println(jSonMapContentsCells);

            //Check element in (0,0) : First column header value
            assertEquals((String) jSonMapContentsCells.get(0).get(0),"H2-1");

        } catch (IkatsException e) {
            fail("Error initializing Export Table operator : Need to have an output csv file name");
        }
        catch (java.io.IOException e){
            fail("IO Exception");
        }
    }



    ///////////////////////////////////////////////////////////////////////////////

    /**
     * Test Array to string
     */
    @Test
    public void testDoExportWithHeaders(){
        //Build buffer
        String Table2_CSV_NewSeparator = TABLE2_CSV.replaceAll(";"," , ");
        StringBuffer buffer = new StringBuffer(Table2_CSV_NewSeparator);

        try{
            //Result
            StringBuffer res = new ExportTable().doExport(new TableManager(),table2WithRow);
            assertEquals(res,buffer);
        }catch (IkatsException e){

        }catch (java.io.IOException e){

        }

    }

    /////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Function to help building tests
     * @param name
     * @param content
     * @param withColumnsHeader
     * @param withRowsHeader
     * @return
     * @throws IOException
     * @throws IkatsException
     */
    private static TableInfo buildTableFromCSVString(String name, String content,
                                                 boolean withColumnsHeader,
                                                 boolean withRowsHeader)
            throws IOException, IkatsException {

        // Convert the CSV table to expected Table format
        BufferedReader bufReader = new BufferedReader(new StringReader(content));

        String line = null;
        Table table = null;

        if (withColumnsHeader) {
            // Assuming first line contains headers
            line = bufReader.readLine();
            List<String> headersTitle = Arrays.asList(line.split(";"));
            // Replace empty strings with null (that's what do the operator when adding empty headers)
            headersTitle.replaceAll(ht -> ht.isEmpty() ? null : ht);
            table = tableManager.initTable(headersTitle, withRowsHeader);
        } else {
            table = TableManager.initEmptyTable(false, withRowsHeader);
        }

        // Other lines contain data
        while ((line = bufReader.readLine()) != null) {
            List<String> items = new ArrayList<>(Arrays.asList(line.split(";")));
            if (withRowsHeader) {
                // First item considered as row Header
                table.appendRow(items.remove(0), items);
            } else {
                table.appendRow(items);
            }
        }

        table.setName(name);
        table.setDescription("Table '" + name + "' description created for tests");
        table.setTitle("Table '" + name + "' title");

        TableInfo tableInfo = table.getTableInfo();
        logger.trace("Table " + name + " ready");

        return tableInfo;
    }

}





