package fr.cs.ikats.operators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.cs.ikats.operators.ExportTable.Request;
import fr.cs.ikats.temporaldata.business.table.Table;
import fr.cs.ikats.temporaldata.business.table.TableInfo;
import fr.cs.ikats.temporaldata.business.table.TableManager;
import fr.cs.ikats.temporaldata.exception.IkatsException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ExportTableTest {

    private static final Logger logger = Logger.getLogger(TablesMergeTest.class);

    private static final String TABLE1_CSV =
            "H1-1;H1-2;H1-3;H1-4;H1-5\n"
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
        table1 = buildTableInfoFromCSVString("table1", TABLE1_CSV, true, false);
        table1WithRow = buildTableInfoFromCSVString("table1WithRow", TABLE1_CSV, false, true);
        table2 = buildTableInfoFromCSVString("table2", TABLE2_CSV, true, false);
        table2WithRow = buildTableInfoFromCSVString("table2WithRow", TABLE2_CSV, true, true);
        table1Smaller = buildTableInfoFromCSVString("table1Smaller", TABLE1_SMALLER_CSV, false, true);
        table2Smaller = buildTableInfoFromCSVString("table2Smaller", TABLE2_SMALLER_CSV, true, false);
        table3 = buildTableInfoFromCSVString("table3", TABLE3_CSV, false, true);
        table4 = buildTableInfoFromCSVString("table4", TABLE4_CSV, false, false);
        table5 = buildTableInfoFromCSVString("table5", TABLE5_CSV, true, false);
    }


    /**
     * jUnit Test on constructor : Must have a table name
     */
    @Test(expected=IkatsOperatorException.class)
    public final void testExportTableConstructorNonNominal_1() throws IkatsOperatorException {

        // Build the nominal request
        Request ExportTableRequest = new Request();
        ExportTableRequest.setTableName(""); // Empty string should not be allowed
        ExportTableRequest.setOutputTableName("coco");

        // Pass it to the constructor
        new ExportTable(ExportTableRequest);
    }

    /**
     * jUnit Test on constructor : Must have an output csv file name
     */
    @Test(expected=IkatsOperatorException.class)
    public final void testExportTableConstructorNonNominal_2() throws IkatsOperatorException{

        // Build the nominal request
        Request ExportTableRequest = new Request();
        ExportTableRequest.setTableName("table_1");
        ExportTableRequest.setOutputTableName("");

        //Build constructor
        new ExportTable(ExportTableRequest);
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

    ///////////////////////////////////////////////////////////////////////////////

    /**
     * Test doExport method
     */
    @Test
    public void testDoExportWithHeaders(){
        //Build buffer
        String copyTable = new String(TABLE2_CSV);
        String Table2_CSV_NewSeparator = copyTable.replaceAll(";"," , ");
        StringBuffer buffer = new StringBuffer(Table2_CSV_NewSeparator);

        //Result
        StringBuffer res = new ExportTable().doExport(table2WithRow);
        System.out.println(res);
        System.out.println(buffer);
        assertEquals(res.toString(),buffer.toString());

    }

    /**
     * Test Array to string
     */
    @Test
    public void testDoExportWithoutHeaders(){
        //Build buffer
        String copyTable = new String(TABLE4_CSV);
        String Table4_CSV_NewSeparator = copyTable.replaceAll(";"," , ");
        StringBuffer buffer = new StringBuffer(Table4_CSV_NewSeparator);

        //Result
        StringBuffer res = new ExportTable().doExport(table4);
        System.out.println(res);
        System.out.println(buffer);
        assertEquals(res.toString(),buffer.toString());

    }

    @Test
    public void testDoExportWithoutRowHeaders(){
        //Build buffer
        String copyTable = new String(TABLE5_CSV);
        String Table5_CSV_NewSeparator = copyTable.replaceAll(";"," , ");
        StringBuffer buffer = new StringBuffer(Table5_CSV_NewSeparator);

        //Result
        StringBuffer res = new ExportTable().doExport(table5);
        System.out.println(res);
        System.out.println(buffer);
        assertEquals(res.toString(),buffer.toString());

    }

    @Test
    public void testDoExportWithoutColHeaders(){
        //Build buffer example
        String copyTable = new String(TABLE3_CSV);
        String Table3_CSV_NewSeparator = copyTable.replaceAll(";"," , ");
        StringBuffer buffer = new StringBuffer(Table3_CSV_NewSeparator);

        //Result with doExport method
        StringBuffer res = new ExportTable().doExport(table3);

        //Compare result and reality
        System.out.println(res);
        System.out.println(buffer);
        assertEquals(res.toString(),buffer.toString());

    }


    /////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Tests on apply method
     */

    @Test
    public void testApplyWithHeaders() throws Exception{

        //Create table in DB
        tableManager.createInDatabase(table2WithRow);

        // Build the nominal request
        Request request = new Request();
        request.setOutputTableName("FirstCSVOutputTestWithHeader");
        request.setTableName("table2WithRow");

        //Build Export constructor
        ExportTable exportTable = new ExportTable(request);

        //Call Apply method
        StringBuffer csvFormatTable2WithRow = exportTable.apply();

        //Prepare data to test equality
        String copyTable = new String(TABLE2_CSV);
        String table2WithRowComma = copyTable.replaceAll(";"," , ");
        System.out.println(table2WithRowComma);
        System.out.println(csvFormatTable2WithRow.toString());
        assertEquals(table2WithRowComma, csvFormatTable2WithRow.toString());
        tableManager.deleteFromDatabase("table2WithRow");
    }

    @Test
    public void testApplyWithoutHeaders() throws Exception{

        //Create table in DB
        tableManager.createInDatabase(table4);

        // Build the nominal request
        Request request = new Request();
        request.setOutputTableName("SecondCSVOutputTestWithoutHeaders");
        request.setTableName("table4");

        //Build Export constructor
        ExportTable exportTable = new ExportTable(request);

        //Call Apply method
        StringBuffer csvFormatTable2WithRow = exportTable.apply();

        //Prepare data to test equality
        String copyTable = new String(TABLE4_CSV);
        String table2WithRowComma = copyTable.replaceAll(";"," , ");
        System.out.println(table2WithRowComma);
        System.out.println(csvFormatTable2WithRow.toString());
        assertEquals(table2WithRowComma, csvFormatTable2WithRow.toString());
        tableManager.deleteFromDatabase("table4");
    }


    @Test
    public void testApplyWithoutRowHeaders() throws Exception{

        //Create table in DB
        tableManager.createInDatabase(table5);

        // Build the nominal request
        Request request = new Request();
        request.setOutputTableName("ThirdCSVOutputTestWithoutRowHeaders");
        request.setTableName("table5");

        //Build Export constructor
        ExportTable exportTable = new ExportTable(request);

        //Call Apply method
        StringBuffer csvFormatTable2WithRow = exportTable.apply();

        //Prepare data to test equality
        String copyTable = new String(TABLE5_CSV);
        String table2WithRowComma = copyTable.replaceAll(";"," , ");
        System.out.println(table2WithRowComma);
        System.out.println(csvFormatTable2WithRow.toString());
        assertEquals(table2WithRowComma, csvFormatTable2WithRow.toString());
        tableManager.deleteFromDatabase("table5");
    }

    @Test
    public void testApplyWithoutColumnHeaders() throws Exception{


        //Create table in DB
        System.out.println("*************************************");
        System.out.println("BEFORE STORING IN DB ; ");
        System.out.println(table3.headers.row.data);
        System.out.println(table3.content.cells);
        System.out.println("*************************************");
        tableManager.createInDatabase(table3);

        // Build the nominal request
        Request request = new Request();
        request.setOutputTableName("FourthCSVOutputTestWithoutColumnHeaders");
        request.setTableName("table3");

        //Build Export constructor
        ExportTable exportTable = new ExportTable(request);

        //Call Apply method
        StringBuffer csvFormatTable3WithRow = exportTable.apply();

        //Prepare data to test equality
        String copyTable = new String(TABLE3_CSV);
        String table2WithRowComma = copyTable.replaceAll(";"," , ");
        System.out.println(table2WithRowComma);
        System.out.println(csvFormatTable3WithRow.toString());
        assertEquals(table2WithRowComma.substring(1), csvFormatTable3WithRow.toString());

        tableManager.deleteFromDatabase("table3");
    }




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
    private static TableInfo buildTableInfoFromCSVString(String name, String content,
                                                         boolean withColumnsHeader,
                                                         boolean withRowsHeader)
            throws IOException, IkatsException {

        String copyContent = new String(content);

        // Convert the CSV table to expected Table format
        BufferedReader bufReader = new BufferedReader(new StringReader(copyContent));

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





