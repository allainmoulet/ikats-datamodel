/**
 * Copyright 2018-2019 CS Systèmes d'Information
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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


    private static Table table1 = null;
    private static Table table1WithRow = null;
    private static Table table2 = null;
    private static Table table2WithRow = null;
    private static Table table1Smaller = null;
    private static Table table2Smaller = null;
    private static Table table3 = null;
    private static Table table4 = null;
    private static Table table5 = null;


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
    @Test(expected = IkatsOperatorException.class)
    public final void testExportTableConstructorNonNominal_1() throws IkatsOperatorException {

        // Build the nominal request
        Request ExportTableRequest = new Request();
        ExportTableRequest.setTableName(""); // Empty string should not be allowed

        // Pass it to the constructor
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

        try {
            // Pass it to the constructor
            new ExportTable(ExportTableRequest);
        } catch (IkatsOperatorException e) {
            fail("Error initializing Export Table operator ");
        }
    }


    /**
     * Test doExport method
     */
    @Test
    public void testDoExportWithHeaders() throws IkatsException {
        //Build buffer
        TableManager tableManager = new TableManager();
        String Table2_CSV_NewSeparator = TABLE2_CSV.replaceAll(";", ",");

        //Result
        StringBuffer resultExport = new ExportTable().doExport(tableManager.tableInfoToTableEntity(table2WithRow.getTableInfo()));

        //Compare first element without "|"
        assertEquals(resultExport.toString().split(",")[0].replace("|",""), Table2_CSV_NewSeparator.split(",")[0]);

        //Compare others elements
        String[] resultSplitted = resultExport.toString().split(",");
        String[] truncateResult = Arrays.copyOfRange(resultSplitted, 1, resultSplitted.length);
        String[] realResult = Table2_CSV_NewSeparator.split(",");
        String[] truncateRealResult = Arrays.copyOfRange(realResult, 1, realResult.length);
        assertEquals(truncateResult , truncateRealResult);

    }

    /**
     * Test Array to string
     */
    @Test
    public void testDoExportWithoutHeaders() throws IkatsException {

        TableManager tableManager = new TableManager();

        //Build buffer
        String Table4_CSV_NewSeparator = TABLE4_CSV.replaceAll(";", ",");
        StringBuffer buffer = new StringBuffer(Table4_CSV_NewSeparator);

        //Result
        StringBuffer resultExport = new ExportTable().doExport(tableManager.tableInfoToTableEntity(table4.getTableInfo()));
        assertEquals(resultExport.toString(), buffer.toString());

    }

    @Test
    public void testDoExportWithoutRowHeaders() throws IkatsException {

        TableManager tableManager = new TableManager();

        //Build buffer
        String Table5_CSV_NewSeparator = TABLE5_CSV.replaceAll(";", ",");
        StringBuffer buffer = new StringBuffer(Table5_CSV_NewSeparator);

        //Result
        StringBuffer resultExport = new ExportTable().doExport(tableManager.tableInfoToTableEntity(table5.getTableInfo()));
        assertEquals(resultExport.toString(), buffer.toString());

    }

    @Test
    public void testDoExportWithoutColHeaders() throws IkatsException {

        TableManager tableManager = new TableManager();

        //Build buffer example
        String Table3_CSV_NewSeparator = TABLE3_CSV.replaceAll(";", ",");
        StringBuffer buffer = new StringBuffer(Table3_CSV_NewSeparator);

        //Result with doExport method
        StringBuffer resultExport = new ExportTable().doExport(tableManager.tableInfoToTableEntity(table3.getTableInfo()));

        //Compare result and reality
        assertEquals(resultExport.toString(), buffer.toString());

    }


    /**
     * Tests on apply method
     */

    @Test
    public void testApplyWithHeaders() throws Exception {

        TableManager tableManager = new TableManager();

        //Create table in DB
        tableManager.createInDatabase(table2WithRow.getTableInfo());

        // Build the nominal request
        Request request1 = new Request();
        request1.setTableName("table2WithRow");

        //Build Export constructor
        ExportTable exportTableHeader = new ExportTable(request1);

        //Call Apply method
        StringBuffer csvFormatTable2WithRow = exportTableHeader.apply();

        //Prepare data to test equality
        String table2WithRowComma = TABLE2_CSV.replaceAll(";", ",");

        //For now : We have the character "|" when there are two headers (Avoid it)
        //Compare first element without "|"
        assertEquals(csvFormatTable2WithRow.toString().split(",")[0].replace("|",""),table2WithRowComma.split(",")[0]);

        //Compare others elements
        String[] resultSplitted = csvFormatTable2WithRow.toString().split(",");
        String[] truncateResult = Arrays.copyOfRange(resultSplitted, 1, resultSplitted.length);
        String[] realResult = table2WithRowComma.split(",");
        String[] truncateRealResult = Arrays.copyOfRange(realResult, 1, realResult.length);
        assertEquals(truncateResult , truncateRealResult);

        tableManager.deleteFromDatabase("table2WithRow");
    }

    @Test
    public void testApplyWithoutHeaders() throws Exception {

        TableManager tableManager = new TableManager();

        //Create table in DB
        tableManager.createInDatabase(table4.getTableInfo());

        // Build the nominal request
        Request request2 = new Request();
        request2.setTableName("table4");

        //Build Export constructor
        ExportTable exportTableWithoutHeader = new ExportTable(request2);

        //Call Apply method
        StringBuffer csvFormatTable4 = exportTableWithoutHeader.apply();

        //Prepare data to test equality
        String table4WithComma = TABLE4_CSV.replaceAll(";", ",");
        assertEquals(table4WithComma, csvFormatTable4.toString());
        tableManager.deleteFromDatabase("table4");
    }


    @Test
    public void testApplyWithoutRowHeaders() throws Exception {

        TableManager tableManager = new TableManager();

        //Create table in DB
        tableManager.createInDatabase(table5.getTableInfo());

        // Build the nominal request
        Request request3 = new Request();
        request3.setTableName("table5");

        //Build Export constructor
        ExportTable exportTableColHeader = new ExportTable(request3);

        //Call Apply method
        StringBuffer csvFormatTable5WithoutRow = exportTableColHeader.apply();

        //Prepare data to test equality
        String table5WithRowComma = TABLE5_CSV.replaceAll(";", ",");
        assertEquals(table5WithRowComma, csvFormatTable5WithoutRow.toString());
        tableManager.deleteFromDatabase("table5");
    }

    @Test
    public void testApplyWithoutColumnHeaders() throws Exception {


        TableManager tableManager = new TableManager();

        //Create table in DB
        tableManager.createInDatabase(table3.getTableInfo());

        // Build the nominal request
        Request request4 = new Request();
        request4.setTableName("table3");

        //Build Export constructor
        ExportTable exportTableRowHeader = new ExportTable(request4);

        //Call Apply method
        StringBuffer csvFormatTable3WithRow = exportTableRowHeader.apply();

        //Prepare data to test equality
        String table3WithRowComma = TABLE3_CSV.replaceAll(";", ",");
        assertEquals(table3WithRowComma, csvFormatTable3WithRow.toString());

        tableManager.deleteFromDatabase("table3");
    }


    /**
     * Function to help building tests
     *
     * @param name
     * @param content
     * @param withColumnsHeader
     * @param withRowsHeader
     * @return
     * @throws IOException
     * @throws IkatsException
     */
    private static Table buildTableInfoFromCSVString(String name, String content,
                                                     boolean withColumnsHeader,
                                                     boolean withRowsHeader)
            throws IOException, IkatsException {


        TableManager tableManager = new TableManager();

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

        logger.trace("Table " + name + " ready");

        return table;
    }

}
