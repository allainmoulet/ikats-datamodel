package fr.cs.ikats.operators;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.cs.ikats.operators.TablesMerge.Request;
import fr.cs.ikats.temporaldata.business.Table;
import fr.cs.ikats.temporaldata.business.TableInfo;
import fr.cs.ikats.temporaldata.business.TableManager;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.IkatsJsonException;

public class TablesMergeTest {

    private static final Logger logger       = Logger.getLogger(TablesMergeTest.class);

    private static final String TABLE1_CSV   = "H1-1;H1-2;H1-3;H1-4;H1-5\n"
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

    private static final String TABLE2_CSV   = "H2-1;H2-2;H2-3;H1-1;H1-2\n"
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

    private static final String TABLE3_CSV   = "H;eight;08;8;1000\n"
            + "E;five;05;5;0101\n"
            + "D;four;04;4;0100\n"
            + "I;nine;09;9;1001\n"
            + "A;one;01;1;0001\n"
            + "G;seven;07;7;0111\n"
            + "F;six;06;6;0110\n"
            + "J;ten;10;10;1010\n"
            + "C;three;03;3;0011\n"
            + "B;two;02;2;0010\n";

    private static final String TABLE4_CSV   = "F;six;0;3,14;6,28\n"
            + "H;eight;3,14;0;0\n"
            + "D;four;3,14;15,71;12,57\n"
            + "A;one;3,14;9,42;6,28\n"
            + "G;seven;6,28;9,42;9,42\n"
            + "I;nine;9,42;6,28;15,71\n"
            + "C;three;9,42;0;12,57\n"
            + "B;two;9,42;12,57;6,28\n"
            + "E;five;15,71;3,14;0\n"
            + "J;ten;15,71;15,71;6,28\n";

    private static Table        table1       = null;
    private static Table        table2       = null;
    private static Table        table3       = null;
    private static Table        table4       = null;
    private static TableManager tableManager = new TableManager();

    @BeforeClass
    public static void setUpBeforClass() throws Exception {
        table1 = buildTableFromCSVString("table1", TABLE1_CSV, true);
        table2 = buildTableFromCSVString("table2", TABLE2_CSV, true);
        table3 = buildTableFromCSVString("table3", TABLE3_CSV, false);
        table4 = buildTableFromCSVString("table4", TABLE4_CSV, false);
    }

    @Test
    public final void testTablesMergeConstructorNominal() {

        // Build the nominal request
        Request tableMergeRequest = new Request();
        tableMergeRequest.joinOn = "join_key";
        tableMergeRequest.outputTableName = "output_table_name";
        tableMergeRequest.tables = new TableInfo[] { table1.getTableInfo(), table2.getTableInfo() };

        try {
            // pass it to the constructor
            new TablesMerge(tableMergeRequest);
        }
        catch (IkatsOperatorException e) {
            fail("Error initializing TablesMerge operator");
        }
    }

    @Test(expected = IkatsOperatorException.class)
    public final void testTablesMergeConstructorException() throws IkatsOperatorException {

        Request tableMergeRequest = new Request();
        tableMergeRequest.joinOn = "join_key";
        tableMergeRequest.outputTableName = "output_table_name";
        // Will raise the exception because 2 tables are expected
        tableMergeRequest.tables = new TableInfo[] { table1.getTableInfo() };

        new TablesMerge(tableMergeRequest);
    }

    @Test
    public final void testDoMergeNominal() throws IOException, IkatsException, IkatsOperatorException {

        String expected_merge = "H1-1;H1-2;H1-3;H1-4;H1-5;H2-1;H2-2;H2-3;H1-1\n"
                + "H;eight;08;8;1000;3,14;0;0;H\n"
                + "E;five;05;5;0101;15,71;3,14;0;E\n"
                + "D;four;04;4;0100;3,14;15,71;12,57;D\n"
                + "I;nine;09;9;1001;9,42;6,28;15,71;I\n"
                + "A;one;01;1;0001;3,14;9,42;6,28;A\n"
                + "G;seven;07;7;0111;6,28;9,42;9,42;G\n"
                + "F;six;06;6;0110;0;3,14;6,28;F\n"
                + "J;ten;10;10;1010;15,71;15,71;6,28;J\n"
                + "C;three;03;3;0011;9,42;0;12,57;C\n"
                + "B;two;02;2;0010;9,42;12,57;6,28;B\n";

        testTableMerge(table1, table2, "H1-2", "expected join", expected_merge);
    }

    @Test
    public final void testDoMergeWithoutJoinOn() throws IOException, IkatsException, IkatsOperatorException {

        String expected_merge = "H1-1;H1-2;H1-3;H1-4;H1-5;H2-1;H2-2;H2-3;H1-2\n"
                + "H;eight;08;8;1000;3,14;0;0;eight\n"
                + "E;five;05;5;0101;15,71;3,14;0;five\n"
                + "D;four;04;4;0100;3,14;15,71;12,57;four\n"
                + "I;nine;09;9;1001;9,42;6,28;15,71;nine\n"
                + "A;one;01;1;0001;3,14;9,42;6,28;one\n"
                + "G;seven;07;7;0111;6,28;9,42;9,42;seven\n"
                + "F;six;06;6;0110;0;3,14;6,28;six\n"
                + "J;ten;10;10;1010;15,71;15,71;6,28;ten\n"
                + "C;three;03;3;0011;9,42;0;12,57;three\n"
                + "B;two;02;2;0010;9,42;12,57;6,28;two\n";

        testTableMerge(table1, table2, null, "expected join_without_join_on", expected_merge);
    }

    @Test
    public final void testDoMergeWithoutColumsHeaderAndNoJoinValue() throws IkatsJsonException, IOException, IkatsException, IkatsOperatorException {

        String expected_merge = "H;eight;08;8;1000;eight;3,14;0;0\n"
                + "E;five;05;5;0101;five;15,71;3,14;0\n"
                + "D;four;04;4;0100;four;3,14;15,71;12,57\n"
                + "I;nine;09;9;1001;nine;9,42;6,28;15,71\n"
                + "A;one;01;1;0001;one;3,14;9,42;6,28\n"
                + "G;seven;07;7;0111;seven;6,28;9,42;9,42\n"
                + "F;six;06;6;0110;six;0;3,14;6,28\n"
                + "J;ten;10;10;1010;ten;15,71;15,71;6,28\n"
                + "C;three;03;3;0011;three;9,42;0;12,57\n"
                + "B;two;02;2;0010;two;9,42;12,57;6,28\n";

        testTableMerge(table3, table4, null, "MergeWithoutColumsHeaderAndNoJoinValue", expected_merge);
    }

    @Test
    public final void testDoMergeWithoutNothing() {

        // Test where no matching colums in other tables

        fail("Not yet implemented"); // TODO
    }

    /**
     * Build a {@link Table} from a CSV string
     * 
     * @param name
     * @param content
     * @param withColumnsHeader
     * @return
     * @throws IOException
     * @throws IkatsException
     */
    private static Table buildTableFromCSVString(String name, String content, boolean withColumnsHeader) throws IOException, IkatsException {

        // Convert the CSV table to expected Table format
        BufferedReader bufReader = new BufferedReader(new StringReader(content));

        String line = null;
        Table table = null;

        if (withColumnsHeader) {
            // Assuming first line contains headers
            line = bufReader.readLine();
            List<String> headersTitle = Arrays.asList(line.split(";"));
            table = tableManager.initTable(headersTitle, false);
        }
        else {
            table = tableManager.initEmptyTable(false, false);
        }

        // Other lines contain data
        while ((line = bufReader.readLine()) != null) {
            List<String> items = Arrays.asList(line.split(";"));
            table.appendRow(items);
        }

        table.setName(name);
        table.setDescription("Table '" + name + "' description created for tests");
        table.setTitle("Table '" + name + "' title");

        logger.trace("Table " + name + " ready");

        return table;
    }

    /**
     * @param firstTable
     * @param secondTable
     * @param joinOn
     * @param outputTableName
     * @param expected_merge
     * @throws IOException
     * @throws IkatsException
     * @throws IkatsJsonException
     * @throws IkatsOperatorException
     */
    private void testTableMerge(Table firstTable, Table secondTable, String joinOn, String outputTableName, String expected_merge) throws IOException, IkatsException, IkatsJsonException, IkatsOperatorException {

        boolean resultTableWithHeader = firstTable.getColumnsHeader() != null || secondTable.getColumnsHeader() != null;

        // Prepare the expected result
        Table expectedResult = buildTableFromCSVString(outputTableName, expected_merge, resultTableWithHeader);
        expectedResult.enableLinks(true, new TableInfo.DataLink(), false, null, true, new TableInfo.DataLink());
        expectedResult.setTitle(null);
        expectedResult.setDescription(null);

        // Prepare the parameters of the merge
        Request tableMergeRequest = new Request();
        tableMergeRequest.joinOn = joinOn;
        tableMergeRequest.outputTableName = outputTableName;
        tableMergeRequest.tables = new TableInfo[] { firstTable.getTableInfo(), secondTable.getTableInfo() };

        // Instanciate the operator and do the job
        TablesMerge tablesMerge = new TablesMerge(tableMergeRequest);
        Table resultTable = tablesMerge.doMerge();

        // Test the expected number of columns
        assertEquals("Bad column count", expectedResult.getColumnCount(true), resultTable.getColumnCount(true));
        // Test the expected number of rows
        assertEquals("Bad row count", expectedResult.getRowCount(true), resultTable.getRowCount(true));

        // Test the JSON rendering -> test all the content
        String expectedTableJSON = tableManager.serializeToJson(expectedResult.getTableInfo());
        String resultTableJSON = tableManager.serializeToJson(resultTable.getTableInfo());
        assertEquals(expectedTableJSON, resultTableJSON);

    }

}
