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

public class TablesMergeTest {

    private static final Logger logger     = Logger.getLogger(TablesMergeTest.class);

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

    private static Table        table1     = null;
    private static Table        table2     = null;

    @BeforeClass
    public static void setUpBeforClass() throws Exception {
        table1 = buildTableFromCSVString("table1", TABLE1_CSV);
        table2 = buildTableFromCSVString("table2", TABLE2_CSV);
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

        TableManager tableManager = new TableManager();
        String EXPECTED_MERGE_1 = "H1-1;H1-2;H1-3;H1-4;H1-5;H2-1;H2-2;H2-3;H1-1\n"
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

        Table expectedResult = buildTableFromCSVString("expected join1", EXPECTED_MERGE_1);
        expectedResult.enableLinks(true, new TableInfo.DataLink(), false, null, true, new TableInfo.DataLink());
        expectedResult.setTitle(null);
        expectedResult.setDescription(null);
        String expectedResultStr = tableManager.serializeToJson(expectedResult.getTableInfo());

        Request tableMergeRequest = new Request();
        tableMergeRequest.joinOn = "H1-2";
        tableMergeRequest.outputTableName = "expected join1";
        tableMergeRequest.tables = new TableInfo[] { table1.getTableInfo(), table2.getTableInfo() };

        TablesMerge tablesMerge = new TablesMerge(tableMergeRequest);
        Table resultTable = tablesMerge.doMerge();

        String resultTableStr = tableManager.serializeToJson(resultTable.getTableInfo());

        assertEquals(expectedResultStr, resultTableStr);
    }

    @Test
    public final void testDoMergeWithoutJoinOn() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public final void testDoMergeWithoutColumsHeader() {
        fail("Not yet implemented"); // TODO
    }

    public final void testDoMergeWithoutNothing() {

        // Test where no matching colums in other tables

        fail("Not yet implemented"); // TODO
    }

    /**
     * 
     * @param name
     * @param content
     * @return
     * @throws IOException
     * @throws IkatsException
     */
    private static Table buildTableFromCSVString(String name, String content) throws IOException, IkatsException {
        TableManager tableManager = new TableManager();

        // Convert the CSV table to expected Table format
        BufferedReader bufReader = new BufferedReader(new StringReader(content));

        // Assuming first line contains headers
        String line = bufReader.readLine();
        List<String> headersTitle = Arrays.asList(line.split(";"));
        Table table = tableManager.initTable(headersTitle, false);

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

}
