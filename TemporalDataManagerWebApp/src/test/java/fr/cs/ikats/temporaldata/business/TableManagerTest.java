package fr.cs.ikats.temporaldata.business;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import fr.cs.ikats.temporaldata.business.TableManager.TableHandler;
import junit.framework.TestCase;

/**
 * TableManagerTest tests the TableManager and its end-user services.
 */
public class TableManagerTest extends TestCase {

    /**
     * Do not change this sample: reused by several tests: for new purposes =>
     * create another one
     */
    private final static String JSON_CONTENT_SAMPLE_1 = "{\"table_desc\":{\"title\":\"Discretized matrix\",\"desc\":\"This is a ...\"},\"headers\":{\"col\":{\"data\":[\"funcId\",\"metric\",\"min_B1\",\"max_B1\",\"min_B2\",\"max_B2\"],\"links\":null,\"default_links\":null},\"row\":{\"data\":[null,\"Flid1_VIB2\",\"Flid1_VIB3\",\"Flid1_VIB4\",\"Flid1_VIB5\"],\"default_links\":{\"type\":\"ts_bucket\",\"context\":\"processdata\"},\"links\":[null,{\"val\":\"1\"},{\"val\":\"2\"},{\"val\":\"3\"},{\"val\":\"4\"}]}},\"content\":{\"cells\":[[\"VIB2\",-50.0,12.1,1.0,3.4],[\"VIB3\",-5.0,2.1,1.0,3.4],[\"VIB4\",0.0,2.1,12.0,3.4],[\"VIB5\",0.0,2.1,1.0,3.4]]}}";

    /**
     * Tests getColumnFromTable: case selecting the row-header values
     */
    public void testGetFirstColumnFromTable() {
        try {
            TableManager mng = new TableManager();

            Table table = mng.loadFromJson(TableManagerTest.JSON_CONTENT_SAMPLE_1);

            TableHandler tableH = mng.getHandler(table);
            String firstColName = (String) tableH.getColumnsHeader().getSimpleElements().get(0);
            List<String> funcIds = mng.getColumnFromTable(table, "funcId");
            List<Object> refFuncIds = new ArrayList<Object>(table.headers.row.data);
            refFuncIds.remove(0);

            // System.out.println( funcIds );
            // System.out.println( refFuncIds );
            assertEquals(refFuncIds, funcIds);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Test got unexptected error");
        }

    }

    /**
     * Tests getColumnFromTable: case selecting the content values
     */
    public void testGetOtherColumnsFromTable() {
        try {
            TableManager mng = new TableManager();

            Table table = mng.loadFromJson(TableManagerTest.JSON_CONTENT_SAMPLE_1);
            // System.out.println( TableManagerTest.JSON_CONTENT_SAMPLE_1 );

            // Testing typed String access
            //

            List<String> metrics = mng.getColumnFromTable(table, "metric");
            List<String> refMetrics = new ArrayList<>();
            for (List<Object> row : table.content.cells) {
                refMetrics.add((String) row.get(0));
            }

            assertEquals(refMetrics, metrics);

            // Testing typed Double access
            //

            List<Double> otherDecimal = mng.getColumnFromTable(table, "min_B1");
            List<Double> refOtherDecimal = new ArrayList<>();
            for (List<Object> row : table.content.cells) {
                refOtherDecimal.add((Double) row.get(1));
            }

            // System.out.println( refOtherDecimal );
            // System.out.println( refOtherDecimal );
            assertEquals(refOtherDecimal, otherDecimal);
            assertEquals(-50.0, otherDecimal.get(0));

            // Testing untyped case: Object
            //
            List<Object> other = mng.getColumnFromTable(table, "max_B1");
            List<Object> refOther = new ArrayList<>();
            for (List<Object> row : table.content.cells) {
                refOther.add(row.get(2));
            }

            assertEquals(refOther, other);
            assertEquals(12.1, other.get(0));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Test got unexptected error");
        }

    }

    /**
     * Tests initCsvLikeTable: case of the creation of a simple-csv Table: with one column header and
     * simple rows (without row header).
     */
    public void testInitCsvLikeTableSimple() {

        TableManager mng = new TableManager();
        Table table = new Table();
        TableHandler tableH = mng.getHandler(table);
        try {

            tableH.initColumnsHeader(false, null, false).addItem("One", null).addItem("Two", null).addItem("Three", null);
            tableH.initContent(false, null);
            
            TableHandler tableHBis = mng.initCsvLikeTable( Arrays.asList(new String[]{ "One", "Two", "Three"} ));
            
            Object[] row1 = new Object[] { "One", new Double(2.0), Boolean.FALSE };

            Double[] row2 = new Double[] { 1.0, 2.2, 3.5 };

            Boolean[] row3 = new Boolean[] { Boolean.TRUE, false, Boolean.TRUE };
            
            tableH.appendRow(Arrays.asList(row1), null);
            tableH.appendRow(Arrays.asList(row2), null);
            tableH.appendRow(Arrays.asList(row3), null);
            tableHBis.appendRow(Arrays.asList(row1), null);
            tableHBis.appendRow(Arrays.asList(row2), null);
            tableHBis.appendRow(Arrays.asList(row3), null);

            assertEquals(mng.serializeToJson(table), mng.serializeToJson(tableHBis.getTable()));
            // System.out.println(mng.serializeToJson(table));
            // System.out.println(mng.serializeToJson(tableHBis.getTable()));
            
            List<Object> columnn = tableH.getColumnFromTable("One");
            // System.out.println( colOne );

        }
        catch (Exception e) {
            e.printStackTrace(System.err);
            fail("Failed test: unexpected error");
        }

    }

    public void testAppendRowWithoutLinks() {

        try {
            TableManager mng = new TableManager();

            mng = new TableManager();

            Table table = mng.loadFromJson(TableManagerTest.JSON_CONTENT_SAMPLE_1);

            TableHandler tableH = mng.getHandler(table);
            int initialRowCount = tableH.getRowCount(true);
            int initialColumnCount = tableH.getColumnCount(true);

            System.out.println(TableManagerTest.JSON_CONTENT_SAMPLE_1);

            List<Object> addedList = new ArrayList<>();
            for (int i = 0; i < 4; i++)
                addedList.add("item" + i);

            // Should accept different types in a row:
            // => insert a different Type: int instead of String
            addedList.add(10);

            // Tests appended row with not links
            String addedRowHeaderData = "AddedRow";
            int index = tableH.appendRow(addedRowHeaderData, null, addedList, null);

            System.out.println("Row header data: " + table.headers.row.data.get(5));
            System.out.println("" + table.content.cells.get(4));

            int finalRowCount = tableH.getRowCount(true);
            int finalColumnCount = tableH.getColumnCount(true);

            assertTrue(initialColumnCount == finalColumnCount);
            assertTrue(initialRowCount + 1 == finalRowCount);

            assertEquals(table.headers.row.data.get(finalRowCount - 1), addedRowHeaderData);
            assertEquals(table.content.cells.get(finalRowCount - 2), addedList);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Test got unexptected error");
        }

    }

}
