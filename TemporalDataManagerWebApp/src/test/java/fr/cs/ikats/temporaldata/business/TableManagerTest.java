package fr.cs.ikats.temporaldata.business;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import fr.cs.ikats.temporaldata.business.TableInfo.DataLink;
import fr.cs.ikats.temporaldata.business.TableInfo.Header;
import fr.cs.ikats.temporaldata.business.TableManager.Table;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import junit.framework.TestCase;
import org.apache.commons.collections.ComparatorUtils;
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
     * Tests getColumnFromTable: case when selected column is the row-header
     * values (below top-left corner)
     */
    public void testGetFirstColumnFromTable() {
        try {
            TableManager mng = new TableManager();

            TableInfo table = mng.loadFromJson(TableManagerTest.JSON_CONTENT_SAMPLE_1);

            Table tableH = mng.initTable(table, false);
            String firstColName = (String) tableH.getColumnsHeader().getData().get(0);
            List<String> funcIds = tableH.getColumn("funcId");
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

            TableInfo tableJson = mng.loadFromJson(TableManagerTest.JSON_CONTENT_SAMPLE_1);
            System.out.println(TableManagerTest.JSON_CONTENT_SAMPLE_1);

            Table table = mng.initTable(tableJson, false);

            // Testing typed String access
            //
            List<String> metrics = table.getColumn("metric");
            List<String> refMetrics = new ArrayList<>();
            for (List<Object> row : tableJson.content.cells) {
                refMetrics.add((String) row.get(0));
            }

            assertEquals(refMetrics, metrics);

            // Testing typed Double access
            //

            List<Double> otherDecimal = table.getColumn("min_B1", Double.class);
            List<Double> refOtherDecimal = new ArrayList<>();
            for (List<Object> row : tableJson.content.cells) {
                refOtherDecimal.add((Double) row.get(1));
            }

            // System.out.println( refOtherDecimal );
            // System.out.println( refOtherDecimal );
            assertEquals(refOtherDecimal, otherDecimal);
            assertEquals(-50.0, otherDecimal.get(0));

            // Testing untyped case: Object
            //
            List<Object> other = table.getColumn("max_B1", Object.class);
            List<Object> refOther = new ArrayList<>();
            for (List<Object> row : tableJson.content.cells) {
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
     * Tests getting a column with col header name, from a table with columns
     * header and without rows header.
     */
    public void testGetColumnFromHeaderName() {
        try {
            TableManager mng = new TableManager();

            // Test first subcase: without rows header
            Table lTestedTable = mng.initTable(Arrays.asList("Id", "Target"), false);
            lTestedTable.appendRow(Arrays.asList("hello", 1));
            lTestedTable.appendRow(Arrays.asList("hello2", 10));
            lTestedTable.appendRow(Arrays.asList("hello3", 100));

            List<String> myIds = lTestedTable.getColumn("Id");
            List<Integer> myTargets = lTestedTable.getColumn("Target", Integer.class);
            assertEquals(Arrays.asList("hello", "hello2", "hello3"), myIds);
            assertEquals(Arrays.asList(1, 10, 100), myTargets);
            
            // Test second subcase: with rows header
            Table lTestedTableWithRowsH = mng.initTable(Arrays.asList("Id", "Target"), true);
            lTestedTableWithRowsH.appendRow("hello", Arrays.asList(1));
            lTestedTableWithRowsH.appendRow("hello2", Arrays.asList(10));
            lTestedTableWithRowsH.appendRow("hello3", Arrays.asList(100));

            List<String> myIdsBIS = lTestedTableWithRowsH.getColumn("Id");
            // testing converted String
            List<String> myTargetsBIS = lTestedTableWithRowsH.getColumn("Target");

            assertEquals(Arrays.asList("hello", "hello2", "hello3"), myIdsBIS);
            // testing converted String
            assertEquals(Arrays.asList("1", "10", "100"), myTargetsBIS);

        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Test got unexptected error");
        }
    }

    public void testGetRowFromTable() {
        try {
            TableManager mng = new TableManager();

            TableInfo table = mng.loadFromJson(TableManagerTest.JSON_CONTENT_SAMPLE_1);

            Table tableH = mng.initTable(table, false);

            // The simpler getter: row at index=... from TableContent:
            int contentIndex = 0;
            List<Object> selectedRowValsBis = tableH.getRow(contentIndex + 1, Object.class);

            // Another way: using the row header name
            // Reads the row header value: at position (contentIndex + 1) (after
            // top left corner)
            String secondRowName = (String) tableH.getRowsHeader().getData().get(contentIndex + 1);
            List<Object> selectedRowVals = tableH.getRow(secondRowName, Object.class);
            List<Object> ref = new ArrayList<Object>(table.content.cells.get(contentIndex));

            System.out.println(selectedRowValsBis);
            System.out.println(selectedRowVals);
            System.out.println(ref);

            assertEquals(selectedRowVals, selectedRowValsBis);
            assertEquals(selectedRowVals, Arrays.asList("VIB2", -50.0, 12.1, 1.0, 3.4));

            assertEquals(selectedRowVals, ref);

        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Test got unexptected error");
        }

    }

    /**
     * Tests initCsvLikeTable: case of the creation of a simple-csv Table: with
     * one column header and simple rows (without row header).
     */
    public void testInitTableSimple() {

        TableManager mng = new TableManager();
        TableInfo table = new TableInfo();
        Table tableH = mng.initTable(table, false);
        try {

            // Deprecated for end-user
            tableH.initColumnsHeader(true, null, new ArrayList<>(), null).addItem("One", null).addItem("Two", null).addItem("Three", null);
            tableH.initContent(false, null);

            // Simple initializer
            Table tableHBis = mng.initTable(Arrays.asList("One", "Two", "Three"), false);

            Object[] row1 = new Object[] { "One", new Double(2.0), Boolean.FALSE };

            Double[] row2 = new Double[] { 1.0, 2.2, 3.5 };

            Boolean[] row3 = new Boolean[] { Boolean.TRUE, false, Boolean.TRUE };

            tableH.appendRow(Arrays.asList(row1));
            tableH.appendRow(Arrays.asList(row2));
            tableH.appendRow(Arrays.asList(row3));
            tableHBis.appendRow(Arrays.asList(row1));
            tableHBis.appendRow(Arrays.asList(row2));
            tableHBis.appendRow(Arrays.asList(row3));

            System.out.println(mng.serializeToJson(table));
            System.out.println(mng.serializeToJson(tableHBis.getTableInfo()));

            assertEquals(mng.serializeToJson(table), mng.serializeToJson(tableHBis.getTableInfo()));

            List<Object> columnn = tableH.getColumn("One");
            // System.out.println( colOne );

        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Failed test: unexpected error");
        }

    }

    /**
     * Tests initCsvLikeTable: case of the creation of a simple-csv Table: with
     * column header and rows header.
     */
    public void testInitTableWithRowsHeader() {

        TableManager mng = new TableManager();
        TableInfo table = new TableInfo();
        Table tableH = mng.initTable(table, false);
        try {

            tableH.initColumnsHeader(true, null, new ArrayList<>(), null).addItem("Above row header", null).addItem("One", null).addItem("Two", null).addItem("Three",
                    null);
            tableH.initRowsHeader(false, null, new ArrayList<>(), null);
            tableH.initContent(false, null);

            // Simplified initializer used with tableHBis
            Table tableHBis = mng.initTable(Arrays.asList(new String[] { "Above row header", "One", "Two", "Three" }), true);

            // Defining the content rows - excluding row header part-
            Object[] row1 = new Object[] { "One", new Double(2.0), Boolean.FALSE };

            Double[] row2 = new Double[] { 1.0, 2.2, 3.5 };

            Boolean[] row3 = new Boolean[] { Boolean.TRUE, false, Boolean.TRUE };

            // append content rows + defines headers "A" "B" ...
            tableH.appendRow("A", Arrays.asList(row1));
            tableH.appendRow("B", Arrays.asList(row2));
            tableH.appendRow("C", Arrays.asList(row3));
            tableHBis.appendRow("A", Arrays.asList(row1));
            tableHBis.appendRow("B", Arrays.asList(row2));
            tableHBis.appendRow("C", Arrays.asList(row3));

            // System.out.println(mng.serializeToJson(table));
            // System.out.println(mng.serializeToJson(tableHBis.getTable()));

            assertEquals(mng.serializeToJson(table), mng.serializeToJson(tableHBis.getTableInfo()));

            List<Object> columnnTwo = tableH.getColumn("Two", Object.class);
            // System.out.println( columnnTwo );

            assertEquals(columnnTwo, Arrays.asList(new Object[] { 2.0, 2.2, false }));

            List<Object> columnnOfRowHeaders = tableH.getColumn("Above row header");
            // System.out.println( columnnOfRowHeaders );

            assertEquals(columnnOfRowHeaders, Arrays.asList("A", "B", "C"));
        }
        catch (Exception e) {
            e.printStackTrace(System.err);
            fail("Failed test: unexpected error");
        }
    }

    /**
     * 
     */
    public void testInitTableWithRowsHeaderWithLinks() {

        TableManager mng = new TableManager();
        TableInfo table = new TableInfo();
        Table tableH = mng.initTable(table, false);
        try {
            DataLink defColH = new DataLink();
            defColH.context = "conf col header link";
            DataLink defRowH = new DataLink();
            defRowH.context = "conf row header link";
            DataLink defContent = new DataLink();
            defContent.context = "conf content link";

            tableH.initColumnsHeader(true, null, new ArrayList<>(), null).addItem("Above row header", null).addItem("One", null).addItem("Two", null).addItem("Three",
                    null);
            tableH.initRowsHeader(false, null, new ArrayList<>(), null);
            tableH.initContent(false, null);
            tableH.enableLinks(true, defColH, true, defRowH, true, defContent);

            // Simplified initializer used with tableHBis
            Table tableHBis = mng.initTable(Arrays.asList(new String[] { "Above row header", "One", "Two", "Three" }), true);
            tableHBis.enableLinks(true, defColH, true, defRowH, true, defContent);

            // Defining the content rows - excluding row header part-
            Object[] row1 = new Object[] { "One", new Double(2.0), Boolean.FALSE };

            // Row2 ....
            DataLink linkOne = new DataLink();
            linkOne.context = "ctx 1";
            linkOne.val = "val 1";

            DataLink linkTwo = new DataLink();
            linkTwo.type = "typ 2";
            linkTwo.val = "val 2";

            // ... this row has defined links ...
            List<TableElement> row2AsList = TableElement.encodeElements(new TableElement("Prem", linkOne), new TableElement("Sec", linkTwo), "Tri");

            // Row3
            List<TableElement> row3 = TableElement.encodeElements(1, 2, 3);

            // Init tableH with links ...
            //
            // append content rows + defines headers "A" "B" ...

            tableH.appendRow("A", Arrays.asList(row1));

            tableH.appendRow("B", row2AsList);

            tableH.appendRow("C", row3);

            // Tests the same content with tableHBis: testing that 2
            // initializations are equivalent.
            tableHBis.appendRow("A", Arrays.asList(row1));

            // ... this row has defined links ...
            tableHBis.appendRow("B", row2AsList);

            tableHBis.appendRow("C", row3);

            // System.out.println(mng.serializeToJson(table));
            // System.out.println(mng.serializeToJson(tableHBis.getTable()));
            assertEquals(mng.serializeToJson(table), mng.serializeToJson(tableHBis.getTableInfo()));

            List<Object> columnnTwo = tableH.getColumn("Two", Object.class);
            // System.out.println( columnnTwo );

            assertEquals(columnnTwo, Arrays.asList(new Object[] { row1[1], row2AsList.get(1).data, row3.get(1).data }));
            assertEquals(columnnTwo, tableHBis.getColumn("Two", Object.class));

            // Tests link selection: linkTwo.type= "typ 2";
            // linkTwo.val= "val 2";
            // assertEquals( tableH.cells.links.get(1).get(1).type,
            // tableHBis.getColumnFromTable("Two") );

            List<Object> columnnOfRowHeaders = tableH.getColumn("Above row header");
            // System.out.println( columnnOfRowHeaders );

            assertEquals(columnnOfRowHeaders, Arrays.asList(new Object[] { "A", "B", "C" }));
        }
        catch (Exception e) {
            e.printStackTrace(System.err);
            fail("Failed test: unexpected error");
        }
    }

    /**
     * 
     */
    public void testAppendRowWithoutLinks() {

        try {
            TableManager mng = new TableManager();

            mng = new TableManager();

            TableInfo table = mng.loadFromJson(TableManagerTest.JSON_CONTENT_SAMPLE_1);

            Table tableH = mng.initTable(table, false);
            int initialRowCount = tableH.getRowCount(true);
            int initialColumnCount = tableH.getColumnCount(true);

            // System.out.println(TableManagerTest.JSON_CONTENT_SAMPLE_1);

            List<Object> addedList = new ArrayList<>();
            for (int i = 0; i < 4; i++)
                addedList.add("item" + i);

            // Should accept different types in a row:
            // => insert a different Type: int instead of String
            addedList.add(10);

            // Tests appended row with not links
            String addedRowHeaderData = "AddedRow";
            int index = tableH.appendRow(addedRowHeaderData, addedList);

            // System.out.println("Row header data: " +
            // table.headers.row.data.get(5));
            // System.out.println("" + table.content.cells.get(4));

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

    /**
     * tests the getColumn services
     */
    public void testGetColumn() {
        try {
            TableManager mng = new TableManager();
            Table myT = mng.initTable(Arrays.asList("One", "Two", "Three"), false);
            for (int i = 0; i < 10; i++) {
                myT.appendRow(Arrays.asList(i < 5, "" + i, null));
            }

            System.out.println(myT.getColumn("One"));
            System.out.println(myT.getColumn("Two"));
            System.out.println(myT.getColumn("Three"));

            List<Boolean> strOneList = myT.getColumn("One", Boolean.class);
            List<String> strOneListAString = myT.getColumn("One");
            try {
                List<BigDecimal> wrongTypeTested = myT.getColumn("One", BigDecimal.class);
                fail("Incorrect: class cast exception not detected !");
            }
            catch (IkatsException e) {
                assertTrue(true);
            }
            catch (Exception e) {
                fail("Unexpected exception");
            }

            assert (strOneList.get(0) instanceof Boolean);
            System.out.println("ok1");
            assert (strOneListAString.get(0) instanceof String);
            System.out.println("ok2");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Test got unexptected error");
        }

    }
    /**
     * Tests the getRow services
     */
    public void testGetRow() {
        try {
            TableManager mng = new TableManager();
            Table myT = mng.initEmptyTable(true, true);
            myT.getColumnsHeader().addItem( "One").addItem( "Two").addItem("Three");
            for (int i = 0; i < 10; i++) {
                myT.appendRow("row"+i, Arrays.asList(i, i +2) );
            }

            System.out.println(myT.getRow("row1"));
            System.out.println(myT.getRow("row2"));
            System.out.println(myT.getRow("row9"));

            List<Integer> strOneList = myT.getRow("row1", Integer.class);
            List<String> strOneListAString = myT.getRow("row1");
            try {
                List<BigDecimal> wrongTypeTested = myT.getRow("row2", BigDecimal.class);
                fail("Incorrect: class cast exception not detected !");
            }
            catch (IkatsException e) {
                assertTrue(true);
            }
            catch (Exception e) {
                fail("Unexpected exception");
            }

            assert (strOneList.get(0) instanceof Integer);
            assert (strOneList.get(0).equals( 0 ) );
            assert (strOneList.get(1).equals( 2 ) );
            assert (strOneListAString.get(0) instanceof String);
            assert (strOneListAString.get(0).equals("0" ));
            assert (strOneListAString.get(1).equals("2" ));
           
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Test got unexptected error");
        }

    }
    
    /**
     * Tests the getRow services
     */
    public void testGetRowsHeaderItems() {
        try {
            TableManager mng = new TableManager();
            Table myT = mng.initEmptyTable(false, true);
            
          
            myT.getRowsHeader().addItems("0", "1", "2", "3", "4");
            for (int i = 0; i < 10; i++) {
                myT.appendColumn( Arrays.asList("a"+i, "b"+i, "c"+i,  "d"+i, "e"+i));
            }

            System.out.println(myT.getRowsHeader().getItems());
            assertEquals( "0", myT.getRowsHeader().getItems().get(0) );
            assertEquals( "4", myT.getRowsHeader().getItems().get(4) );
            
            System.out.println(myT.getRow("0"));
            assertEquals( "a0", myT.getRow("0").get(0) );
            assertEquals( "a5", myT.getRow("0").get(5) );
          
            // Note: we could manage also  myT.getRowsHeader().addItems(0, 1, 2, 3, 4);
            // ...
            // in that case: we also retrieve the row using myT.getRow("0") instead of myT.getRow(0) 
            // => this will work
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Test got unexptected error");
        }

    }
    
    
    /**
     * Tests the getColumn services:
     * 
     */
    public void testGetColumnsHeaderItems() {
        try {
            TableManager mng = new TableManager();
            Table myT = mng.initEmptyTable(true, false);
            
            // unusual case: handling Integer -instead of String- in header data ... why not ...
            myT.getColumnsHeader().addItems(0, 1, 2, 3, 8);
            for (int i = 0; i < 10; i++) {
                myT.appendRow( Arrays.asList("a"+i, "b"+i, "c"+i,  "d"+i, "e"+i));
            }
            
            System.out.println(myT.getColumnsHeader().getItems());
            // getting effective header data
            assertEquals( new Integer(0), myT.getColumnsHeader().getItems(Integer.class).get(0));
            assertEquals( new Integer(8), myT.getColumnsHeader().getItems(Integer.class).get(4));
            // getting header data as string
            assertEquals( "8", myT.getColumnsHeader().getItems().get(4));
            
            // a bit weird but the key for header 0 is toString() representation "0"
            // => 
            System.out.println(myT.getColumn("0"));
            
            
            // usual case: handling Strings in header data
            Table myUsualT = mng.initEmptyTable(true, false);
            myUsualT.getColumnsHeader().addItems("10", "1", "2", "3", "8");
            for (int i = 0; i < 10; i++) {
                myUsualT.appendRow( Arrays.asList("a"+i, "b"+i, "c"+i,  "d"+i, "e"+i));
            }
            
            System.out.println(myUsualT.getColumnsHeader().getItems());
            assertEquals( "10", myUsualT.getColumnsHeader().getItems().get(0));
            assertEquals( "8", myUsualT.getColumnsHeader().getItems().get(4));
            System.out.println(myUsualT.getColumn("10"));
            
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Test got unexptected error");
        }
    }
    
    /**
     * Tests sort algo based upon column values from table content, - not from the row header -
     * Ascending order is tested here.
     */
    public void testSortRowswithoutHeaders()
    {
        try {
            TableManager mng = new TableManager();
            Table myTWithoutRowHeader = mng.initEmptyTable(false, false);
            
            myTWithoutRowHeader.appendRow(Arrays.asList("bla3", "BLAH3", 3.5));
            myTWithoutRowHeader.appendRow(Arrays.asList("bla2", "BLAH2", 2.0));
            myTWithoutRowHeader.appendRow(Arrays.asList("bla4", "BLAH4", 3.7));
            myTWithoutRowHeader.appendRow(Arrays.asList("bla6", "BLAH6", 6.0));
            myTWithoutRowHeader.appendRow(Arrays.asList("-bla6", "-BLAH6", -6.0));

            boolean display = true;
            
            displayTestedTable(display, myTWithoutRowHeader);

            myTWithoutRowHeader.sortRowsByColumnValues(2, false);
            
            displayTestedTable(display, myTWithoutRowHeader);
            
            assertEquals(Arrays.asList( -6.0, 2.0, 3.5, 3.7, 6.0), myTWithoutRowHeader.getColumn(2, Double.class));
            
            assertEquals(Arrays.asList("-bla6", "-BLAH6", -6.0), myTWithoutRowHeader.getRow(0, Object.class) );
            assertEquals(Arrays.asList("bla2", "BLAH2", 2.0), myTWithoutRowHeader.getRow(1, Object.class) );
            assertEquals(Arrays.asList("bla3", "BLAH3", 3.5), myTWithoutRowHeader.getRow(2, Object.class) );
            assertEquals(Arrays.asList("bla4", "BLAH4", 3.7), myTWithoutRowHeader.getRow(3, Object.class) );
            assertEquals(Arrays.asList("bla6", "BLAH6", 6.0), myTWithoutRowHeader.getRow(4, Object.class) );
            
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Test got unexptected error");
        } 
    }
    
    /**
     * Tests sort algo based upon column values from table content, - not from the row header -
     * Ascending order is tested here.
     */
    public void testSortRowsWithColHeader()
    {
        try {
            TableManager mng = new TableManager();
            Table myTWithoutRowHeader = mng.initEmptyTable(true, false);
            
            // unusual case: handling Integer -instead of String- in header data ... why not ...
            myTWithoutRowHeader.getColumnsHeader().addItems("First", "Blabla", "Order");
          
            myTWithoutRowHeader.appendRow(Arrays.asList("bla3", "BLAH3", 3));
            myTWithoutRowHeader.appendRow(Arrays.asList("bla2", "BLAH2", 2));
            myTWithoutRowHeader.appendRow(Arrays.asList("bla4", "BLAH4", 4));
            myTWithoutRowHeader.appendRow(Arrays.asList("bla6", "BLAH6", 6));
            myTWithoutRowHeader.appendRow(Arrays.asList("-bla6", "-BLAH6", -6));

            boolean display = true;
            
            displayTestedTable(display, myTWithoutRowHeader);

            myTWithoutRowHeader.sortRowsByColumnValues("Order", false);
            
            displayTestedTable(display, myTWithoutRowHeader);
            
            assertEquals(Arrays.asList( -6, 2, 3, 4, 6), myTWithoutRowHeader.getColumn("Order", Integer.class));
            
            assertEquals(Arrays.asList("-bla6", "-BLAH6", -6), myTWithoutRowHeader.getRow(1, Object.class) );
            assertEquals(Arrays.asList("bla2", "BLAH2", 2), myTWithoutRowHeader.getRow(2, Object.class) );
            assertEquals(Arrays.asList("bla3", "BLAH3", 3), myTWithoutRowHeader.getRow(3, Object.class) );
            assertEquals(Arrays.asList("bla4", "BLAH4", 4), myTWithoutRowHeader.getRow(4, Object.class) );
            assertEquals(Arrays.asList("bla6", "BLAH6", 6), myTWithoutRowHeader.getRow(5, Object.class) );
            
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Test got unexptected error");
        } 
    }
    
    /**
     * Tests sort algo based upon column values from table content, - not from the row header -
     * Ascending order is tested here.
     */
    public void testSortRowsWitAllHeaders()
    {
        try {
            TableManager mng = new TableManager();
            Table myTWithoutRowHeader = mng.initEmptyTable(true, true);
            
            // unusual case: handling Integer -instead of String- in header data ... why not ...
            myTWithoutRowHeader.getColumnsHeader().addItems("TopLeft", "First", "Blabla", "Order");
            // needs to define the top left corner as undefined !
            myTWithoutRowHeader.getRowsHeader().addItem(null);
            
            myTWithoutRowHeader.appendRow("B1.1", Arrays.asList("bla3", "BLAH3", 3));
            myTWithoutRowHeader.appendRow("B1", Arrays.asList("bla2", "BLAH2", 2));
            myTWithoutRowHeader.appendRow("A100", Arrays.asList("bla4", "BLAH4", 4));
            myTWithoutRowHeader.appendRow("A10", Arrays.asList("bla6", "BLAH6", 6));
            myTWithoutRowHeader.appendRow("A2", Arrays.asList("-bla6", "-BLAH6", -6));

            boolean display = true;
            
            displayTestedTable(display, myTWithoutRowHeader);

            myTWithoutRowHeader.sortRowsByColumnValues("Order", false);
            
            displayTestedTable(display, myTWithoutRowHeader);
            
            assertEquals(Arrays.asList( -6, 2, 3, 4, 6), myTWithoutRowHeader.getColumn("Order", Integer.class));
            
            assertEquals(Arrays.asList("-bla6", "-BLAH6", -6), myTWithoutRowHeader.getRow(1, Object.class) );
            assertEquals(Arrays.asList("bla2", "BLAH2", 2), myTWithoutRowHeader.getRow(2, Object.class) );
            assertEquals(Arrays.asList("bla3", "BLAH3", 3), myTWithoutRowHeader.getRow(3, Object.class) );
            assertEquals(Arrays.asList("bla4", "BLAH4", 4), myTWithoutRowHeader.getRow(4, Object.class) );
            assertEquals(Arrays.asList("bla6", "BLAH6", 6), myTWithoutRowHeader.getRow(5, Object.class) );
            
            myTWithoutRowHeader.sortRowsByColumnValues("TopLeft", false);
            
            displayTestedTable(display, myTWithoutRowHeader);
            
            assertEquals(Arrays.asList( "A2", "A10", "A100", "B1", "B1.1"), myTWithoutRowHeader.getColumn("TopLeft", String.class));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Test got unexptected error");
        } 
    }

    private void displayTestedTable(boolean enableDisplay, Table table) throws IkatsException, ResourceNotFoundException {
        if ( enableDisplay ) 
        {
            if ( table.isHandlingColumnsHeader() )
                System.out.println( table.getColumnsHeader().getItems());
            List<Object> rowsHeaderItems = table.isHandlingRowsHeader() ? table.getRowsHeader().getItems(Object.class): null; 
            for (int i = 0; i < table.getRowCount(true); i++) {
                  String start = "";
                  if ( rowsHeaderItems != null)
                      {
                          start = "" + rowsHeaderItems.get(i ) + ": ";
                      }
                System.out.println(start +  table.getRow(i, Object.class));
            }
            System.out.println( " ");
        }
    }
}
