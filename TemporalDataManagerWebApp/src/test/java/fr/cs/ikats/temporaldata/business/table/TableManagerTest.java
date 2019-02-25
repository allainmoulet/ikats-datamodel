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

package fr.cs.ikats.temporaldata.business.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import fr.cs.ikats.operators.TablesMergeTest;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.cs.ikats.table.TableEntitySummary;
import fr.cs.ikats.temporaldata.business.table.TableInfo.DataLink;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;

/**
 * TableManagerTest tests the TableManager and its end-user services.
 */
public class TableManagerTest {

    /**
     * Do not change this sample: reused by several tests: for new purposes => create another one
     */
    private final static String JSON_CONTENT_SAMPLE_1 = "{\"table_desc\":{\"title\":\"Discretized matrix\",\"desc\":\"This is a ...\"},\"headers\":{\"col\":{\"data\":[\"funcId\",\"metric\",\"min_B1\",\"max_B1\",\"min_B2\",\"max_B2\"],\"links\":null,\"default_links\":null},\"row\":{\"data\":[null,\"Flid1_VIB2\",\"Flid1_VIB3\",\"Flid1_VIB4\",\"Flid1_VIB5\"],\"default_links\":{\"type\":\"ts_bucket\",\"context\":\"processdata\"},\"links\":[null,{\"val\":\"1\"},{\"val\":\"2\"},{\"val\":\"3\"},{\"val\":\"4\"}]}},\"content\":{\"cells\":[[\"VIB2\",-50.0,12.1,1.0,3.4],[\"VIB3\",-5.0,2.1,1.0,3.4],[\"VIB4\",0.0,2.1,12.0,3.4],[\"VIB5\",0.0,2.1,1.0,3.4]]}}";
    private final static String JSON_CONTENT_SAMPLE_headers = "{\"table_desc\":{\"title\":\"tableAllHeaders\",\"desc\":\"test\"},"
        +"\"headers\":{\"col\":{\"data\":[\"Index\",\"C_one\",\"C_two\",\"C_three\"]},"
        +"\"row\":{\"data\":[null,\"R_one\",\"R_two\",\"R_three\"]}},"
        +"\"content\":{\"cells\":[[\"1\",\"2\",\"3\"],[\"4\",\"5\",\"5\"],[\"6\",\"7\",\"8\"]]}}";
    private final static String JSON_CONTENT_SAMPLE_no_Headers = "{\"table_desc\":{\"title\":\"tableNoHeader\",\"desc\":\"test\"},"
            +"\"content\":{\"cells\":[[\"Index\",\"C_one\",\"C_two\",\"C_three\"],[\"R_one\",\"1\",\"2\",\"3\"],[\"R_two\",\"4\",\"5\",\"5\"],[\"R_three\",\"6\",\"7\",\"8\"]]}}";
    private final static String JSON_CONTENT_SAMPLE_colHeaders = "{\"table_desc\":{\"title\":\"tableOnlyColHeader\",\"desc\":\"test\"},"
            +"\"headers\":{\"col\":{\"data\":[\"Index\",\"C_one\",\"C_two\",\"C_three\"]}},"
            +"\"content\":{\"cells\":[[\"R_one\",\"1\",\"2\",\"3\"],[\"R_two\",\"4\",\"5\",\"5\"],[\"R_three\",\"6\",\"7\",\"8\"]]}}";
    private final static String JSON_CONTENT_SAMPLE_rowHeaders = "{\"table_desc\":{\"title\":\"tableOnlyRowHeader\",\"desc\":\"test\"},"
            +"\"headers\":{\"row\":{\"data\":[\"Index\",\"R_one\",\"R_two\",\"R_three\"]}},"
            +"\"content\":{\"cells\":[[\"C_one\",\"C_two\",\"C_three\"],[\"1\",\"2\",\"3\"],[\"4\",\"5\",\"5\"],[\"6\",\"7\",\"8\"]]}}";


    private static final Logger logger = Logger.getLogger(TablesMergeTest.class);


    private static final String TABLE_CSV = "Index;C_one;C_two;C_three\n"
            + "R_one;1;2;3\n"
            + "R_two;4;5;5\n"
            + "R_three;6;7;8\n";

    /**
     * Tests getColumnFromTable: case when selected column is the row-header values (below top-left corner)
     *
     * @throws Exception
     */
    @Test
    public void testGetFirstColumnFromTable() throws Exception {

        TableManager mng = new TableManager();

        TableInfo table = mng.loadFromJson(TableManagerTest.JSON_CONTENT_SAMPLE_1);

        Table tableH = mng.initTable(table, false);

        String firstColName = (String) tableH.getColumnsHeader().getData().get(0);
        List<String> funcIds = tableH.getColumn(firstColName);
        List<Object> refFuncIds = new ArrayList<Object>(table.headers.row.data);
        refFuncIds.remove(0);

        assertEquals(refFuncIds, funcIds);

    }

    /**
     * Tests getColumnFromTable: case selecting the content values
     *
     * @throws Exception
     */
    @Test
    public void testGetOtherColumnsFromTable() throws Exception {

        TableManager mng = new TableManager();

        TableInfo tableJson = mng.loadFromJson(TableManagerTest.JSON_CONTENT_SAMPLE_1);

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

        assertEquals(refOtherDecimal, otherDecimal);
        assertEquals(-50.0d, otherDecimal.get(0).doubleValue(), 0.0d);

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

    /**
     * Tests getting a column with col header name, from a table with columns header and without rows header.
     *
     * @throws Exception
     */
    @Test
    public void testGetColumnFromHeaderName() throws Exception {

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

    /**
     * Tests getRow service
     *
     * @throws Exception
     */
    @Test
    public void testGetRowFromTable() throws Exception {

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

        assertEquals(selectedRowVals, selectedRowValsBis);
        assertEquals(selectedRowVals, Arrays.asList("VIB2", -50.0, 12.1, 1.0, 3.4));

        assertEquals(selectedRowVals, ref);
    }

    /**
     * Tests initCsvLikeTable: case of the creation of a simple-csv Table: with one column header and simple rows
     * (without row header).
     *
     * @throws Exception
     */
    @Test
    public void testInitTableSimple() throws Exception {

        TableManager mng = new TableManager();
        TableInfo table = new TableInfo();
        Table tableH = mng.initTable(table, false);

        // Deprecated for end-user
        tableH.initColumnsHeader(true, null, new ArrayList<>(), null).addItem("One", null).addItem("Two", null)
                .addItem("Three", null);
        tableH.initContent(false, null);

        // Simple initializer
        Table tableHBis = mng.initTable(Arrays.asList("One", "Two", "Three"), false);

        Object[] row1 = new Object[]{"One", new Double(2.0), Boolean.FALSE};

        Double[] row2 = new Double[]{1.0, 2.2, 3.5};

        Boolean[] row3 = new Boolean[]{Boolean.TRUE, false, Boolean.TRUE};

        tableH.appendRow(Arrays.asList(row1));
        tableH.appendRow(Arrays.asList(row2));
        tableH.appendRow(Arrays.asList(row3));
        tableHBis.appendRow(Arrays.asList(row1));
        tableHBis.appendRow(Arrays.asList(row2));
        tableHBis.appendRow(Arrays.asList(row3));

        assertEquals(mng.serializeToJson(table), mng.serializeToJson(tableHBis.getTableInfo()));
    }

    /**
     * Tests initCsvLikeTable: case of the creation of a simple-csv Table: with column header and rows header.
     *
     * @throws Exception
     */
    @Test
    public void testInitTableWithRowsHeader() throws Exception {

        TableManager mng = new TableManager();
        TableInfo table = new TableInfo();
        Table tableH = mng.initTable(table, false);

        tableH.initColumnsHeader(true, null, new ArrayList<>(), null).addItem("Above row header", null)
                .addItem("One", null).addItem("Two", null).addItem("Three", null);
        tableH.initRowsHeader(false, null, new ArrayList<>(), null);
        tableH.initContent(false, null);

        // Simplified initializer used with tableHBis
        Table tableHBis = mng.initTable(Arrays.asList(new String[]{"Above row header", "One", "Two", "Three"}),
                true);

        // Defining the content rows - excluding row header part-
        Object[] row1 = new Object[]{"One", new Double(2.0), Boolean.FALSE};

        Double[] row2 = new Double[]{1.0, 2.2, 3.5};

        Boolean[] row3 = new Boolean[]{Boolean.TRUE, false, Boolean.TRUE};

        // append content rows + defines headers "A" "B" ...
        tableH.appendRow("A", Arrays.asList(row1));
        tableH.appendRow("B", Arrays.asList(row2));
        tableH.appendRow("C", Arrays.asList(row3));
        tableHBis.appendRow("A", Arrays.asList(row1));
        tableHBis.appendRow("B", Arrays.asList(row2));
        tableHBis.appendRow("C", Arrays.asList(row3));

        assertEquals(mng.serializeToJson(table), mng.serializeToJson(tableHBis.getTableInfo()));

        List<Object> columnnTwo = tableH.getColumn("Two", Object.class);

        assertEquals(columnnTwo, Arrays.asList(new Object[]{2.0, 2.2, false}));

        List<String> columnnOfRowHeaders = tableH.getColumn("Above row header");

        assertEquals(columnnOfRowHeaders, Arrays.asList("A", "B", "C"));

    }

    /**
     * Tests the init of Table handling links and headers. Added at the end of test: the getters on TableElements by Row
     * or by Column.
     *
     * @throws Exception
     */
    @Test
    public void testInitTableWithRowsHeaderWithLinks() throws Exception {

        TableManager mng = new TableManager();
        TableInfo table = new TableInfo();
        Table tableH = mng.initTable(table, false);

        DataLink defColH = new DataLink();
        defColH.context = "conf col header link";
        DataLink defRowH = new DataLink();
        defRowH.context = "conf row header link";
        DataLink defContent = new DataLink();
        defContent.context = "conf content link";

        tableH.initColumnsHeader(true, null, new ArrayList<>(), null).addItem("Above row header", null)
                .addItem("One", null).addItem("Two", null).addItem("Three", null);
        tableH.initRowsHeader(false, null, new ArrayList<>(), null);
        tableH.initContent(false, null);
        tableH.enableLinks(true, defColH, true, defRowH, true, defContent);

        // Simplified initializer used with tableHBis
        Table tableHBis = mng.initTable(Arrays.asList(new String[]{"Above row header", "One", "Two", "Three"}),
                true);
        tableHBis.enableLinks(true, defColH, true, defRowH, true, defContent);

        // Defining the content rows - excluding row header part-
        Object[] row1 = new Object[]{"One", new Double(2.0), Boolean.FALSE};

        // Row2 ....
        DataLink linkOne = new DataLink();
        linkOne.context = "ctx 1";
        linkOne.val = "val 1";

        DataLink linkTwo = new DataLink();
        linkTwo.type = "typ 2";
        linkTwo.val = "val 2";

        // ... this row has defined links ...
        List<TableElement> row2AsList = TableElement.encodeElements(new TableElement("Prem", linkOne),
                new TableElement("Sec", linkTwo), "Tri");

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

        assertEquals(mng.serializeToJson(table), mng.serializeToJson(tableHBis.getTableInfo()));

        List<Object> columnnTwo = tableH.getColumn("Two", Object.class);

        assertEquals(columnnTwo, Arrays.asList(new Object[]{row1[1], row2AsList.get(1).data, row3.get(1).data}));
        assertEquals(columnnTwo, tableHBis.getColumn("Two", Object.class));

        // Tests link selection: linkTwo.type= "typ 2";
        // linkTwo.val= "val 2";
        // assertEquals( tableH.cells.links.get(1).get(1).type,
        // tableHBis.getColumnFromTable("Two") );

        List<String> columnnOfRowHeaders = tableH.getColumn("Above row header");

        assertEquals(columnnOfRowHeaders, Arrays.asList(new Object[]{"A", "B", "C"}));

        // added test for getRow getting TableElement
        List<TableElement> elemsInB = tableHBis.getRow("B", TableElement.class);
        assertEquals("Prem", elemsInB.get(0).data);
        assertEquals(linkOne, elemsInB.get(0).link);

        // added test for getRow getting TableElement from index
        List<TableElement> elemsInBfromIndex = tableHBis.getRow(2, TableElement.class);
        assertEquals("Prem", elemsInBfromIndex.get(0).data);
        assertEquals(linkOne, elemsInBfromIndex.get(0).link);

        // added test for getRow getting TableElement from index
        List<TableElement> elemsContentRow1 = tableHBis.getContentRow(1, TableElement.class);
        assertEquals("Prem", elemsContentRow1.get(0).data);
        assertEquals(linkOne, elemsContentRow1.get(0).link);

        // added test for getColumn getting TableElement
        List<TableElement> elemsInOne = tableHBis.getColumn("One", TableElement.class);
        assertEquals("Prem", elemsInOne.get(1).data);
        assertEquals(linkOne, elemsInOne.get(1).link);

        // added test for getColumn getting TableElement
        List<TableElement> elemsContent0 = tableHBis.getContentColumn(0, TableElement.class);
        assertEquals("Prem", elemsContent0.get(1).data);
        assertEquals(linkOne, elemsContent0.get(1).link);

        // added test for getColumn getting TableElement from index
        List<TableElement> elemsInOnefromIndex = tableHBis.getColumn(1, TableElement.class);
        assertEquals("Prem", elemsInOnefromIndex.get(1).data);
        assertEquals(linkOne, elemsInOnefromIndex.get(1).link);

    }

    /**
     * tests simple case of appendRow, with only data part, without link. @throws
     *
     * @throws Exception
     */
    @Test
    public void testAppendRowWithoutLinks() throws Exception {

        TableManager mng = new TableManager();

        mng = new TableManager();

        TableInfo table = mng.loadFromJson(TableManagerTest.JSON_CONTENT_SAMPLE_1);

        Table tableH = mng.initTable(table, false);
        int initialRowCount = tableH.getRowCount(true);
        int initialColumnCount = tableH.getColumnCount(true);

        List<Object> addedList = new ArrayList<>();
        for (int i = 0; i < 4; i++)
            addedList.add("item" + i);

        // Should accept different types in a row:
        // => insert a different Type: int instead of String
        addedList.add(10);

        // Tests appended row with not links
        String addedRowHeaderData = "AddedRow";
        tableH.appendRow(addedRowHeaderData, addedList);

        int finalRowCount = tableH.getRowCount(true);
        int finalColumnCount = tableH.getColumnCount(true);

        assertTrue(initialColumnCount == finalColumnCount);
        assertTrue(initialRowCount + 1 == finalRowCount);

        assertEquals(table.headers.row.data.get(finalRowCount - 1), addedRowHeaderData);
        assertEquals(table.content.cells.get(finalRowCount - 2), addedList);

    }

    /**
     * Tests the getColumn services
     *
     * @throws Exception
     */
    @Test
    public void testGetColumn() throws Exception {

        TableManager mng = new TableManager();
        Table myT = mng.initTable(Arrays.asList("One", "Two", "Three"), false);
        for (int i = 0; i < 10; i++) {
            myT.appendRow(Arrays.asList(i < 5, "" + i, null));
        }

        List<Boolean> strOneList = myT.getColumn("One", Boolean.class);
        List<String> strOneListAString = myT.getColumn("One");

        try {
            myT.getColumn("One", BigDecimal.class);
            fail("Incorrect: class cast exception not detected !");
        } catch (IkatsException e) {
            assertTrue(true);
        } catch (Exception e) {
            fail("Unexpected exception");
        }

        assert (strOneList.get(0) instanceof Boolean);
        assert (strOneListAString.get(0) instanceof String);

    }

    /**
     * Tests the getRow services
     *
     * @throws Exception
     */
    @Test
    public void testGetRow() throws Exception {

        Table myT = TableManager.initEmptyTable(true, true);
        myT.getColumnsHeader().addItem("One").addItem("Two").addItem("Three");
        for (int i = 0; i < 10; i++) {
            myT.appendRow("row" + i, Arrays.asList(i, i + 2));
        }

        List<Integer> strOneList = myT.getRow("row1", Integer.class);
        List<String> strOneListAString = myT.getRow("row1");
        try {
            myT.getRow("row2", BigDecimal.class);
            fail("Incorrect: class cast exception not detected !");
        } catch (IkatsException e) {
            // testGetRow: Got expected exception
        } catch (Exception e) {
            fail("Unexpected exception");
        }

        assert (strOneList.get(0) instanceof Integer);
        assert (strOneList.get(0).equals(0));
        assert (strOneList.get(1).equals(2));
        assert (strOneListAString.get(0) instanceof String);
        assert (strOneListAString.get(0).equals("0"));
        assert (strOneListAString.get(1).equals("2"));
    }

    /**
     * Tests the getRow services
     *
     * @throws Exception
     */
    @Test
    public void testGetRowsHeaderItems() throws Exception {

        Table myT = TableManager.initEmptyTable(false, true);

        myT.getRowsHeader().addItems("0", "1", "2", "3", "4");
        for (int i = 0; i < 10; i++) {
            myT.appendColumn(Arrays.asList("a" + i, "b" + i, "c" + i, "d" + i, "e" + i));
        }

        assertEquals("0", myT.getRowsHeader().getItems().get(0));
        assertEquals("4", myT.getRowsHeader().getItems().get(4));

        assertEquals("a0", myT.getRow("0").get(0));
        assertEquals("a5", myT.getRow("0").get(5));

        // Note: we could manage also myT.getRowsHeader().addItems(0, 1, 2, 3, 4);
        // ...
        // in that case: we also retrieve the row using myT.getRow("0") instead of myT.getRow(0)
        // => this will work

    }

    /**
     * Tests the getColumn services:
     *
     * @throws Exception
     */
    @Test
    public void testGetColumnsHeaderItems() throws Exception {

        Table myT = TableManager.initEmptyTable(true, false);

        // unusual case: handling Integer -instead of String- in header data ... why not ...
        myT.getColumnsHeader().addItems(0, 1, 2, 3, 8);
        for (int i = 0; i < 10; i++) {
            myT.appendRow(Arrays.asList("a" + i, "b" + i, "c" + i, "d" + i, "e" + i));
        }

        // getting effective header data
        assertEquals(new Integer(0), myT.getColumnsHeader().getItems(Integer.class).get(0));
        assertEquals(new Integer(8), myT.getColumnsHeader().getItems(Integer.class).get(4));
        // getting header data as string
        assertEquals("8", myT.getColumnsHeader().getItems().get(4));

        // a bit weird but the key for header 0 is toString() representation "0"

        // usual case: handling Strings in header data
        Table myUsualT = TableManager.initEmptyTable(true, false);
        myUsualT.getColumnsHeader().addItems("10", "1", "2", "3", "8");
        for (int i = 0; i < 10; i++) {
            myUsualT.appendRow(Arrays.asList("a" + i, "b" + i, "c" + i, "d" + i, "e" + i));
        }

        assertEquals("10", myUsualT.getColumnsHeader().getItems().get(0));
        assertEquals("8", myUsualT.getColumnsHeader().getItems().get(4));

    }

    /**
     * Tests sort algo based upon column values from table content, - not from the row header - Ascending order is
     * tested here.
     *
     * @throws Exception
     */
    @Test
    public void testSortRowswithoutHeaders() throws Exception {

        Table myTWithoutRowHeader = TableManager.initEmptyTable(false, false);

        myTWithoutRowHeader.appendRow(Arrays.asList("bla3", "BLAH3", 3.5));
        myTWithoutRowHeader.appendRow(Arrays.asList("bla2", "BLAH2", 2.0));
        myTWithoutRowHeader.appendRow(Arrays.asList("bla4", "BLAH4", 3.7));
        myTWithoutRowHeader.appendRow(Arrays.asList("bla6", "BLAH6", 6.0));
        myTWithoutRowHeader.appendRow(Arrays.asList("-bla6", "-BLAH6", -6.0));

        myTWithoutRowHeader.sortRowsByColumnValues(2, false);

        assertEquals(Arrays.asList(-6.0, 2.0, 3.5, 3.7, 6.0), myTWithoutRowHeader.getColumn(2, Double.class));

        assertEquals(Arrays.asList("-bla6", "-BLAH6", -6.0), myTWithoutRowHeader.getRow(0, Object.class));
        assertEquals(Arrays.asList("bla2", "BLAH2", 2.0), myTWithoutRowHeader.getRow(1, Object.class));
        assertEquals(Arrays.asList("bla3", "BLAH3", 3.5), myTWithoutRowHeader.getRow(2, Object.class));
        assertEquals(Arrays.asList("bla4", "BLAH4", 3.7), myTWithoutRowHeader.getRow(3, Object.class));
        assertEquals(Arrays.asList("bla6", "BLAH6", 6.0), myTWithoutRowHeader.getRow(4, Object.class));

    }

    /**
     * Tests sort algo based upon column values from table content, - not from the row header - Ascending order is
     * tested here.
     *
     * @throws Exception
     */
    @Test
    public void testSortRowsWithColHeader() throws Exception {

        Table myTWithoutRowHeader = TableManager.initEmptyTable(true, false);

        // unusual case: handling Integer -instead of String- in header data ... why not ...
        myTWithoutRowHeader.getColumnsHeader().addItems("First", "Blabla", "Order");

        myTWithoutRowHeader.appendRow(Arrays.asList("bla3", "BLAH3", 3));
        myTWithoutRowHeader.appendRow(Arrays.asList("bla2", "BLAH2", 2));
        myTWithoutRowHeader.appendRow(Arrays.asList("bla4", "BLAH4", 4));
        myTWithoutRowHeader.appendRow(Arrays.asList("bla6", "BLAH6", 6));
        myTWithoutRowHeader.appendRow(Arrays.asList("-bla6", "-BLAH6", -6));

        myTWithoutRowHeader.sortRowsByColumnValues("Order", false);

        assertEquals(Arrays.asList(-6, 2, 3, 4, 6), myTWithoutRowHeader.getColumn("Order", Integer.class));

        assertEquals(Arrays.asList("-bla6", "-BLAH6", -6), myTWithoutRowHeader.getRow(1, Object.class));
        assertEquals(Arrays.asList("bla2", "BLAH2", 2), myTWithoutRowHeader.getRow(2, Object.class));
        assertEquals(Arrays.asList("bla3", "BLAH3", 3), myTWithoutRowHeader.getRow(3, Object.class));
        assertEquals(Arrays.asList("bla4", "BLAH4", 4), myTWithoutRowHeader.getRow(4, Object.class));
        assertEquals(Arrays.asList("bla6", "BLAH6", 6), myTWithoutRowHeader.getRow(5, Object.class));

    }

    /**
     * Tests sort algo based upon column values from table content, - not from the row header - Ascending order is
     * tested here.
     *
     * @throws Exception
     */
    @Test
    public void testSortRowsWithAllHeaders() throws Exception {

        Table myTWithoutRowHeader = TableManager.initEmptyTable(true, true);

        // unusual case: handling Integer -instead of String- in header data ... why not ...
        myTWithoutRowHeader.getColumnsHeader().addItems("TopLeft", "First", "Blabla", "Order");
        // needs to define the top left corner as undefined !
        myTWithoutRowHeader.getRowsHeader().addItem(null);

        myTWithoutRowHeader.appendRow("B1.1", Arrays.asList("bla3", "BLAH3", 3));
        myTWithoutRowHeader.appendRow("B1", Arrays.asList("bla2", "BLAH2", 2));
        myTWithoutRowHeader.appendRow("A100", Arrays.asList("bla4", "BLAH4", 4));
        myTWithoutRowHeader.appendRow("A10", Arrays.asList("bla6", "BLAH6", 6));
        myTWithoutRowHeader.appendRow("A2", Arrays.asList("-bla6", "-BLAH6", -6));

        myTWithoutRowHeader.sortRowsByColumnValues("Order", false);

        assertEquals(Arrays.asList(-6, 2, 3, 4, 6), myTWithoutRowHeader.getColumn("Order", Integer.class));

        assertEquals(Arrays.asList("-bla6", "-BLAH6", -6), myTWithoutRowHeader.getRow(1, Object.class));
        assertEquals(Arrays.asList("bla2", "BLAH2", 2), myTWithoutRowHeader.getRow(2, Object.class));
        assertEquals(Arrays.asList("bla3", "BLAH3", 3), myTWithoutRowHeader.getRow(3, Object.class));
        assertEquals(Arrays.asList("bla4", "BLAH4", 4), myTWithoutRowHeader.getRow(4, Object.class));
        assertEquals(Arrays.asList("bla6", "BLAH6", 6), myTWithoutRowHeader.getRow(5, Object.class));

        myTWithoutRowHeader.sortRowsByColumnValues("TopLeft", false);

        assertEquals(Arrays.asList("A2", "A10", "A100", "B1", "B1.1"),
                myTWithoutRowHeader.getColumn("TopLeft", String.class));

    }

    /**
     * Test insertColumn with all headers activated
     *
     * @throws Exception
     */
    @Test
    public void testInsertColumnWithAllHeaders() throws Exception {

        // both headers are managed
        Table myTable = TableManager.initEmptyTable(true, true);

        // unusual case: handling Integer -instead of String- in header data ... why not ...
        myTable.getColumnsHeader().addItems("TopLeft", "First", "Blabla", "Order");
        // needs to define the top left corner as undefined !
        myTable.getRowsHeader().addItem(null);

        myTable.appendRow("B1.1", Arrays.asList("bla3", "BLAH3", 3));
        myTable.appendRow("B1", Arrays.asList("bla2", "BLAH2", 2));
        myTable.appendRow("A100", Arrays.asList("bla4", "BLAH4", 4));
        myTable.appendRow("A10", Arrays.asList("bla6", "BLAH6", 6));
        myTable.appendRow("A2", Arrays.asList("-bla6", "-BLAH6", -6));

        myTable.insertColumn("Blabla", "Bazar", Arrays.asList(Boolean.TRUE, Boolean.FALSE, "text", null, 3.14));

        assertEquals(Arrays.asList("TopLeft", "First", "Bazar", "Blabla", "Order"),
                myTable.getColumnsHeader().getItems());
        assertEquals(Arrays.asList(true, false, "text", null, 3.14), myTable.getColumn("Bazar", Object.class));
    }

    /**
     * Test insertColumn() with only the column header
     *
     * @throws Exception
     */
    @Test
    public void testInsertColumnWithColHeader() throws Exception {

        Table myTWithColHeader = TableManager.initEmptyTable(true, false);

        // unusual case: handling Integer -instead of String- in header data ... why not ...
        myTWithColHeader.getColumnsHeader().addItems("First", "Blabla", "Order");

        myTWithColHeader.appendRow(Arrays.asList("bla3", "BLAH3", 3));
        myTWithColHeader.appendRow(Arrays.asList("bla2", "BLAH2", 2));
        myTWithColHeader.appendRow(Arrays.asList("bla4", "BLAH4", 4));
        myTWithColHeader.appendRow(Arrays.asList("bla6", "BLAH6", 6));
        myTWithColHeader.appendRow(Arrays.asList("-bla6", "-BLAH6", -6));

        myTWithColHeader.insertColumn("Blabla", "Bazar",
                Arrays.asList(Boolean.TRUE, Boolean.FALSE, "text", null, 3.14));

        assertEquals(Arrays.asList("First", "Bazar", "Blabla", "Order"),
                myTWithColHeader.getColumnsHeader().getItems());
        assertEquals(Arrays.asList(true, false, "text", null, 3.14), myTWithColHeader.getColumn("Bazar", Object.class));

    }

    /**
     * Tests insertColumn() with index, and no headers at all
     *
     * @throws Exception
     */
    @Test
    public void testInsertColumnWithoutHeader() throws Exception {

        // no headers
        Table myTable = TableManager.initEmptyTable(false, false);

        myTable.appendRow(Arrays.asList("bla3", "BLAH3", 3));
        myTable.appendRow(Arrays.asList("bla2", "BLAH2", 2));
        myTable.appendRow(Arrays.asList("bla4", "BLAH4", 4));
        myTable.appendRow(Arrays.asList("bla6", "BLAH6", 6));
        myTable.appendRow(Arrays.asList("-bla6", "-BLAH6", -6));

        myTable.insertColumn(1, Arrays.asList(Boolean.TRUE, Boolean.FALSE, "text", null, 3.14));

        assertEquals(Arrays.asList(true, false, "text", null, 3.14), myTable.getColumn(1, Object.class));

    }

    /**
     * Tests the insertRow() without header
     *
     * @throws Exception
     */
    @Test
    public void testInsertRowWithoutHeader() throws Exception {

        // no headers
        Table myTable = TableManager.initEmptyTable(false, false);

        myTable.appendRow(Arrays.asList("bla3", "BLAH3", 3));
        myTable.appendRow(Arrays.asList("bla2", "BLAH2", 2));
        myTable.appendRow(Arrays.asList("bla4", "BLAH4", 4));
        myTable.appendRow(Arrays.asList("bla6", "BLAH6", 6));
        myTable.appendRow(Arrays.asList("-bla6", "-BLAH6", -6));

        myTable.insertRow(1, Arrays.asList("avantBla2", Boolean.FALSE, "text"));

        assertEquals(Arrays.asList("avantBla2", Boolean.FALSE, "text"), myTable.getRow(1, Object.class));
        assertEquals(Arrays.asList("bla2", "BLAH2", 2), myTable.getRow(2, Object.class));

    }

    /**
     * Test insertRow with all headers activated
     *
     * @throws Exception
     */
    @Test
    public void testInsertRowWithAllHeaders() throws Exception {

        Table myTable = TableManager.initEmptyTable(true, true);

        // unusual case: handling Integer -instead of String- in header data ... why not ...
        myTable.getColumnsHeader().addItems("TopLeft", "First", "Blabla", "Order");
        // needs to define the top left corner as undefined !
        myTable.getRowsHeader().addItem(null);

        myTable.appendRow("B1.1", Arrays.asList("bla3", "BLAH3", 3));
        myTable.appendRow("B1", Arrays.asList("bla2", "BLAH2", 2));
        myTable.appendRow("A100", Arrays.asList("bla4", "BLAH4", 4));
        myTable.appendRow("A10", Arrays.asList("bla6", "BLAH6", 6));
        myTable.appendRow("A2", Arrays.asList("-bla6", "-BLAH6", -6));

        myTable.insertRow("A100", "Bazar", Arrays.asList(Boolean.TRUE, Boolean.FALSE, 3.14));

        assertEquals(Arrays.asList(null, "B1.1", "B1", "Bazar", "A100", "A10", "A2"),
                myTable.getRowsHeader().getItems());
        assertEquals(Arrays.asList(true, false, 3.14), myTable.getRow("Bazar", Object.class));
        assertEquals(Arrays.asList(true, false, 3.14), myTable.getRow(3, Object.class));
    }


    /**
     * Tests the List all Tables service with no result
     */

    @Test
    public void testListTablesEmpty() throws Exception {

        TableManager mng = new TableManager();
        List<TableEntitySummary> result = mng.listTables();

        assertNotNull(result);
        assertEquals(0, result.size());
    }

    /**
     * Tests the List all Table service with result
     */
    @Test
    public void testListTablesNotEmpty() throws Exception {

        TableManager mng = new TableManager();

        TableInfo table = new TableInfo();
        Table tableH = mng.initTable(table, false);

        // Deprecated for end-user
        tableH.initColumnsHeader(true, null, new ArrayList<>(), null)
                .addItem("One", null)
                .addItem("Two", null)
                .addItem("Three", null);
        tableH.initContent(false, null);

        // Simple initializer
        Table tableHBis = mng.initTable(Arrays.asList("One", "Two", "Three"), false);

        Object[] row1 = new Object[]{"One", new Double(2.0), Boolean.FALSE};

        Double[] row2 = new Double[]{1.0, 2.2, 3.5};

        Boolean[] row3 = new Boolean[]{Boolean.TRUE, false, Boolean.TRUE};

        tableH.appendRow(Arrays.asList(row1));
        tableH.appendRow(Arrays.asList(row2));
        tableH.appendRow(Arrays.asList(row3));
        tableHBis.appendRow(Arrays.asList(row1));
        tableHBis.appendRow(Arrays.asList(row2));
        tableHBis.appendRow(Arrays.asList(row3));

        tableH.setName("TestTable");
        mng.createInDatabase(tableH.getTableInfo());

        List<TableEntitySummary> result = mng.listTables();

        assertNotNull(result);
        assertEquals(1, result.size());

        // clean
        mng.deleteFromDatabase("TestTable");

    }

    /**
     * Tests the Delete a table service with result
     */

    @Test
    public void testDeleteTable() throws Exception {

        TableManager mng = new TableManager();

        TableInfo table = new TableInfo();
        Table tableH = mng.initTable(table, false);

        // Deprecated for end-user
        tableH.initColumnsHeader(true, null, new ArrayList<>(), null)
                .addItem("One", null)
                .addItem("Two", null)
                .addItem("Three", null);
        tableH.initContent(false, null);

        // Simple initializer
        Table tableHBis = mng.initTable(Arrays.asList("One", "Two", "Three"), false);

        Object[] row1 = new Object[]{"One", new Double(2.0), Boolean.FALSE};

        Double[] row2 = new Double[]{1.0, 2.2, 3.5};

        Boolean[] row3 = new Boolean[]{Boolean.TRUE, false, Boolean.TRUE};

        tableH.appendRow(Arrays.asList(row1));
        tableH.appendRow(Arrays.asList(row2));
        tableH.appendRow(Arrays.asList(row3));
        tableHBis.appendRow(Arrays.asList(row1));
        tableHBis.appendRow(Arrays.asList(row2));
        tableHBis.appendRow(Arrays.asList(row3));

        String tableName = "TestTableToDelete";
        tableH.setName(tableName);
        mng.createInDatabase(tableH.getTableInfo());

        List<TableEntitySummary> resultBefore = mng.listTables();
        assertNotNull(resultBefore);

        // clean
        mng.deleteFromDatabase(tableName);

        List<TableEntitySummary> resultAfter = mng.listTables();
        assertNotNull(resultAfter);
        assertEquals(resultBefore.size() - 1, resultAfter.size());
    }

    /**
     * Tests creation and retrieval of a table in db
     */
    @Test
    public void testCreateTable() throws Exception {

        TableManager mng = new TableManager();

        TableInfo table = new TableInfo();
        Table tableH = mng.initTable(table, false);

        String tableName = "TestTable";
        String tableDesc = "TestDescription";
        String tableTitle = "TestTitle";

        tableH.setName(tableName);
        tableH.setDescription(tableDesc);
        tableH.setTitle(tableTitle);

        // Deprecated for end-user
        tableH.initColumnsHeader(true, null, new ArrayList<>(), null)
                .addItem("Columns", null)
                .addItem("C_One", null)
                .addItem("C_Two", null)
                .addItem("C_Three", null);
        tableH.initRowsHeader(true, null, new ArrayList<>(), null)
                .addItem("Rows", null)
                .addItem("R_One", null)
                .addItem("R_Two", null)
                .addItem("R_Three", null);
        tableH.initContent(false, null);

        // Simple initializer
        Table tableHBis = mng.initTable(Arrays.asList("One", "Two", "Three"), false);

        Object[] row1 = new Object[]{1, 2, 3};
        Object[] row2 = new Object[]{4, 5, 5};
        Object[] row3 = new Object[]{7, 8, 9};

        tableH.appendRow(Arrays.asList(row1));
        tableH.appendRow(Arrays.asList(row2));
        tableH.appendRow(Arrays.asList(row3));
        tableHBis.appendRow(Arrays.asList(row1));
        tableHBis.appendRow(Arrays.asList(row2));
        tableHBis.appendRow(Arrays.asList(row3));

        mng.createInDatabase(tableH.getTableInfo());

        TableInfo result = mng.readFromDatabase("TestTable");
        Table resultTable = mng.initTable(result, false);

        assertEquals(Arrays.asList("Rows", "R_One", "R_Two", "R_Three"), resultTable.getRowsHeader().getItems());
        assertEquals(Arrays.asList("Columns", "C_One", "C_Two", "C_Three"), resultTable.getColumnsHeader().getItems());
        assertEquals(Arrays.asList(row1), resultTable.getRow("R_One", Object.class));
        assertEquals(Arrays.asList(row2), resultTable.getRow("R_Two", Object.class));
        assertEquals(Arrays.asList(row3), resultTable.getRow("R_Three", Object.class));

        assertEquals(tableName, resultTable.getName());
        assertEquals(tableDesc, resultTable.getDescription());
        assertEquals(tableTitle, resultTable.getTitle());

        // clean
        mng.deleteFromDatabase("TestTable");

    }


    @Test
    public void testCreateTableOnlyRowHeader() throws Exception {
        TableManager mng = new TableManager();
        TableInfo tableRow = mng.loadFromJson(JSON_CONTENT_SAMPLE_rowHeaders);
        tableRow.table_desc.name = "tableOnlyRowHeader";
        mng.createInDatabase(tableRow);
        TableInfo result = mng.readFromDatabase("tableOnlyRowHeader");

        assertEquals(Arrays.asList("Index", "R_one", "R_two", "R_three"), result.headers.row.data);
        assertEquals(Arrays.asList("C_one", "C_two", "C_three"), result.content.cells.get(0));
        assertEquals(Arrays.asList("1", "2", "3"), result.content.cells.get(1));
        assertEquals(Arrays.asList("4", "5", "5"), result.content.cells.get(2));
        assertEquals(Arrays.asList("6", "7", "8"), result.content.cells.get(3));
        assertEquals(result.headers.row.data.size(), result.content.cells.size());
        // clean
        mng.deleteFromDatabase("tableOnlyRowHeader");
    }

    @Test
    public void testCreateTableOnlyColHeader() throws Exception {
        TableManager mng = new TableManager();
        TableInfo tableCol = mng.loadFromJson(JSON_CONTENT_SAMPLE_colHeaders);
        tableCol.table_desc.name = "tableOnlyColHeader";
        mng.createInDatabase(tableCol);

        TableInfo result = mng.readFromDatabase("tableOnlyColHeader");

        assertEquals(Arrays.asList("Index", "C_one", "C_two", "C_three"), result.headers.col.data);
        assertEquals(Arrays.asList("R_one", "1", "2", "3"), result.content.cells.get(0));
        assertEquals(Arrays.asList("R_two", "4", "5", "5"), result.content.cells.get(1));
        assertEquals(Arrays.asList("R_three", "6", "7", "8"), result.content.cells.get(2));
        // clean
        mng.deleteFromDatabase("tableOnlyColHeader");
    }


    @Test
    public void testCreateTableHeaders() throws Exception {
        TableManager mng = new TableManager();
        TableInfo tableHead = mng.loadFromJson(JSON_CONTENT_SAMPLE_headers);
        tableHead.table_desc.name = "tableAllHeaders";
        mng.createInDatabase(tableHead);

        TableInfo result = mng.readFromDatabase("tableAllHeaders");

        assertEquals(Arrays.asList("Index", "C_one", "C_two", "C_three"), result.headers.col.data);
        assertEquals(Arrays.asList(null, "R_one", "R_two", "R_three"), result.headers.row.data);
        assertTrue(result.headers.row.data.get(0) == null);
        assertEquals(Arrays.asList("1", "2", "3"), result.content.cells.get(0));
        assertEquals(Arrays.asList("4", "5", "5"), result.content.cells.get(1));
        assertEquals(Arrays.asList("6", "7", "8"), result.content.cells.get(2));
        // clean
        mng.deleteFromDatabase("tableAllHeaders");
    }

    @Test
    public void testCreateTableNoHeader() throws Exception {
        TableManager mng = new TableManager();
        TableInfo tableNoHeader = mng.loadFromJson(JSON_CONTENT_SAMPLE_no_Headers);
        tableNoHeader.table_desc.name = "tableNoHeader";
        mng.createInDatabase(tableNoHeader);

        TableInfo result = mng.readFromDatabase("tableNoHeader");

        assertEquals(Arrays.asList("Index", "C_one", "C_two", "C_three"), result.content.cells.get(0));
        assertEquals(Arrays.asList("R_one", "1", "2", "3"), result.content.cells.get(1));
        assertEquals(Arrays.asList("R_two", "4", "5", "5"), result.content.cells.get(2));
        assertEquals(Arrays.asList("R_three", "6", "7", "8"), result.content.cells.get(3));
        // clean
        mng.deleteFromDatabase("tableNoHeader");
    }


}
