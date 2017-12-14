/**
 * LICENSE:
 * --------
 * Copyright 2017 CS SYSTEMES D'INFORMATION
 *
 * Licensed to CS SYSTEMES D'INFORMATION under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. CS licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * @author Fabien TORAL <fabien.toral@c-s.fr>
 * @author Fabien TORTORA <fabien.tortora@c-s.fr>
 * @author Mathieu BERAUD <mathieu.beraud@c-s.fr>
 * @author Maxime PERELMUTER <maxime.perelmuter@c-s.fr>
 * @author Pierre BONHOURE <pierre.bonhoure@c-s.fr>
 *
 */

package fr.cs.ikats.temporaldata.business;

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
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.table.TableEntitySummary;
import fr.cs.ikats.temporaldata.business.TableInfo.DataLink;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;

/**
 * TableManagerTest tests the TableManager and its end-user services.
 */
public class TableManagerTest {

    /**
     * verbose == true enables more logs (to sysout), for instance in DEV environment, debugging the JUnit tests.
     * Expected for usual deployment: verbose == false for usual tests, not requiring displays.
     */
    private static boolean verbose = false;

    /**
     * Do not change this sample: reused by several tests: for new purposes => create another one
     */
    private final static String JSON_CONTENT_SAMPLE_1 = "{\"table_desc\":{\"title\":\"Discretized matrix\",\"desc\":\"This is a ...\"},\"headers\":{\"col\":{\"data\":[\"funcId\",\"metric\",\"min_B1\",\"max_B1\",\"min_B2\",\"max_B2\"],\"links\":null,\"default_links\":null},\"row\":{\"data\":[null,\"Flid1_VIB2\",\"Flid1_VIB3\",\"Flid1_VIB4\",\"Flid1_VIB5\"],\"default_links\":{\"type\":\"ts_bucket\",\"context\":\"processdata\"},\"links\":[null,{\"val\":\"1\"},{\"val\":\"2\"},{\"val\":\"3\"},{\"val\":\"4\"}]}},\"content\":{\"cells\":[[\"VIB2\",-50.0,12.1,1.0,3.4],[\"VIB3\",-5.0,2.1,1.0,3.4],[\"VIB4\",0.0,2.1,12.0,3.4],[\"VIB5\",0.0,2.1,1.0,3.4]]}}";

    /**
     * test TrainTestSplitTable
     * cases:
     * - only column headers present
     * - a  class with only one element
     * - duplicates ids
     * - 4 classes
     */
    @Test
    public void testTrainTestSplitTableNominal() throws Exception {

        TableManager tableManager = new TableManager();
        String tableContent = "MainId;Target\n"
                + "125;A\n"
                + "1;A\n"
                + "2;A\n"
                + "2;A\n"
                + "3;B\n"
                + "4;B\n"
                + "42;C\n"
                + "6;D\n"
                + "7;D\n"
                + "8;D\n";

        Table tableIn = tableFromCSV("tableTestIn", tableContent, false);

        List<Table> result;
        double repartitionRate = 0.56;
        result = tableManager.trainTestSplitTable(tableIn, "Target", repartitionRate);

        // Collect all classes of each table result to check repartition rate
        List<Object> classList1 = new ArrayList<>();
        List<Object> classList2 = new ArrayList<>();
        for (int i = 0; i < result.get(0).getContentData().size(); i++) {
            classList1.add(result.get(0).getContentData().get(i).get(1));
        }
        // checking repartition rate of each class in result
        assertEquals(2, Collections.frequency(classList1, "A"));
        assertEquals(1, Collections.frequency(classList1, "B"));
        assertEquals(1, Collections.frequency(classList1, "C"));
        assertEquals(2, Collections.frequency(classList1, "D"));

        for (int i = 0; i < result.get(1).getContentData().size(); i++) {
            classList2.add(result.get(1).getContentData().get(i).get(1));
        }
        // checking repartition rate of each class in result
        assertEquals(2, Collections.frequency(classList2, "A"));
        assertEquals(1, Collections.frequency(classList2, "B"));
        assertEquals(0, Collections.frequency(classList2, "C"));
        assertEquals(1, Collections.frequency(classList2, "D"));
    }

    /**
     * test TrainTestSplitTable
     * cases:
     * - only column headers present
     * - a  class with only one element
     * - duplicates ids
     * - 4 classes
     */
    @Test
    public void testTrainTestSplitTableSingleClass() throws Exception {

        TableManager tableManager = new TableManager();
        String tableContent = "MainId;Target\n"
                + "125;A\n"
                + "1;A\n"
                + "2;A\n"
                + "2;A\n"
                + "3;A\n"
                + "4;A\n"
                + "42;A\n"
                + "6;A\n"
                + "7;A\n";

        Table tableIn = tableFromCSV("tableTestIn", tableContent, false);

        List<Table> result;
        double repartitionRate = 0.24;
        result = tableManager.trainTestSplitTable(tableIn, "Target", repartitionRate);

        // Collect all classes of each table result to check repartition rate
        List<Object> classList1 = new ArrayList<>();
        List<Object> classList2 = new ArrayList<>();
        for (int i = 0; i < result.get(0).getContentData().size(); i++) {
            classList1.add(result.get(0).getContentData().get(i).get(1));
        }
        // checking repartition rate of each class in result
        assertEquals(2, Collections.frequency(classList1, "A"));

        for (int i = 0; i < result.get(1).getContentData().size(); i++) {
            classList2.add(result.get(1).getContentData().get(i).get(1));
        }
        // checking repartition rate of each class in result
        assertEquals(7, Collections.frequency(classList2, "A"));

    }

    /**
     * test TrainTestSplitTable
     * cases:
     * - only column headers present
     * - a  class with only one element
     * - duplicates ids
     * - 4 classes
     */
    @Test
    public void testTrainTestSplitTableTooHighRepartition() throws Exception {

        TableManager tableManager = new TableManager();
        String tableContent = "MainId;Target\n"
                + "125;A\n"
                + "1;A\n"
                + "2;A\n"
                + "2;A\n"
                + "3;A\n"
                + "4;A\n"
                + "42;A\n"
                + "6;A\n"
                + "7;A\n";

        Table tableIn = tableFromCSV("tableTestIn", tableContent, false);

        List<Table> result;
        double repartitionRate = 1.01;
        result = tableManager.trainTestSplitTable(tableIn, "Target", repartitionRate);

        // Collect all classes of each table result to check repartition rate
        List<Object> classList1 = new ArrayList<>();
        List<Object> classList2 = new ArrayList<>();
        for (int i = 0; i < result.get(0).getContentData().size(); i++) {
            classList1.add(result.get(0).getContentData().get(i).get(1));
        }
        // checking repartition rate of each class in result
        assertEquals(9, Collections.frequency(classList1, "A"));

        for (int i = 0; i < result.get(1).getContentData().size(); i++) {
            classList2.add(result.get(1).getContentData().get(i).get(1));
        }
        // checking repartition rate of each class in result
        assertEquals(0, Collections.frequency(classList2, "A"));

    }

    /**
     * test TrainTestSplitTable
     * cases:
     * - only column headers present
     * - a  class with only one element
     * - duplicates ids
     * - 4 classes
     */
    @Test
    public void testTrainTestSplitTableTooLowRepartition() throws Exception {

        TableManager tableManager = new TableManager();
        String tableContent = "MainId;Target\n"
                + "125;A\n"
                + "1;A\n"
                + "2;A\n"
                + "2;A\n"
                + "3;A\n"
                + "4;A\n"
                + "42;A\n"
                + "6;A\n"
                + "7;A\n";

        Table tableIn = tableFromCSV("tableTestIn", tableContent, false);

        List<Table> result;
        double repartitionRate = -0.01;
        result = tableManager.trainTestSplitTable(tableIn, "Target", repartitionRate);

        // Collect all classes of each table result to check repartition rate
        List<Object> classList1 = new ArrayList<>();
        List<Object> classList2 = new ArrayList<>();
        for (int i = 0; i < result.get(0).getContentData().size(); i++) {
            classList1.add(result.get(0).getContentData().get(i).get(1));
        }
        // checking repartition rate of each class in result
        assertEquals(0, Collections.frequency(classList1, "A"));

        for (int i = 0; i < result.get(1).getContentData().size(); i++) {
            classList2.add(result.get(1).getContentData().get(i).get(1));
        }
        // checking repartition rate of each class in result
        assertEquals(9, Collections.frequency(classList2, "A"));

    }

    /**
     * test TrainTestSplitTable
     * cases:
     * - only column headers present
     * - a  class with only one element
     * - duplicates ids
     * - 4 classes
     */
    @Test(expected = ResourceNotFoundException.class)
    public void testTrainTestSplitTableWrongTarget() throws Exception {

        TableManager tableManager = new TableManager();
        String tableContent = "MainId;Target\n"
                + "125;A\n"
                + "1;A\n"
                + "2;A\n"
                + "2;A\n"
                + "3;B\n"
                + "4;B\n"
                + "42;C\n"
                + "6;D\n"
                + "7;D\n"
                + "8;D\n";

        Table tableIn = tableFromCSV("tableTestIn", tableContent, false);

        double repartitionRate = 0.56;
        tableManager.trainTestSplitTable(tableIn, "WrongTarget", repartitionRate);
    }

    /**
     * test TrainTestSplitTable
     * cases:
     * - column and row headers are present
     * - 2 classes
     */
    @Test
    public void testTrainTestSplitTableWithRowHeader() throws Exception {

        TableManager tableManager = new TableManager();
        String tableJson = "{\"table_desc\":" +
                "{\"title\":\"max_test_traintestsplit\"," +
                "\"desc\":\"population.csv\"}," +
                "\"headers\":" +
                "{\"col\":" +
                "{\"data\":" +
                "[\"flight_id\",\"target\"]}," +
                "\"row\":" +
                "{\"data\":" +
                "[null,\"0\",\"1\",\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\",\"9\"]}}," +
                "\"content\":" +
                "{\"cells\":[[\"1\"],[\"1\"],[\"1\"],[\"0\"],[\"0\"],[\"0\"],[\"0\"],[\"0\"],[\"0\"],[\"0\"]]}}";

        TableInfo tableInfo = tableManager.loadFromJson(tableJson);
        Table table = tableManager.initTable(tableInfo, false);

        List<Table> result;
        double repartitionRate = 0.56;
        result = tableManager.trainTestSplitTable(table, "target", repartitionRate);

        // collect all classes of each table result to check repartition rate
        List<Object> classList1 = new ArrayList<>();
        List<Object> classList2 = new ArrayList<>();
        for (int i = 0; i < result.get(0).getContentData().size(); i++) {
            classList1.add(result.get(0).getContentData().get(i).get(0));
        }
        for (int i = 0; i < result.get(1).getContentData().size(); i++) {
            classList2.add(result.get(1).getContentData().get(i).get(0));
        }
        // checking repartition rate of each class in result
        assertEquals(4, Collections.frequency(classList1, "0"));
        assertEquals(2, Collections.frequency(classList1, "1"));

        // checking repartition rate of each class in result
        assertEquals(3, Collections.frequency(classList2, "0"));
        assertEquals(1, Collections.frequency(classList2, "1"));

    }

    /**
     * test randomSplitTable case : input table handles only column headers
     */
    @Test
    public void testRandomSplitTable() throws Exception {

        TableManager tableManager = new TableManager();
        String tableContent = "MainId;Target\n"
                + "125;A\n"
                + "1;A\n"
                + "2;A\n"
                + "2;A\n"
                + "3;B\n"
                + "4;B\n"
                + "42;C\n"
                + "6;D\n"
                + "7;D\n"
                + "8;D\n";
        double tableContentSize = 10;

        Table tableIn = tableFromCSV("tableTestIn", tableContent, false);

        List<Table> result;
        double repartitionRate = 0.56;
        result = tableManager.randomSplitTable(tableIn, repartitionRate);

        // checking repartition rate in result
        assertEquals(Math.round(tableContentSize * repartitionRate), result.get(0).getRowCount(false));
        assertEquals(Math.round((tableContentSize * (1 - repartitionRate))), result.get(1).getRowCount(false));
    }

    /**
     * test randomSplitTable case : table handels column AND row headers
     */
    @Test
    public void testRandomSplitTableWithRowHeader() throws Exception {

        TableManager tableManager = new TableManager();
        String tableJson = "{\"table_desc\":" +
                "{\"title\":\"max_test_traintestsplit\"," +
                "\"desc\":\"population.csv\"}," +
                "\"headers\":" +
                "{\"col\":" +
                "{\"data\":" +
                "[\"flight_id\",\"target\"]}," +
                "\"row\":" +
                "{\"data\":" +
                "[null,\"0\",\"1\",\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\",\"9\"]}}," +
                "\"content\":" +
                "{\"cells\":[[\"1\"],[\"1\"],[\"1\"],[\"0\"],[\"0\"],[\"0\"],[\"0\"],[\"0\"],[\"0\"],[\"0\"]]}}";

        double tableContentSize = 10;

        TableInfo tableInfo = tableManager.loadFromJson(tableJson);
        Table tableIn = tableManager.initTable(tableInfo, false);


        List<Table> result;
        double repartitionRate = 0.6;
        result = tableManager.randomSplitTable(tableIn, repartitionRate);

        // checking repartition rate in result
        assertEquals(Math.round(tableContentSize * repartitionRate), result.get(0).getRowCount(false));
        assertEquals(Math.round((tableContentSize * (1 - repartitionRate))), result.get(1).getRowCount(false));

    }

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

        tableH.checkConsistency();

        String firstColName = (String) tableH.getColumnsHeader().getData().get(0);
        List<String> funcIds = tableH.getColumn(firstColName);
        List<Object> refFuncIds = new ArrayList<Object>(table.headers.row.data);
        refFuncIds.remove(0);

        if (verbose)
            System.out.println(funcIds);
        if (verbose)
            System.out.println(refFuncIds);

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

        if (verbose)
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

        if (verbose)
            System.out.println(refOtherDecimal);
        if (verbose)
            System.out.println(otherDecimal);

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

        lTestedTable.checkConsistency();

        List<String> myIds = lTestedTable.getColumn("Id");
        List<Integer> myTargets = lTestedTable.getColumn("Target", Integer.class);
        assertEquals(Arrays.asList("hello", "hello2", "hello3"), myIds);
        assertEquals(Arrays.asList(1, 10, 100), myTargets);

        // Test second subcase: with rows header
        Table lTestedTableWithRowsH = mng.initTable(Arrays.asList("Id", "Target"), true);
        lTestedTableWithRowsH.appendRow("hello", Arrays.asList(1));
        lTestedTableWithRowsH.appendRow("hello2", Arrays.asList(10));
        lTestedTableWithRowsH.appendRow("hello3", Arrays.asList(100));

        lTestedTableWithRowsH.checkConsistency();

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

        if (verbose)
            System.out.println(selectedRowValsBis);
        if (verbose)
            System.out.println(selectedRowVals);
        if (verbose)
            System.out.println(ref);

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

        tableH.checkConsistency();
        tableHBis.checkConsistency();

        if (verbose)
            System.out.println(mng.serializeToJson(table));
        if (verbose)
            System.out.println(mng.serializeToJson(tableHBis.getTableInfo()));

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

        tableH.checkConsistency();
        tableHBis.checkConsistency();

        if (verbose)
            System.out.println(mng.serializeToJson(table));
        if (verbose)
            System.out.println(mng.serializeToJson(tableHBis.getTableInfo()));

        assertEquals(mng.serializeToJson(table), mng.serializeToJson(tableHBis.getTableInfo()));

        List<Object> columnnTwo = tableH.getColumn("Two", Object.class);
        if (verbose)
            System.out.println(columnnTwo);

        assertEquals(columnnTwo, Arrays.asList(new Object[]{2.0, 2.2, false}));

        List<Object> columnnOfRowHeaders = tableH.getColumn("Above row header");
        if (verbose)
            System.out.println(columnnOfRowHeaders);

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

        tableH.checkConsistency();
        tableHBis.checkConsistency();

        if (verbose)
            System.out.println(mng.serializeToJson(table));
        if (verbose)
            System.out.println(mng.serializeToJson(tableHBis.getTableInfo()));
        assertEquals(mng.serializeToJson(table), mng.serializeToJson(tableHBis.getTableInfo()));

        List<Object> columnnTwo = tableH.getColumn("Two", Object.class);
        if (verbose)
            System.out.println(columnnTwo);

        assertEquals(columnnTwo, Arrays.asList(new Object[]{row1[1], row2AsList.get(1).data, row3.get(1).data}));
        assertEquals(columnnTwo, tableHBis.getColumn("Two", Object.class));

        // Tests link selection: linkTwo.type= "typ 2";
        // linkTwo.val= "val 2";
        // assertEquals( tableH.cells.links.get(1).get(1).type,
        // tableHBis.getColumnFromTable("Two") );

        List<Object> columnnOfRowHeaders = tableH.getColumn("Above row header");
        if (verbose)
            System.out.println(columnnOfRowHeaders);

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

        if (verbose)
            System.out.println(TableManagerTest.JSON_CONTENT_SAMPLE_1);

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

        tableH.checkConsistency();

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

        if (verbose)
            System.out.println(myT.getColumn("One"));
        if (verbose)
            System.out.println(myT.getColumn("Two"));
        if (verbose)
            System.out.println(myT.getColumn("Three"));

        List<Boolean> strOneList = myT.getColumn("One", Boolean.class);
        List<String> strOneListAString = myT.getColumn("One");

        try {
            myT.getColumn("One", BigDecimal.class);
            fail("Incorrect: class cast exception not detected !");
        }
        catch (IkatsException e) {
            assertTrue(true);
        }
        catch (Exception e) {
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

        TableManager mng = new TableManager();
        Table myT = mng.initEmptyTable(true, true);
        myT.getColumnsHeader().addItem("One").addItem("Two").addItem("Three");
        for (int i = 0; i < 10; i++) {
            myT.appendRow("row" + i, Arrays.asList(i, i + 2));
        }

        if (verbose)
            System.out.println(myT.getRow("row1"));
        if (verbose)
            System.out.println(myT.getRow("row2"));
        if (verbose)
            System.out.println(myT.getRow("row9"));

        List<Integer> strOneList = myT.getRow("row1", Integer.class);
        List<String> strOneListAString = myT.getRow("row1");
        try {
            myT.getRow("row2", BigDecimal.class);
            fail("Incorrect: class cast exception not detected !");
        }
        catch (IkatsException e) {
            if (verbose)
                System.out.println("testGetRow: Got expected exception");
        }
        catch (Exception e) {
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

        TableManager mng = new TableManager();
        Table myT = mng.initEmptyTable(false, true);

        myT.getRowsHeader().addItems("0", "1", "2", "3", "4");
        for (int i = 0; i < 10; i++) {
            myT.appendColumn(Arrays.asList("a" + i, "b" + i, "c" + i, "d" + i, "e" + i));
        }

        if (verbose)
            System.out.println(myT.getRowsHeader().getItems());
        assertEquals("0", myT.getRowsHeader().getItems().get(0));
        assertEquals("4", myT.getRowsHeader().getItems().get(4));

        if (verbose)
            System.out.println(myT.getRow("0"));
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

        TableManager mng = new TableManager();
        Table myT = mng.initEmptyTable(true, false);

        // unusual case: handling Integer -instead of String- in header data ... why not ...
        myT.getColumnsHeader().addItems(0, 1, 2, 3, 8);
        for (int i = 0; i < 10; i++) {
            myT.appendRow(Arrays.asList("a" + i, "b" + i, "c" + i, "d" + i, "e" + i));
        }

        if (verbose)
            System.out.println(myT.getColumnsHeader().getItems());
        // getting effective header data
        assertEquals(new Integer(0), myT.getColumnsHeader().getItems(Integer.class).get(0));
        assertEquals(new Integer(8), myT.getColumnsHeader().getItems(Integer.class).get(4));
        // getting header data as string
        assertEquals("8", myT.getColumnsHeader().getItems().get(4));

        // a bit weird but the key for header 0 is toString() representation "0"
        // =>
        if (verbose)
            System.out.println(myT.getColumn("0"));

        // usual case: handling Strings in header data
        Table myUsualT = mng.initEmptyTable(true, false);
        myUsualT.getColumnsHeader().addItems("10", "1", "2", "3", "8");
        for (int i = 0; i < 10; i++) {
            myUsualT.appendRow(Arrays.asList("a" + i, "b" + i, "c" + i, "d" + i, "e" + i));
        }

        if (verbose)
            System.out.println(myUsualT.getColumnsHeader().getItems());
        assertEquals("10", myUsualT.getColumnsHeader().getItems().get(0));
        assertEquals("8", myUsualT.getColumnsHeader().getItems().get(4));
        if (verbose)
            System.out.println(myUsualT.getColumn("10"));

    }

    /**
     * Tests sort algo based upon column values from table content, - not from the row header - Ascending order is
     * tested here.
     *
     * @throws Exception
     */
    @Test
    public void testSortRowswithoutHeaders() throws Exception {

        TableManager mng = new TableManager();
        Table myTWithoutRowHeader = mng.initEmptyTable(false, false);

        myTWithoutRowHeader.appendRow(Arrays.asList("bla3", "BLAH3", 3.5));
        myTWithoutRowHeader.appendRow(Arrays.asList("bla2", "BLAH2", 2.0));
        myTWithoutRowHeader.appendRow(Arrays.asList("bla4", "BLAH4", 3.7));
        myTWithoutRowHeader.appendRow(Arrays.asList("bla6", "BLAH6", 6.0));
        myTWithoutRowHeader.appendRow(Arrays.asList("-bla6", "-BLAH6", -6.0));

        // displayed when verbose == true
        displayTestedTable(myTWithoutRowHeader);

        myTWithoutRowHeader.sortRowsByColumnValues(2, false);

        displayTestedTable(myTWithoutRowHeader);

        assertEquals(Arrays.asList(-6.0, 2.0, 3.5, 3.7, 6.0), myTWithoutRowHeader.getColumn(2, Double.class));

        assertEquals(Arrays.asList("-bla6", "-BLAH6", -6.0), myTWithoutRowHeader.getRow(0, Object.class));
        assertEquals(Arrays.asList("bla2", "BLAH2", 2.0), myTWithoutRowHeader.getRow(1, Object.class));
        assertEquals(Arrays.asList("bla3", "BLAH3", 3.5), myTWithoutRowHeader.getRow(2, Object.class));
        assertEquals(Arrays.asList("bla4", "BLAH4", 3.7), myTWithoutRowHeader.getRow(3, Object.class));
        assertEquals(Arrays.asList("bla6", "BLAH6", 6.0), myTWithoutRowHeader.getRow(4, Object.class));

        myTWithoutRowHeader.checkConsistency();
    }

    /**
     * Tests sort algo based upon column values from table content, - not from the row header - Ascending order is
     * tested here.
     *
     * @throws Exception
     */
    @Test
    public void testSortRowsWithColHeader() throws Exception {

        TableManager mng = new TableManager();
        Table myTWithoutRowHeader = mng.initEmptyTable(true, false);

        // unusual case: handling Integer -instead of String- in header data ... why not ...
        myTWithoutRowHeader.getColumnsHeader().addItems("First", "Blabla", "Order");

        myTWithoutRowHeader.appendRow(Arrays.asList("bla3", "BLAH3", 3));
        myTWithoutRowHeader.appendRow(Arrays.asList("bla2", "BLAH2", 2));
        myTWithoutRowHeader.appendRow(Arrays.asList("bla4", "BLAH4", 4));
        myTWithoutRowHeader.appendRow(Arrays.asList("bla6", "BLAH6", 6));
        myTWithoutRowHeader.appendRow(Arrays.asList("-bla6", "-BLAH6", -6));

        // displayed when verbose == true
        displayTestedTable(myTWithoutRowHeader);

        myTWithoutRowHeader.sortRowsByColumnValues("Order", false);

        displayTestedTable(myTWithoutRowHeader);

        assertEquals(Arrays.asList(-6, 2, 3, 4, 6), myTWithoutRowHeader.getColumn("Order", Integer.class));

        assertEquals(Arrays.asList("-bla6", "-BLAH6", -6), myTWithoutRowHeader.getRow(1, Object.class));
        assertEquals(Arrays.asList("bla2", "BLAH2", 2), myTWithoutRowHeader.getRow(2, Object.class));
        assertEquals(Arrays.asList("bla3", "BLAH3", 3), myTWithoutRowHeader.getRow(3, Object.class));
        assertEquals(Arrays.asList("bla4", "BLAH4", 4), myTWithoutRowHeader.getRow(4, Object.class));
        assertEquals(Arrays.asList("bla6", "BLAH6", 6), myTWithoutRowHeader.getRow(5, Object.class));

        myTWithoutRowHeader.checkConsistency();

    }

    /**
     * Tests sort algo based upon column values from table content, - not from the row header - Ascending order is
     * tested here.
     *
     * @throws Exception
     */
    @Test
    public void testSortRowsWithAllHeaders() throws Exception {

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

        // displayed when verbose == true
        displayTestedTable(myTWithoutRowHeader);

        myTWithoutRowHeader.sortRowsByColumnValues("Order", false);

        displayTestedTable(myTWithoutRowHeader);

        assertEquals(Arrays.asList(-6, 2, 3, 4, 6), myTWithoutRowHeader.getColumn("Order", Integer.class));

        assertEquals(Arrays.asList("-bla6", "-BLAH6", -6), myTWithoutRowHeader.getRow(1, Object.class));
        assertEquals(Arrays.asList("bla2", "BLAH2", 2), myTWithoutRowHeader.getRow(2, Object.class));
        assertEquals(Arrays.asList("bla3", "BLAH3", 3), myTWithoutRowHeader.getRow(3, Object.class));
        assertEquals(Arrays.asList("bla4", "BLAH4", 4), myTWithoutRowHeader.getRow(4, Object.class));
        assertEquals(Arrays.asList("bla6", "BLAH6", 6), myTWithoutRowHeader.getRow(5, Object.class));

        myTWithoutRowHeader.sortRowsByColumnValues("TopLeft", false);

        displayTestedTable(myTWithoutRowHeader);

        assertEquals(Arrays.asList("A2", "A10", "A100", "B1", "B1.1"),
                     myTWithoutRowHeader.getColumn("TopLeft", String.class));

        myTWithoutRowHeader.checkConsistency();

    }

    /**
     * Test insertColumn with all headers activated
     *
     * @throws Exception
     */
    @Test
    public void testInsertColumnWithAllHeaders() throws Exception {

        TableManager mng = new TableManager();

        // both headers are managed
        Table myTable = mng.initEmptyTable(true, true);

        // unusual case: handling Integer -instead of String- in header data ... why not ...
        myTable.getColumnsHeader().addItems("TopLeft", "First", "Blabla", "Order");
        // needs to define the top left corner as undefined !
        myTable.getRowsHeader().addItem(null);

        myTable.appendRow("B1.1", Arrays.asList("bla3", "BLAH3", 3));
        myTable.appendRow("B1", Arrays.asList("bla2", "BLAH2", 2));
        myTable.appendRow("A100", Arrays.asList("bla4", "BLAH4", 4));
        myTable.appendRow("A10", Arrays.asList("bla6", "BLAH6", 6));
        myTable.appendRow("A2", Arrays.asList("-bla6", "-BLAH6", -6));

        displayTestedTable(myTable);

        myTable.insertColumn("Blabla", "Bazar", Arrays.asList(Boolean.TRUE, Boolean.FALSE, "text", null, 3.14));

        displayTestedTable(myTable);

        myTable.checkConsistency();

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

        TableManager mng = new TableManager();
        Table myTWithColHeader = mng.initEmptyTable(true, false);

        // unusual case: handling Integer -instead of String- in header data ... why not ...
        myTWithColHeader.getColumnsHeader().addItems("First", "Blabla", "Order");

        myTWithColHeader.appendRow(Arrays.asList("bla3", "BLAH3", 3));
        myTWithColHeader.appendRow(Arrays.asList("bla2", "BLAH2", 2));
        myTWithColHeader.appendRow(Arrays.asList("bla4", "BLAH4", 4));
        myTWithColHeader.appendRow(Arrays.asList("bla6", "BLAH6", 6));
        myTWithColHeader.appendRow(Arrays.asList("-bla6", "-BLAH6", -6));

        // displayed if verbose == true
        displayTestedTable(myTWithColHeader);

        myTWithColHeader.insertColumn("Blabla", "Bazar",
                                      Arrays.asList(Boolean.TRUE, Boolean.FALSE, "text", null, 3.14));

        displayTestedTable(myTWithColHeader);

        myTWithColHeader.checkConsistency();

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

        TableManager mng = new TableManager();

        // no headers
        Table myTable = mng.initEmptyTable(false, false);

        myTable.appendRow(Arrays.asList("bla3", "BLAH3", 3));
        myTable.appendRow(Arrays.asList("bla2", "BLAH2", 2));
        myTable.appendRow(Arrays.asList("bla4", "BLAH4", 4));
        myTable.appendRow(Arrays.asList("bla6", "BLAH6", 6));
        myTable.appendRow(Arrays.asList("-bla6", "-BLAH6", -6));

        // displayed when verbose == true
        displayTestedTable(myTable);

        myTable.insertColumn(1, Arrays.asList(Boolean.TRUE, Boolean.FALSE, "text", null, 3.14));

        displayTestedTable(myTable);

        myTable.checkConsistency();

        assertEquals(Arrays.asList(true, false, "text", null, 3.14), myTable.getColumn(1, Object.class));

    }

    /**
     * Tests the insertRow() without header
     *
     * @throws Exception
     */
    @Test
    public void testInsertRowWithoutHeader() throws Exception {

        TableManager mng = new TableManager();

        // no headers
        Table myTable = mng.initEmptyTable(false, false);

        myTable.appendRow(Arrays.asList("bla3", "BLAH3", 3));
        myTable.appendRow(Arrays.asList("bla2", "BLAH2", 2));
        myTable.appendRow(Arrays.asList("bla4", "BLAH4", 4));
        myTable.appendRow(Arrays.asList("bla6", "BLAH6", 6));
        myTable.appendRow(Arrays.asList("-bla6", "-BLAH6", -6));

        // displayed when verbose == true
        displayTestedTable(myTable);

        myTable.insertRow(1, Arrays.asList("avantBla2", Boolean.FALSE, "text"));

        displayTestedTable(myTable);

        myTable.checkConsistency();

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

        TableManager mng = new TableManager();
        Table myTable = mng.initEmptyTable(true, true);

        // unusual case: handling Integer -instead of String- in header data ... why not ...
        myTable.getColumnsHeader().addItems("TopLeft", "First", "Blabla", "Order");
        // needs to define the top left corner as undefined !
        myTable.getRowsHeader().addItem(null);

        myTable.appendRow("B1.1", Arrays.asList("bla3", "BLAH3", 3));
        myTable.appendRow("B1", Arrays.asList("bla2", "BLAH2", 2));
        myTable.appendRow("A100", Arrays.asList("bla4", "BLAH4", 4));
        myTable.appendRow("A10", Arrays.asList("bla6", "BLAH6", 6));
        myTable.appendRow("A2", Arrays.asList("-bla6", "-BLAH6", -6));

        // displayed when verbose == true
        displayTestedTable(myTable);

        myTable.insertRow("A100", "Bazar", Arrays.asList(Boolean.TRUE, Boolean.FALSE, 3.14));

        displayTestedTable(myTable);

        myTable.checkConsistency();

        assertEquals(Arrays.asList(null, "B1.1", "B1", "Bazar", "A100", "A10", "A2"),
                     myTable.getRowsHeader().getItems());
        assertEquals(Arrays.asList(true, false, 3.14), myTable.getRow("Bazar", Object.class));
        assertEquals(Arrays.asList(true, false, 3.14), myTable.getRow(3, Object.class));
    }

    /**
     * Makes sysout display of Table activated, once verbose is True
     *
     * @param table
     *
     * @throws IkatsException
     * @throws ResourceNotFoundException
     */
    private void displayTestedTable(Table table) throws IkatsException, ResourceNotFoundException {
        if (verbose) {
            if (table.isHandlingColumnsHeader())
                System.out.println(table.getColumnsHeader().getItems());
            List<Object> rowsHeaderItems = table.isHandlingRowsHeader() ? table.getRowsHeader().getItems(Object.class)
                    : null;
            for (int i = 0; i < table.getRowCount(true); i++) {
                String start = "";
                if (rowsHeaderItems != null) {
                    start = "" + rowsHeaderItems.get(i) + ": ";
                }
                System.out.println(start + table.getRow(i, Object.class));
            }
            System.out.println(" ");
        }
    }

    /**
     * Convert a CSV to Table object
     *
     * @param name    name identifying the Table
     * @param content text corresponding to the CSV format
     */
    private Table tableFromCSV(String name, String content, boolean rowHeader) throws IOException, IkatsException, IkatsDaoException, InvalidValueException {

        TableManager tableMNG = new TableManager();

        // Convert the CSV table to expected Table format
        BufferedReader bufReader = new BufferedReader(new StringReader(content));

        // Assuming first line contains headers
        String line = bufReader.readLine();
        List<String> headersTitle = Arrays.asList(line.split(";"));
        Table table = tableMNG.initTable(headersTitle, rowHeader);

        if (rowHeader) {
            table.getRowsHeader().getData().add(null);
        }

        // Other lines contain data
        while ((line = bufReader.readLine()) != null) {
            List<String> items = new ArrayList<>(Arrays.asList(line.split(";")));
            if (rowHeader) {
                table.getRowsHeader().getData().add(items.get(0));
                items.remove(0);
            }
            table.appendRow(items);
        }
        return table;
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

        tableH.checkConsistency();
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

        tableH.checkConsistency();
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
}

