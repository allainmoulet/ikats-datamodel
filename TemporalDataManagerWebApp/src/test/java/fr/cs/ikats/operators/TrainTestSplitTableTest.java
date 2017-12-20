package fr.cs.ikats.operators;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.temporaldata.business.table.Table;
import fr.cs.ikats.temporaldata.business.table.TableInfo;
import fr.cs.ikats.temporaldata.business.table.TableManager;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;


public class TrainTestSplitTableTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     * test randomSplitTable case : input table handles only column headers
     */
    @Test
    public void testRandomSplitTable() throws Exception {
    
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
        result = new TrainTestSplitTable().randomSplitTable(tableIn, repartitionRate);
    
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
        result = new TrainTestSplitTable().randomSplitTable(tableIn, repartitionRate);
    
        // checking repartition rate in result
        assertEquals(Math.round(tableContentSize * repartitionRate), result.get(0).getRowCount(false));
        assertEquals(Math.round((tableContentSize * (1 - repartitionRate))), result.get(1).getRowCount(false));
    
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
    public void testTrainTestSplitTableNominal() throws Exception {
    
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
        result = new TrainTestSplitTable().trainTestSplitTable(tableIn, "Target", repartitionRate);
    
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
        result = new TrainTestSplitTable().trainTestSplitTable(tableIn, "Target", repartitionRate);
    
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
        result = new TrainTestSplitTable().trainTestSplitTable(tableIn, "Target", repartitionRate);
    
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
        result = new TrainTestSplitTable().trainTestSplitTable(tableIn, "Target", repartitionRate);
    
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
        result = new TrainTestSplitTable().trainTestSplitTable(table, "target", repartitionRate);
    
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
     * test TrainTestSplitTable
     * cases:
     * - only column headers present
     * - a  class with only one element
     * - duplicates ids
     * - 4 classes
     */
    @Test(expected = ResourceNotFoundException.class)
    public void testTrainTestSplitTableWrongTarget() throws Exception {
    
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
        new TrainTestSplitTable().trainTestSplitTable(tableIn, "WrongTarget", repartitionRate);
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
}
