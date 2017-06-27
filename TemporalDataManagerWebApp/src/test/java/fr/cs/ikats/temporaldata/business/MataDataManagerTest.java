package fr.cs.ikats.temporaldata.business;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.metadata.MetaDataFacade;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.metadata.model.MetadataCriterion;
import fr.cs.ikats.temporaldata.business.TableManager.Table;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for TemporalDataManager
 */
public class MataDataManagerTest {

    private static final Logger logger = Logger.getLogger(MataDataManagerTest.class);

    /**
     * Saves the content of a CSV as a Table
     *
     * @param name    name identifying the Table
     * @param content text corresponding to the CSV format
     */
    private static void saveTable(String name, String content) {
        try {
            TableManager tableManager = new TableManager();

            // Check if database is clean

            if (tableManager.existsInDatabase(name)) {
                // Table name already exists
                fail("Table name already exists: " + name);
            }

            // Convert the CSV table to expected Table format
            BufferedReader bufReader = new BufferedReader(new StringReader(content));

            // First line contains headers
            String line = bufReader.readLine();
            List<String> headersTitle = Arrays.asList(line.split(";"));
            Table table = tableManager.initTable(headersTitle, false);

            // Other lines contain data
            while ((line = bufReader.readLine()) != null) {
                List<String> items = Arrays.asList(line.split(";"));
                table.appendRow(items);
            }

            // Save the table into database
            String rid = tableManager.createInDatabase(name, table.getTable());

            logger.trace("Table " + name + " saved with RID=" + rid);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    private void deleteTable(String tableName) {
        try {
            TableManager tableManager = new TableManager();

            tableManager.deteteFromDatabase(tableName);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    /**
     * Adds a criterion to a criteria list
     *
     * @param criteria          criteria list
     * @param critName          name of the metadata (left Operand part)
     * @param critOperator      comparator to use
     * @param rightOperandValue value of the right Operand
     */
    private void addCrit(ArrayList<MetadataCriterion> criteria, String critName, String critOperator, String rightOperandValue) {
        MetadataCriterion crit = new MetadataCriterion(critName, critOperator, rightOperandValue);
        criteria.add(crit);
    }

    /**
     * Add Functional Identifier to expected list
     *
     * @param expected list containing the expected values
     * @param tsuid    tsuid matching the expected value
     * @param funcId   Functional Identifier matching the expected value
     */
    private void addToScope(List<FunctionalIdentifier> expected, String tsuid, String funcId) {
        FunctionalIdentifier fid = new FunctionalIdentifier(tsuid, funcId);
        expected.add(fid);
    }

    /**
     * Test the metadata filtering based on "in table" operator with no match:
     * - Table well formatted (The filter will be done on column "FlightId")
     * * all necessary columns are present
     * * Id are not contiguous
     * - No column defined in criterion and metadata name doesn't match the column name
     * <p>
     * Test method for
     * {@link MetaDataManager#filterByMetaWithTsuidList(java.util.List, java.util.List)}
     */
    @Test
    public void test_filterByMetaWithTsuidList_inTable_NoColumnNoMatch() {

        MetaDataFacade facade = new MetaDataFacade();
        MetaDataManager metaDataManager = new MetaDataManager();
        try {

            // Create the test set
            facade.persistMetaData("TS1", "Identifier", "1"); // Match
            facade.persistMetaData("TS2", "Identifier", "2"); // Match
            facade.persistMetaData("TS3", "Identifier", "5");
            facade.persistMetaData("TS4", "Identifier", "8"); // Match
            facade.persistMetaData("TS5", "Identifier", "9");
            facade.persistMetaData("TS5", "NoIdentifier", "4");
            facade.persistMetaData("TS6", "Identifier", "0");
            facade.persistMetaData("TS7", "NoIdentifier", "4");
            facade.persistMetaData("TS8", "Identifier", "10");
            facade.persistMetaData("TS9", "Identifier", "42"); // Match

            // Prepare the Table data
            String tableContent = "FlightId;Target\n"
                    + "1;A\n"
                    + "2;B\n"
                    + "3;C\n"
                    + "4;D\n"
                    + "42;A\n"
                    + "6;B\n"
                    + "7;C\n"
                    + "8;D\n";
            saveTable("TestTable", tableContent);

            // Create the initial scope
            List<FunctionalIdentifier> scope = new ArrayList<FunctionalIdentifier>();
            addToScope(scope, "TS1", "FID1");
            addToScope(scope, "TS2", "FID2");
            addToScope(scope, "TS3", "FID3");
            addToScope(scope, "TS4", "FID4");
            addToScope(scope, "TS5", "FID5");
            addToScope(scope, "TS6", "FID6");
            addToScope(scope, "TS7", "FID7");
            addToScope(scope, "TS8", "FID8");
            addToScope(scope, "TS9", "FID9");

            // Criteria
            ArrayList<MetadataCriterion> critList = new ArrayList<MetadataCriterion>();
            addCrit(critList, "Identifier", "in table", "TestTable");


            // Compute
            try {
                ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>)
                        metaDataManager.filterByMetaWithTsuidList(scope, critList);
            } catch (ResourceNotFoundException e) {
                // No column matches --> Test is OK
                assertTrue(e.getMessage().contains("no column named"));
            } catch (Exception e) {
                fail();
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected error");
        } finally {
            // Cleanup
            try {
                facade.removeMetaDataForTS("TS1");
                facade.removeMetaDataForTS("TS2");
                facade.removeMetaDataForTS("TS3");
                facade.removeMetaDataForTS("TS4");
                facade.removeMetaDataForTS("TS5");
                facade.removeMetaDataForTS("TS6");
                facade.removeMetaDataForTS("TS7");
                facade.removeMetaDataForTS("TS8");
                facade.removeMetaDataForTS("TS9");
                deleteTable("TestTable");

            } catch (IkatsDaoException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Test the metadata filtering based on "in table" operator with redundant identifier:
     * - Table well formatted (The filter will be done on column "FlightId")
     * * Id contains a doubloon
     * - TS match metadata name / value pair
     * - TS doesn't match the following cases:
     * * metadata name with value not in expected values
     * * different metadata name matching the value
     * * No metadata name for a TS
     * <p>
     * Test method for
     * {@link MetaDataManager#filterByMetaWithTsuidList(java.util.List, java.util.List)}
     */
    @Test
    public void test_filterByMetaWithTsuidList_inTable_RedundantIdentifiers() {

        MetaDataFacade facade = new MetaDataFacade();
        MetaDataManager metaDataManager = new MetaDataManager();
        try {

            // Create the test set
            facade.persistMetaData("TS1", "Identifier", "1"); // Match
            facade.persistMetaData("TS2", "Identifier", "2"); // Match
            facade.persistMetaData("TS3", "Identifier", "5");
            facade.persistMetaData("TS4", "Identifier", "8"); // Match
            facade.persistMetaData("TS5", "Identifier", "9");
            facade.persistMetaData("TS5", "NoIdentifier", "4");
            facade.persistMetaData("TS6", "Identifier", "0");
            facade.persistMetaData("TS7", "NoIdentifier", "4");
            facade.persistMetaData("TS8", "Identifier", "10");
            facade.persistMetaData("TS9", "Identifier", "42"); // Match

            // Prepare the Table data
            String tableContent = "MainId;Target\n"
                    + "1;A\n"
                    + "1;A\n"
                    + "2;B\n"
                    + "3;C\n"
                    + "4;D\n"
                    + "42;A\n"
                    + "6;B\n"
                    + "7;C\n"
                    + "8;D\n";
            saveTable("TestTable", tableContent);

            // Create the initial scope
            List<FunctionalIdentifier> scope = new ArrayList<FunctionalIdentifier>();
            addToScope(scope, "TS1", "FID1");
            addToScope(scope, "TS2", "FID2");
            addToScope(scope, "TS3", "FID3");
            addToScope(scope, "TS4", "FID4");
            addToScope(scope, "TS5", "FID5");
            addToScope(scope, "TS6", "FID6");
            addToScope(scope, "TS7", "FID7");
            addToScope(scope, "TS8", "FID8");
            addToScope(scope, "TS9", "FID9");

            // Criteria
            ArrayList<MetadataCriterion> critList = new ArrayList<MetadataCriterion>();
            addCrit(critList, "Identifier", "in table", "TestTable.MainId");

            // Preparing results
            ArrayList<FunctionalIdentifier> expected = new ArrayList<FunctionalIdentifier>();
            addToScope(expected, "TS1", "FID1");
            addToScope(expected, "TS2", "FID2");
            addToScope(expected, "TS4", "FID4");
            addToScope(expected, "TS9", "FID9");

            // Compute
            ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>)
                    metaDataManager.filterByMetaWithTsuidList(scope, critList);

            // Check results
            assertTrue(obtained.equals(expected));


        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected error");
        } finally {

            // Cleanup
            try {
                facade.removeMetaDataForTS("TS1");
                facade.removeMetaDataForTS("TS2");
                facade.removeMetaDataForTS("TS3");
                facade.removeMetaDataForTS("TS4");
                facade.removeMetaDataForTS("TS5");
                facade.removeMetaDataForTS("TS6");
                facade.removeMetaDataForTS("TS7");
                facade.removeMetaDataForTS("TS8");
                facade.removeMetaDataForTS("TS9");
                deleteTable("TestTable");

            } catch (IkatsDaoException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Test the metadata filtering based on "in table" operator with nominal behavior:
     * - Table well formatted (The filter will be done on column "FlightId")
     * * all necessary columns are present
     * * Id are not contiguous
     * - No column defined in criterion but metadata name match the column name
     * - TS match metadata name / value pair
     * - TS doesn't match the following cases:
     * * metadata name with value not in expected values
     * * different metadata name matching the value
     * * No metadata name for a TS
     * <p>
     * Test method for
     * {@link MetaDataManager#filterByMetaWithTsuidList(java.util.List, java.util.List)}
     */
    @Test
    public void test_filterByMetaWithTsuidList_inTable_NoColumnButMatch() {

        MetaDataFacade facade = new MetaDataFacade();
        MetaDataManager metaDataManager = new MetaDataManager();

        try {
            // Create the test set
            facade.persistMetaData("TS1", "Identifier", "1"); // Match
            facade.persistMetaData("TS2", "Identifier", "2"); // Match
            facade.persistMetaData("TS3", "Identifier", "5");
            facade.persistMetaData("TS4", "Identifier", "8"); // Match
            facade.persistMetaData("TS5", "Identifier", "9");
            facade.persistMetaData("TS5", "NoIdentifier", "4");
            facade.persistMetaData("TS6", "Identifier", "0");
            facade.persistMetaData("TS7", "NoIdentifier", "4");
            facade.persistMetaData("TS8", "Identifier", "10");
            facade.persistMetaData("TS9", "Identifier", "42"); // Match

            // Prepare the Table data
            String tableContent = "Identifier;Target\n"
                    + "1;A\n"
                    + "2;B\n"
                    + "3;C\n"
                    + "4;D\n"
                    + "42;A\n"
                    + "6;B\n"
                    + "7;C\n"
                    + "8;D\n";
            saveTable("TestTable", tableContent);

            // Create the initial scope
            List<FunctionalIdentifier> scope = new ArrayList<FunctionalIdentifier>();
            addToScope(scope, "TS1", "FID1");
            addToScope(scope, "TS2", "FID2");
            addToScope(scope, "TS3", "FID3");
            addToScope(scope, "TS4", "FID4");
            addToScope(scope, "TS5", "FID5");
            addToScope(scope, "TS6", "FID6");
            addToScope(scope, "TS7", "FID7");
            addToScope(scope, "TS8", "FID8");
            addToScope(scope, "TS9", "FID9");

            // Criteria
            ArrayList<MetadataCriterion> critList = new ArrayList<MetadataCriterion>();
            addCrit(critList, "Identifier", "in table", "TestTable");

            // Preparing results
            ArrayList<FunctionalIdentifier> expected = new ArrayList<FunctionalIdentifier>();
            addToScope(expected, "TS1", "FID1");
            addToScope(expected, "TS2", "FID2");
            addToScope(expected, "TS4", "FID4");
            addToScope(expected, "TS9", "FID9");

            // Compute
            ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>)
                    metaDataManager.filterByMetaWithTsuidList(scope, critList);

            // Check results
            assertTrue(obtained.equals(expected));


        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected error");
        } finally {

            // Cleanup
            try {
                facade.removeMetaDataForTS("TS1");
                facade.removeMetaDataForTS("TS2");
                facade.removeMetaDataForTS("TS3");
                facade.removeMetaDataForTS("TS4");
                facade.removeMetaDataForTS("TS5");
                facade.removeMetaDataForTS("TS6");
                facade.removeMetaDataForTS("TS7");
                facade.removeMetaDataForTS("TS8");
                facade.removeMetaDataForTS("TS9");
                deleteTable("TestTable");
            } catch (IkatsDaoException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Test the metadata filtering based on "in table" operator with no match (different case):
     * - Table well formatted (The filter will be done on column "FlightId")
     * * all necessary columns are present
     * * Id are not contiguous
     * - No column defined in criterion and metadata name doesn't match the column name (they have different case)
     * <p>
     * Test method for
     * {@link MetaDataManager#filterByMetaWithTsuidList(java.util.List, java.util.List)}
     */
    @Test
    public void test_filterByMetaWithTsuidList_inTable_NoColumnDiffCase() {

        MetaDataFacade facade = new MetaDataFacade();
        MetaDataManager metaDataManager = new MetaDataManager();

        try {
            // Create the test set
            facade.persistMetaData("TS1", "Identifier", "1"); // Match
            facade.persistMetaData("TS2", "Identifier", "2"); // Match
            facade.persistMetaData("TS3", "Identifier", "5");
            facade.persistMetaData("TS4", "Identifier", "8"); // Match
            facade.persistMetaData("TS5", "Identifier", "9");
            facade.persistMetaData("TS5", "NoIdentifier", "4");
            facade.persistMetaData("TS6", "Identifier", "0");
            facade.persistMetaData("TS7", "NoIdentifier", "4");
            facade.persistMetaData("TS8", "Identifier", "10");
            facade.persistMetaData("TS9", "Identifier", "42"); // Match

            // Prepare the Table data (with "Identifier" column having different case than expected)
            String tableContent = "identifier;Target\n"
                    + "1;A\n"
                    + "2;B\n"
                    + "3;C\n"
                    + "4;D\n"
                    + "42;A\n"
                    + "6;B\n"
                    + "7;C\n"
                    + "8;D\n";
            saveTable("TestTable", tableContent);

            // Create the initial scope
            List<FunctionalIdentifier> scope = new ArrayList<FunctionalIdentifier>();
            addToScope(scope, "TS1", "FID1");
            addToScope(scope, "TS2", "FID2");
            addToScope(scope, "TS3", "FID3");
            addToScope(scope, "TS4", "FID4");
            addToScope(scope, "TS5", "FID5");
            addToScope(scope, "TS6", "FID6");
            addToScope(scope, "TS7", "FID7");
            addToScope(scope, "TS8", "FID8");
            addToScope(scope, "TS9", "FID9");

            // Criteria
            ArrayList<MetadataCriterion> critList = new ArrayList<MetadataCriterion>();
            addCrit(critList, "Identifier", "in table", "TestTable");

            // Compute
            try {
                ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>)
                        metaDataManager.filterByMetaWithTsuidList(scope, critList);
            } catch (ResourceNotFoundException e) {
                // No column matches --> Test is OK
            } catch (Exception e) {
                fail();
            }
        } catch (Exception e) {
            fail("Unexpected error");
        } finally {
            // Cleanup
            try {
                facade.removeMetaDataForTS("TS1");
                facade.removeMetaDataForTS("TS2");
                facade.removeMetaDataForTS("TS3");
                facade.removeMetaDataForTS("TS4");
                facade.removeMetaDataForTS("TS5");
                facade.removeMetaDataForTS("TS6");
                facade.removeMetaDataForTS("TS7");
                facade.removeMetaDataForTS("TS8");
                facade.removeMetaDataForTS("TS9");
                deleteTable("TestTable");
            } catch (IkatsDaoException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Test the metadata filtering based on "in table" operator with table not found
     * <p>
     * Test method for
     * {@link MetaDataManager#filterByMetaWithTsuidList(java.util.List, java.util.List)}
     */
    @Test
    public void test_filterByMetaWithTsuidList_inTable_NoTableFound() {

        try {
            MetaDataManager metaDataManager = new MetaDataManager();

            // Create the initial scope
            List<FunctionalIdentifier> scope = new ArrayList<FunctionalIdentifier>();
            addToScope(scope, "TS1", "FID1");
            addToScope(scope, "TS2", "FID2");

            // Criteria
            ArrayList<MetadataCriterion> critList = new ArrayList<MetadataCriterion>();
            addCrit(critList, "Identifier", "in table", "UnknownTable");

            // Compute
            try {
                ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>)
                        metaDataManager.filterByMetaWithTsuidList(scope, critList);
            } catch (ResourceNotFoundException e) {
                // No column matches --> Test is OK
                e.printStackTrace();
                assertTrue(e.getMessage().contains("No result found for table"));
            } catch (Exception e) {
                fail();
            }


        } catch (Exception e) {
            fail("Unexpected error");
        } finally {
            // Cleanup
            deleteTable("TestTable");
        }
    }
}
