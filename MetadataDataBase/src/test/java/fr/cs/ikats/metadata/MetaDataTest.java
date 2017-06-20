/**
 * $Id$
 * <p>
 * HISTORIQUE
 * <p>
 * VERSION : 1.0 : <US> : <NumUS> : 8 oct. 2015 : Creation
 * <p>
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.metadata;

import fr.cs.ikats.common.dao.DataBaseDAO;
import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import fr.cs.ikats.common.expr.Atom;
import fr.cs.ikats.common.expr.Expression;
import fr.cs.ikats.common.expr.Group;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.metadata.model.MetadataCriterion;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;


/**
 * CRUD tests on MetaData
 */
public class MetaDataTest {

    // MetaDataTest.dao is specifically defined in order to clean the TU DB before every test case:
    // see setUpBeforClass() and init_db()
    // 
    // then in each test case: the hibernate config is re-run with:
    //   new MetaDataFacade()
    //
    private static DataBaseDAO dao = new DataBaseDAO() {
    };

    /**
     * Saves the content of a CSV as a Table
     *
     * @param name    name identifying the Table
     * @param content text corresponding to the CSV format
     */
    private static void saveTable(String name, String content) {
        //TODO Save the table

    }

    /**
     * Initializing the class
     */
    @BeforeClass
    public static void setUpBeforClass() {

        // expects HSQLDB "IN-MEMORY" configuration for the Unit tests
        dao.init("/metaDataHibernate.cfg.xml");

        // test 'IN-MEMORY' case
        String configuredConnectionUrl =
                dao.getAnnotationConfigurationProperty("hibernate.connection.url");
        if (!configuredConnectionUrl.startsWith("jdbc:hsqldb:mem:")) {
            // avoid unwanted deletions with init_db()
            dao = null;
            throw new RuntimeException("Unit tests require that property [hibernate.connection.url] starts with [jdbc:hsqldb:mem] got: " + configuredConnectionUrl);
        }

        dao.addAnotatedPackage("fr.cs.ikats.metadata.model");
        dao.addAnnotatedClass(MetaData.class);
        dao.completeConfiguration();
    }

    /**
     * For each test case: purge the database
     */
    @Before
    public void init_db() {

        Session session = dao.getSession();

        Transaction tx = null;
        Integer mdId = null;
        try {
            tx = session.beginTransaction();
            Query q = session.createQuery("DELETE FROM tsmetadata where tsuid like '%ts%';");
            int results = q.executeUpdate();
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) {
                tx.rollback();
            }
        } finally {
            session.close();
        }
    }


    /**
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#persistMetaData(java.lang.String, java.lang.String, java.lang.String)}
     * .
     */
    @Test
    public void testPersistMetaData() {
        try {
            MetaDataFacade facade = new MetaDataFacade();
            Integer results = facade.persistMetaData("tsuid1", "MDName1", "value1");
            assertTrue("Error, results not imported", results >= 0);
            System.out.println(facade.getCSVForMetaData(facade.getMetaDataForTS("tsuid1")));
        } catch (Exception e) {
            fail();
        }

    }

    @Test
    public void testMetaDataTypes() {
        MetaDataFacade facade = new MetaDataFacade();
        try {
            facade.persistMetaData("tsuidB01", "thatsastring", "blabla", "string");
            facade.persistMetaData("tsuidB02", "thatsanumber", "12", "number");
            Map metaTypesTable = facade.getMetaDataTypes();
            assertNotNull(metaTypesTable);
            assertEquals("number", metaTypesTable.get("thatsanumber"));
            assertEquals("string", metaTypesTable.get("thatsastring"));
        } catch (IkatsDaoException e) {
            e.printStackTrace();
        }
    }

    /**
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#persistMetaData(java.lang.String, java.lang.String, java.util.List)}
     * .
     */
    @Test
    public void testUpdateMetaData() {
        try {
            MetaDataFacade facade = new MetaDataFacade();
            Integer results1 = facade.persistMetaData("tsuidA01", "MDName1", "value1");

            assertTrue("Error, results1 not created", results1.intValue() >= 0);

            List<MetaData> metadataList = facade.getMetaDataForTS("tsuidA01");
            assertTrue((metadataList != null) && (metadataList.size() == 1));
            assertTrue(metadataList.get(0).getValue().equals("value1"));

            Integer results2 = facade.updateMetaData("tsuidA01", "MDName1", "value2");

            assertTrue("Error, results2 not updated", results2.intValue() == results1.intValue());

            metadataList = facade.getMetaDataForTS("tsuidA01");
            assertTrue((metadataList != null) && (metadataList.size() == 1));
            assertTrue(metadataList.get(0).getValue().equals("value2"));

            Integer results3 = facade.updateMetaData("tsuidA01", "MDName1", "value2");

            assertTrue("Error, results3 not updated", results3.intValue() == results1.intValue());

            metadataList = facade.getMetaDataForTS("tsuidA01");
            assertTrue((metadataList != null) && (metadataList.size() == 1));
            assertTrue(metadataList.get(0).getValue().equals("value2"));

            System.out.println(facade.getCSVForMetaData(metadataList));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    /**
     *
     */
    @Test
    public void testUpdateMetadataFailure() {
        System.out.println("---------- START: testUpdateMetadataFailure -----------");
        try {
            MetaDataFacade facade = new MetaDataFacade();
            try {
                Integer results4 = facade.updateMetaData("tsuidF01", "MDFailedUpdateWithoutCreate", "value");
                fail("Expected error: update without any resource created");
            } catch (IkatsDaoConflictException conflict) {
                fail("Unexpected conflict error with update (tsuidF01, MDFailedUpdateWithoutCreate )");
            } catch (IkatsDaoException e) {
                // Good failure: nothing to do
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        System.out.println("---------- END: testUpdateMetadataFailure -----------");
    }

    /**
     * Test the failures of searchs by TSUIDs
     */
    @Test
    public void testSearchByTsuidFailure() {

        System.out.println("---------- START: testSearchByTsuidFailure -----------");
        try {
            MetaDataFacade facade = new MetaDataFacade();
            try {
                // test failure: non existant tsuid
                facade.getMetaDataForTS("tsuidM01");

                fail("Expected error: update without any resource created");
            } catch (IkatsDaoMissingRessource e) {
                // Good failure: nothing to do
            } catch (IkatsDaoException conflict) {
                fail("Unexpected error instead of IkatsDaoMissingRessource");
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        System.out.println("---------- END: testSearchByTsuidFailure -----------");

    }

    /**
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#persistMetaData(java.lang.String, java.lang.String, java.util.List)}
     * .
     */
    @Test
    public void testCreateMetaDataDuplicate() {
        try {
            MetaDataFacade facade = new MetaDataFacade();
            Integer results1 = facade.persistMetaData("tsuidBX", "MDName1", "v1");
            Integer results2 = facade.persistMetaData("tsuidBX", "MDName2", "v1");
            Integer results3 = facade.persistMetaData("tsuidBY", "MDName1", "v2");
            assertTrue("Error, results1 not imported", results1 >= 0);
            assertTrue("Error, results3 not imported", results2 >= 0);
            assertTrue("Error, results4 not imported", results3 >= 0);
            System.out.println(facade.getCSVForMetaData(facade.getMetaDataForTS("tsuidBX")));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    /**
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#removeMetaDataForTS(java.lang.String)}
     * .
     */
    @Test
    public void testRemoveMetaDataForTS() {
        try {
            MetaDataFacade facade = new MetaDataFacade();

            facade.persistMetaData("tsuidC3", "MDName1", "value1");
            facade.persistMetaData("tsuidC3", "MDName2", "value1");
            facade.persistMetaData("tsuidC4", "MDName1", "value2");

            facade.removeMetaDataForTS("tsuidC4");
            System.out.println("---------- START failure expected ----------");
            try {
                List<MetaData> result = facade.getMetaDataForTS("tsuidC4");
                fail("Expected error was not raised: IkatsDaoMissingRessource");
            } catch (IkatsDaoMissingRessource e) {
                // Good: expected failure
            } catch (IkatsDaoException e) {
                fail("Unexpected error instead of IkatsDaoMissingRessource");
            }
            System.out.println("---------- END failure expected ----------");

            List<MetaData> result = facade.getMetaDataForTS("tsuidC3");
            assertEquals(2, result.size());
            facade.removeMetaDataForTS("tsuidC3", "MDName1");
            result = facade.getMetaDataForTS("tsuidC3");
            assertEquals(1, result.size());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    /**
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#getMetaDataForTS(java.lang.String)}
     * .
     */
    @Test
    public void testGetMetaDataForTS() {
        try {
            MetaDataFacade facade = new MetaDataFacade();

            facade.persistMetaData("tsuidD5", "MDName1", "value1");

            List<MetaData> result = facade.getMetaDataForTS("tsuidD5");
            System.out.println(facade.getCSVForMetaData(result));
            assertEquals(1, result.size());
            MetaData result0 = result.get(0);

            assertEquals("MDName1", result0.getName());
            assertEquals("value1", result0.getValue());
            assertNotNull(result0.getId());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

    }

    /**
     *
     */
    @Test
    public void testCreateMetaDataFailure() {

        System.out.println("---------- START: testCreateMetaDataFailure -----------");
        try {
            MetaDataFacade facade = new MetaDataFacade();
            facade.persistMetaData("tsG1", "MDName1", "21");


            try {
                facade.persistMetaData("tsG1", "MDName1", "12");
            } catch (IkatsDaoConflictException e) {
                // Good: expected error
            } catch (IkatsDaoException e) {
                fail("Unexpected error instead of IkatsDaoConflictException (case with different values");
            }

            try {
                facade.persistMetaData("tsG1", "MDName1", "21");
            } catch (IkatsDaoConflictException e) {
                // Good: expected error
            } catch (IkatsDaoException e) {
                fail("Unexpected error instead of IkatsDaoConflictException (case with same values");
            }
        } catch (Exception e) {
            fail("Unexpected error");
        }
        System.out.println("---------- END: testCreateMetaDataFailure -----------");
    }

    private void addCrit(Group<MetadataCriterion> formula, String critName, String critOperator, String rightOperandValue) {
        MetadataCriterion crit = new MetadataCriterion(critName, critOperator, rightOperandValue);
        Atom<MetadataCriterion> atomCriterion = new Atom<MetadataCriterion>();
        atomCriterion.atomicTerm = crit;
        formula.terms.add(atomCriterion);
    }

    /**
     * Add Functional Identifier to expected list
     *
     * @param expected list containing the expected values
     * @param tsuid    tsuid matching the expected value
     * @param funcid   Functional Identifier matching the expected value
     */
    private void addToScope(List<FunctionalIdentifier> expected, String tsuid, String funcid) {
        FunctionalIdentifier fid = new FunctionalIdentifier(tsuid, funcid);
        expected.add(fid);
    }

    /**
     * Test the metadata filtering based on "in" operator with single item in operand list
     */
    @Test
    public void testSearchFuncId_in_single() {

        try {
            MetaDataFacade facade = new MetaDataFacade();

            // Create the test set
            facade.persistMetaData("TS1", "MD1", "A");
            facade.persistMetaData("TS2", "MD2", "A");
            facade.persistMetaData("TS3", "MD1", "A");
            facade.persistMetaData("TS3", "MD2", "A");
            facade.persistMetaData("TS4", "MD1", "B");
            facade.persistMetaData("TS4", "MD2", "A");
            facade.persistMetaData("TS5", "MD1", "A");
            facade.persistMetaData("TS5", "MD2", "B");
            facade.persistMetaData("TS6", "MD1", "B");
            facade.persistMetaData("TS6", "MD2", "B");
            facade.persistMetaData("TS7", "MD1", "C");
            facade.persistMetaData("TS7", "MD2", "B");
            facade.persistMetaData("TS8", "MD1", "A");
            facade.persistMetaData("TS8", "MD2", "C");
            facade.persistMetaData("TS9", "MD2", "A");

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

            // Formula
            Group<MetadataCriterion> formula = new Group<MetadataCriterion>();
            formula.connector = Expression.ConnectorExpression.AND;
            formula.terms = new ArrayList<Expression<MetadataCriterion>>();

            ArrayList<FunctionalIdentifier> expected = new ArrayList<FunctionalIdentifier>();

            // Preparing results
            addCrit(formula, "MD1", "in", "A");
            addToScope(expected, "TS1", "FID1");
            addToScope(expected, "TS3", "FID3");
            addToScope(expected, "TS5", "FID5");
            addToScope(expected, "TS8", "FID8");

            // Compute
            ArrayList<FunctionalIdentifier> obtained =
                    (ArrayList<FunctionalIdentifier>) facade.searchFuncId(scope, formula);

            // Check results
            assertTrue(obtained.equals(expected));

            // Cleanup
            facade.removeMetaDataForTS("TS1");
            facade.removeMetaDataForTS("TS2");
            facade.removeMetaDataForTS("TS3");
            facade.removeMetaDataForTS("TS4");
            facade.removeMetaDataForTS("TS5");
            facade.removeMetaDataForTS("TS6");
            facade.removeMetaDataForTS("TS7");
            facade.removeMetaDataForTS("TS8");
            facade.removeMetaDataForTS("TS9");


        } catch (Exception e) {
            fail("Unexpected error");
        }
    }

    /**
     * Test the metadata filtering based on "in" operator with multiple items in operand list
     */
    @Test
    public void testSearchFuncId_in_multiple() {

        try {
            MetaDataFacade facade = new MetaDataFacade();

            // Create the test set
            facade.persistMetaData("TS1", "MD1", "A");
            facade.persistMetaData("TS2", "MD2", "A");
            facade.persistMetaData("TS3", "MD1", "A");
            facade.persistMetaData("TS3", "MD2", "A");
            facade.persistMetaData("TS4", "MD1", "B");
            facade.persistMetaData("TS4", "MD2", "A");
            facade.persistMetaData("TS5", "MD1", "A");
            facade.persistMetaData("TS5", "MD2", "B");
            facade.persistMetaData("TS6", "MD1", "B");
            facade.persistMetaData("TS6", "MD2", "B");
            facade.persistMetaData("TS7", "MD1", "C");
            facade.persistMetaData("TS7", "MD2", "B");
            facade.persistMetaData("TS8", "MD1", "A");
            facade.persistMetaData("TS8", "MD2", "C");

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

            // Formula
            Group<MetadataCriterion> formula = new Group<MetadataCriterion>();
            formula.connector = Expression.ConnectorExpression.AND;
            formula.terms = new ArrayList<Expression<MetadataCriterion>>();

            ArrayList<FunctionalIdentifier> expected = new ArrayList<FunctionalIdentifier>();

            // Preparing results
            addCrit(formula, "MD1", "in", "A;B");
            addToScope(expected, "TS1", "FID1");
            addToScope(expected, "TS3", "FID3");
            addToScope(expected, "TS4", "FID4");
            addToScope(expected, "TS5", "FID5");
            addToScope(expected, "TS6", "FID6");
            addToScope(expected, "TS8", "FID8");

            // Compute
            ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>) facade.searchFuncId(scope, formula);

            // Check results
            assertTrue(obtained.equals(expected));

            // Cleanup
            facade.removeMetaDataForTS("TS1");
            facade.removeMetaDataForTS("TS2");
            facade.removeMetaDataForTS("TS3");
            facade.removeMetaDataForTS("TS4");
            facade.removeMetaDataForTS("TS5");
            facade.removeMetaDataForTS("TS6");
            facade.removeMetaDataForTS("TS7");
            facade.removeMetaDataForTS("TS8");

        } catch (Exception e) {
            fail("Unexpected error");
        }
    }

    /**
     * Test the metadata filtering based on "not in" operator with multiple items in operand list
     */
    @Test
    public void testSearchFuncId_notin_multiple() {

        try {
            MetaDataFacade facade = new MetaDataFacade();

            // Create the test set
            facade.persistMetaData("TS1", "MD1", "A");
            facade.persistMetaData("TS2", "MD2", "A");
            facade.persistMetaData("TS3", "MD1", "A");
            facade.persistMetaData("TS3", "MD2", "A");
            facade.persistMetaData("TS4", "MD1", "B");
            facade.persistMetaData("TS4", "MD2", "A");
            facade.persistMetaData("TS5", "MD1", "A");
            facade.persistMetaData("TS5", "MD2", "B");
            facade.persistMetaData("TS6", "MD1", "B");
            facade.persistMetaData("TS6", "MD2", "B");
            facade.persistMetaData("TS7", "MD1", "C");
            facade.persistMetaData("TS7", "MD2", "B");
            facade.persistMetaData("TS8", "MD1", "A");
            facade.persistMetaData("TS8", "MD2", "C");

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

            // Formula
            Group<MetadataCriterion> formula = new Group<MetadataCriterion>();
            formula.connector = Expression.ConnectorExpression.AND;
            formula.terms = new ArrayList<Expression<MetadataCriterion>>();

            ArrayList<FunctionalIdentifier> expected = new ArrayList<FunctionalIdentifier>();

            // Preparing results
            addCrit(formula, "MD2", "not in", "A");
            addToScope(expected, "TS5", "FID5");
            addToScope(expected, "TS6", "FID6");
            addToScope(expected, "TS7", "FID7");
            addToScope(expected, "TS8", "FID8");

            // Compute
            ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>) facade.searchFuncId(scope, formula);

            // Check results
            assertTrue(obtained.equals(expected));

            // Cleanup
            facade.removeMetaDataForTS("TS1");
            facade.removeMetaDataForTS("TS2");
            facade.removeMetaDataForTS("TS3");
            facade.removeMetaDataForTS("TS4");
            facade.removeMetaDataForTS("TS5");
            facade.removeMetaDataForTS("TS6");
            facade.removeMetaDataForTS("TS7");
            facade.removeMetaDataForTS("TS8");

        } catch (Exception e) {
            fail("Unexpected error");
        }
    }

    /**
     * Test the metadata filtering based on mixed "in" and "not in" operators with multiple items in operand list
     */
    @Test
    public void testSearchFuncId_in_notin_mixed() {

        try {
            MetaDataFacade facade = new MetaDataFacade();

            // Create the test set
            facade.persistMetaData("TS1", "MD1", "A");
            facade.persistMetaData("TS2", "MD2", "A");
            facade.persistMetaData("TS3", "MD1", "A");
            facade.persistMetaData("TS3", "MD2", "A");
            facade.persistMetaData("TS4", "MD1", "B");
            facade.persistMetaData("TS4", "MD2", "A");
            facade.persistMetaData("TS5", "MD1", "A");
            facade.persistMetaData("TS5", "MD2", "B");
            facade.persistMetaData("TS6", "MD1", "B");
            facade.persistMetaData("TS6", "MD2", "B");
            facade.persistMetaData("TS7", "MD1", "C");
            facade.persistMetaData("TS7", "MD2", "B");
            facade.persistMetaData("TS8", "MD1", "A");
            facade.persistMetaData("TS8", "MD2", "C");

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

            // Formula
            Group<MetadataCriterion> formula = new Group<MetadataCriterion>();
            formula.connector = Expression.ConnectorExpression.AND;
            formula.terms = new ArrayList<Expression<MetadataCriterion>>();

            ArrayList<FunctionalIdentifier> expected = new ArrayList<FunctionalIdentifier>();

            // Preparing results
            addCrit(formula, "MD1", "in", "A;B");
            addCrit(formula, "MD2", "not in", "B;C");
            addToScope(expected, "TS3", "FID3");
            addToScope(expected, "TS4", "FID4");

            // Compute
            ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>) facade.searchFuncId(scope, formula);

            // Check results
            assertTrue(obtained.equals(expected));

            // Cleanup
            facade.removeMetaDataForTS("TS1");
            facade.removeMetaDataForTS("TS2");
            facade.removeMetaDataForTS("TS3");
            facade.removeMetaDataForTS("TS4");
            facade.removeMetaDataForTS("TS5");
            facade.removeMetaDataForTS("TS6");
            facade.removeMetaDataForTS("TS7");
            facade.removeMetaDataForTS("TS8");

        } catch (Exception e) {
            fail("Unexpected error");
        }
    }

    /**
     * Test the metadata filtering based on mixed "in" and "not in" operators with multiple items in operand list
     */
    @Test
    public void testSearchFuncId_empty_result() {

        try {
            MetaDataFacade facade = new MetaDataFacade();

            // Create the test set
            facade.persistMetaData("TS1", "MD1", "A");
            facade.persistMetaData("TS2", "MD2", "A");
            facade.persistMetaData("TS3", "MD1", "A");
            facade.persistMetaData("TS3", "MD2", "A");
            facade.persistMetaData("TS4", "MD1", "B");
            facade.persistMetaData("TS4", "MD2", "A");
            facade.persistMetaData("TS5", "MD1", "A");
            facade.persistMetaData("TS5", "MD2", "B");
            facade.persistMetaData("TS6", "MD1", "B");
            facade.persistMetaData("TS6", "MD2", "B");
            facade.persistMetaData("TS7", "MD1", "C");
            facade.persistMetaData("TS7", "MD2", "B");
            facade.persistMetaData("TS8", "MD1", "A");
            facade.persistMetaData("TS8", "MD2", "C");

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

            // Formula
            Group<MetadataCriterion> formula = new Group<MetadataCriterion>();
            formula.connector = Expression.ConnectorExpression.AND;
            formula.terms = new ArrayList<Expression<MetadataCriterion>>();

            ArrayList<FunctionalIdentifier> expected = new ArrayList<FunctionalIdentifier>();

            // Preparing results
            addCrit(formula, "MD1", "in", "F");

            // Compute
            ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>) facade.searchFuncId(scope, formula);

            // Check results
            assertTrue(obtained.equals(expected));

            // Cleanup
            facade.removeMetaDataForTS("TS1");
            facade.removeMetaDataForTS("TS2");
            facade.removeMetaDataForTS("TS3");
            facade.removeMetaDataForTS("TS4");
            facade.removeMetaDataForTS("TS5");
            facade.removeMetaDataForTS("TS6");
            facade.removeMetaDataForTS("TS7");
            facade.removeMetaDataForTS("TS8");

        } catch (Exception e) {
            fail("Unexpected error");
        }
    }


    /**
     * Test the metadata filtering based on "inTable" operator with nominal behavior:
     * - Table well formatted (The filter will be done on column "FlightId")
     *     * all necessary columns are present
     *     * Id are not contiguous
     * - TS match metadata name / value pair
     * - TS doesn't match the following cases:
     *     * metadata name with value not in expected values
     *     * different metadata name matching the value
     *     * No metadata name for a TS
     *
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#searchFuncId}

     */
    @Test
    public void testSearchFuncId_inTable_Nominal() {

        try {
            MetaDataFacade facade = new MetaDataFacade();

            // Create the test set
            facade.persistMetaData("TS1", "Identifier", "1"); // Match
            facade.persistMetaData("TS2", "Identifier", "2"); // Match
            facade.persistMetaData("TS3", "Identifier", "5");
            facade.persistMetaData("TS4", "Identifier", "8"); // Match
            facade.persistMetaData("TS5", "Identifier", "9");
            facade.persistMetaData("TS5", "OtherIdentifier", "4");
            facade.persistMetaData("TS6", "Identifier", "0");
            facade.persistMetaData("TS7", "NoIdentifier", "4");
            facade.persistMetaData("TS8", "Identifier", "10");
            facade.persistMetaData("TS9", "Identifier", "42"); // Match

            // Prepare the Table data
            String tableContent = "MainId;Target\n"
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

            // Formula
            Group<MetadataCriterion> formula = new Group<MetadataCriterion>();
            formula.connector = Expression.ConnectorExpression.AND;
            formula.terms = new ArrayList<Expression<MetadataCriterion>>();
            addCrit(formula, "Identifier", "inTable", "TestTable.MainId");

            // Preparing results
            ArrayList<FunctionalIdentifier> expected = new ArrayList<FunctionalIdentifier>();
            addToScope(expected, "TS1", "FID1");
            addToScope(expected, "TS3", "FID3");
            addToScope(expected, "TS4", "FID4");
            addToScope(expected, "TS9", "FID9");

            // Compute
            ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>)
                    facade.searchFuncId(scope, formula);

            // Check results
            assertTrue(obtained.equals(expected));

            // Cleanup
            facade.removeMetaDataForTS("TS1");
            facade.removeMetaDataForTS("TS2");
            facade.removeMetaDataForTS("TS3");
            facade.removeMetaDataForTS("TS4");
            facade.removeMetaDataForTS("TS5");
            facade.removeMetaDataForTS("TS6");
            facade.removeMetaDataForTS("TS7");
            facade.removeMetaDataForTS("TS8");
            facade.removeMetaDataForTS("TS9");


        } catch (Exception e) {
            fail("Unexpected error");
        }
    }

    /**
     * Test the metadata filtering based on "inTable" operator with redundant identifier:
     * - Table well formatted (The filter will be done on column "FlightId")
     *     * Id contains a doubloon
     * - TS match metadata name / value pair
     * - TS doesn't match the following cases:
     *     * metadata name with value not in expected values
     *     * different metadata name matching the value
     *     * No metadata name for a TS
     *
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#searchFuncId}
     */
    @Test
    public void testSearchFuncId_inTable_RedundantIdentifiers() {

        try {
            MetaDataFacade facade = new MetaDataFacade();

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

            // Formula
            Group<MetadataCriterion> formula = new Group<MetadataCriterion>();
            formula.connector = Expression.ConnectorExpression.AND;
            formula.terms = new ArrayList<Expression<MetadataCriterion>>();
            addCrit(formula, "Identifier", "inTable", "TestTable.MainId");

            // Preparing results
            ArrayList<FunctionalIdentifier> expected = new ArrayList<FunctionalIdentifier>();
            addToScope(expected, "TS1", "FID1");
            addToScope(expected, "TS3", "FID3");
            addToScope(expected, "TS4", "FID4");
            addToScope(expected, "TS9", "FID9");

            // Compute
            ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>)
                    facade.searchFuncId(scope, formula);

            // Check results
            assertTrue(obtained.equals(expected));

            // Cleanup
            facade.removeMetaDataForTS("TS1");
            facade.removeMetaDataForTS("TS2");
            facade.removeMetaDataForTS("TS3");
            facade.removeMetaDataForTS("TS4");
            facade.removeMetaDataForTS("TS5");
            facade.removeMetaDataForTS("TS6");
            facade.removeMetaDataForTS("TS7");
            facade.removeMetaDataForTS("TS8");
            facade.removeMetaDataForTS("TS9");


        } catch (Exception e) {
            fail("Unexpected error");
        }
    }

    /**
     * Test the metadata filtering based on "inTable" operator without any match:
     * - Table well formatted (The filter will be done on column "FlightId")
     *     * all necessary columns are present
     *     * Id are not contiguous
     *
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#searchFuncId}
     */
    @Test
    public void testSearchFuncId_inTable_NoMatch() {

        try {
            MetaDataFacade facade = new MetaDataFacade();

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
                    + "101;A\n"
                    + "102;B\n"
                    + "103;C\n"
                    + "104;D\n"
                    + "142;A\n"
                    + "106;B\n"
                    + "107;C\n"
                    + "108;D\n";
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

            // Formula
            Group<MetadataCriterion> formula = new Group<MetadataCriterion>();
            formula.connector = Expression.ConnectorExpression.AND;
            formula.terms = new ArrayList<Expression<MetadataCriterion>>();
            addCrit(formula, "Identifier", "inTable", "TestTable.MainId");

            // Compute
            ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>)
                    facade.searchFuncId(scope, formula);

            // Check results
            assertEquals(0, obtained.size());

            // Cleanup
            facade.removeMetaDataForTS("TS1");
            facade.removeMetaDataForTS("TS2");
            facade.removeMetaDataForTS("TS3");
            facade.removeMetaDataForTS("TS4");
            facade.removeMetaDataForTS("TS5");
            facade.removeMetaDataForTS("TS6");
            facade.removeMetaDataForTS("TS7");
            facade.removeMetaDataForTS("TS8");
            facade.removeMetaDataForTS("TS9");


        } catch (Exception e) {
            fail("Unexpected error");
        }
    }

    /**
     * Test the metadata filtering based on "inTable" operator with nominal behavior:
     * - Table well formatted (The filter will be done on column "FlightId")
     *     * all necessary columns are present
     *     * Id are not contiguous
     * - No column defined in criterion but metadata name match the column name
     * - TS match metadata name / value pair
     * - TS doesn't match the following cases:
     *     * metadata name with value not in expected values
     *     * different metadata name matching the value
     *     * No metadata name for a TS
     *
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#searchFuncId}
     */
    @Test
    public void testSearchFuncId_inTable_NoColumnButMatch() {

        try {
            MetaDataFacade facade = new MetaDataFacade();

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

            // Formula
            Group<MetadataCriterion> formula = new Group<MetadataCriterion>();
            formula.connector = Expression.ConnectorExpression.AND;
            formula.terms = new ArrayList<Expression<MetadataCriterion>>();
            addCrit(formula, "Identifier", "inTable", "TestTable");

            // Preparing results
            ArrayList<FunctionalIdentifier> expected = new ArrayList<FunctionalIdentifier>();
            addToScope(expected, "TS1", "FID1");
            addToScope(expected, "TS3", "FID3");
            addToScope(expected, "TS4", "FID4");
            addToScope(expected, "TS9", "FID9");

            // Compute
            ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>)
                    facade.searchFuncId(scope, formula);

            // Check results
            assertTrue(obtained.equals(expected));

            // Cleanup
            facade.removeMetaDataForTS("TS1");
            facade.removeMetaDataForTS("TS2");
            facade.removeMetaDataForTS("TS3");
            facade.removeMetaDataForTS("TS4");
            facade.removeMetaDataForTS("TS5");
            facade.removeMetaDataForTS("TS6");
            facade.removeMetaDataForTS("TS7");
            facade.removeMetaDataForTS("TS8");
            facade.removeMetaDataForTS("TS9");


        } catch (Exception e) {
            fail("Unexpected error");
        }
    }

    /**
     * Test the metadata filtering based on "inTable" operator with no match:
     * - Table well formatted (The filter will be done on column "FlightId")
     *    * all necessary columns are present
     *    * Id are not contiguous
     * - No column defined in criterion and metadata name doesn't match the column name
     *
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#searchFuncId}
     */
    @Test
    public void testSearchFuncId_inTable_NoColumnNoMatch() {

        try {
            MetaDataFacade facade = new MetaDataFacade();

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

            // Formula
            Group<MetadataCriterion> formula = new Group<MetadataCriterion>();
            formula.connector = Expression.ConnectorExpression.AND;
            formula.terms = new ArrayList<Expression<MetadataCriterion>>();
            addCrit(formula, "Identifier", "inTable", "TestTable");

            // Compute
            ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>)
                    facade.searchFuncId(scope, formula);

            // Check results
            assertEquals(0, obtained.size());

            // Cleanup
            facade.removeMetaDataForTS("TS1");
            facade.removeMetaDataForTS("TS2");
            facade.removeMetaDataForTS("TS3");
            facade.removeMetaDataForTS("TS4");
            facade.removeMetaDataForTS("TS5");
            facade.removeMetaDataForTS("TS6");
            facade.removeMetaDataForTS("TS7");
            facade.removeMetaDataForTS("TS8");
            facade.removeMetaDataForTS("TS9");


        } catch (Exception e) {
            fail("Unexpected error");
        }
    }

    /**
     * Test the metadata filtering based on "inTable" operator with no match (different case):
     * - Table well formatted (The filter will be done on column "FlightId")
     *    * all necessary columns are present
     *    * Id are not contiguous
     * - No column defined in criterion and metadata name doesn't match the column name (they have different case)
     *
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#searchFuncId}
     */
    @Test
    public void testSearchFuncId_inTable_NoColumnDiffCase() {

        try {
            MetaDataFacade facade = new MetaDataFacade();

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

            // Formula
            Group<MetadataCriterion> formula = new Group<MetadataCriterion>();
            formula.connector = Expression.ConnectorExpression.AND;
            formula.terms = new ArrayList<Expression<MetadataCriterion>>();
            addCrit(formula, "Identifier", "inTable", "TestTable");

            // Compute
            ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>)
                    facade.searchFuncId(scope, formula);

            // Check results
            assertEquals(0, obtained.size());

            // Cleanup
            facade.removeMetaDataForTS("TS1");
            facade.removeMetaDataForTS("TS2");
            facade.removeMetaDataForTS("TS3");
            facade.removeMetaDataForTS("TS4");
            facade.removeMetaDataForTS("TS5");
            facade.removeMetaDataForTS("TS6");
            facade.removeMetaDataForTS("TS7");
            facade.removeMetaDataForTS("TS8");
            facade.removeMetaDataForTS("TS9");


        } catch (Exception e) {
            fail("Unexpected error");
        }
    }

    /**
     * Test the metadata filtering based on "inTable" operator with table not found
     *
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#searchFuncId}
     */
    @Test
    public void testSearchFuncId_inTable_NoTableFound() {

        try {
            MetaDataFacade facade = new MetaDataFacade();

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

            // Formula
            Group<MetadataCriterion> formula = new Group<MetadataCriterion>();
            formula.connector = Expression.ConnectorExpression.AND;
            formula.terms = new ArrayList<Expression<MetadataCriterion>>();
            addCrit(formula, "Identifier", "inTable", "TestTable");


            // Compute
            //TODO Handle exception here
            ArrayList<FunctionalIdentifier> obtained = (ArrayList<FunctionalIdentifier>)
                    facade.searchFuncId(scope, formula);

            // Cleanup
            facade.removeMetaDataForTS("TS1");
            facade.removeMetaDataForTS("TS2");
            facade.removeMetaDataForTS("TS3");
            facade.removeMetaDataForTS("TS4");
            facade.removeMetaDataForTS("TS5");
            facade.removeMetaDataForTS("TS6");
            facade.removeMetaDataForTS("TS7");
            facade.removeMetaDataForTS("TS8");
            facade.removeMetaDataForTS("TS9");


        } catch (Exception e) {
            fail("Unexpected error");
        }
    }
}
