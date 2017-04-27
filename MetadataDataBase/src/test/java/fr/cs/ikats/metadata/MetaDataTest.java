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
     * For each test case: purge the
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
     * {@link fr.cs.ikats.metadata.MetaDataFacade#persistMetaData(java.lang.String, java.lang.String, java.util.List)}
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
            facade.persistMetaData("tsuidA01", "thatsastring", "blabla", "string");
            facade.persistMetaData("tsuidA02", "thatsanumber", "12", "number");
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
    /**
     *
     */
    @Test
    public void testSearchFuncId() {

        try {
            MetaDataFacade facade = new MetaDataFacade();

            // Create the test set
            facade.persistMetaData("TS1", "MD1", "1");
            facade.persistMetaData("TS2", "MD1", "2");
            facade.persistMetaData("TS3", "MD2", "2");
            facade.persistMetaData("TS4", "MD2", "1");
            facade.persistMetaData("TS5", "MD1", "11");
            facade.persistMetaData("TS5", "MD2", "22");
            facade.persistMetaData("TS6", "MD1", "1");
            facade.persistMetaData("TS6", "MD2", "2");

            // Create the initial scope
            List<FunctionalIdentifier> scope = new ArrayList<FunctionalIdentifier>();
            scope.add(new FunctionalIdentifier("TS1","FID1"));
            scope.add(new FunctionalIdentifier("TS2","FID2"));
            scope.add(new FunctionalIdentifier("TS3","FID3"));
            scope.add(new FunctionalIdentifier("TS4","FID4"));
            scope.add(new FunctionalIdentifier("TS5","FID5"));
            scope.add(new FunctionalIdentifier("TS6","FID6"));

            // Formula
            Group<MetadataCriterion> formula;

            facade.searchFuncId(scope, formula);


        } catch (Exception e) {
            fail("Unexpected error");
        }
    }
}
