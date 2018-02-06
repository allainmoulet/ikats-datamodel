/**
 * LICENSE:
 * --------
 * Copyright 2017-2018 CS SYSTEMES D'INFORMATION
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

package fr.cs.ikats.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoInvalidValueException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingResource;
import fr.cs.ikats.common.expr.Atom;
import fr.cs.ikats.common.expr.Expression;
import fr.cs.ikats.common.expr.Group;
import fr.cs.ikats.metadata.dao.MetaDataDAO;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.metadata.model.MetaData.MetaType;
import fr.cs.ikats.metadata.model.MetadataCriterion;


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
    private static MetaDataDAO dao;

    /**
     * Initializing the class
     */
    @BeforeClass
    public static void setUpBeforClass() {

    	dao = new MetaDataDAO();
    	
    }

    /**
     * For each test case: purge the database
     */
    @Before
    public void init_db() throws IkatsDaoException {

        Session session = dao.getSession();

        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            Query q = session.createQuery("DELETE FROM MetaData where tsuid like '%ts%'");
            q.executeUpdate();
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) {
                tx.rollback();
            }
            throw e;
        } finally {
            session.close();
        }
    }


    /**
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#persistMetaData(java.lang.String, java.lang.String, java.lang.String)}
     * .
     *
     * @throws IkatsDaoException
     * @throws IkatsDaoConflictException
     */
    @Test
    public void testPersistMetaData() throws IkatsDaoConflictException, IkatsDaoException {
        MetaDataFacade facade = new MetaDataFacade();
        Integer results = facade.persistMetaData("tsuid1", "MDName1", "value1");
        assertTrue("Error, results not imported", results >= 0);
    }

    @Test
    public void testMetaDataTypes() throws IkatsDaoConflictException, IkatsDaoInvalidValueException, IkatsDaoException {
        MetaDataFacade facade = new MetaDataFacade();
        facade.persistMetaData("tsuidB01", "thatsastring", "blabla", "string");
        facade.persistMetaData("tsuidB02", "thatsanumber", "12", "number");
        Map<String, String> metaTypesTable = facade.getMetaDataTypes();
        assertNotNull(metaTypesTable);
        assertEquals("number", metaTypesTable.get("thatsanumber"));
        assertEquals("string", metaTypesTable.get("thatsastring"));
    }

    /**
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#persistMetaData(java.lang.String, java.lang.String, java.lang.String)}
     * .
     *
     * @throws IkatsDaoException
     * @throws IkatsDaoConflictException
     */
    @Test
    public void testUpdateMetaData() throws IkatsDaoConflictException, IkatsDaoException {
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

    }

    /**
     * @throws IkatsDaoException
     * @throws IkatsDaoMissingResource
     */
    @Test(expected = IkatsDaoMissingResource.class)
    public void testUpdateMetadataFailure() throws IkatsDaoMissingResource, IkatsDaoException {
        MetaDataFacade facade = new MetaDataFacade();
        facade.updateMetaData("tsuidF01", "MDFailedUpdateWithoutCreate", "value");
        fail("IkatsDaoMissingResource should be raised");
    }

    /**
     * Test the failures of searchs by TSUIDs
     *
     * @throws IkatsDaoException
     * @throws IkatsDaoMissingResource
     */
    @Test(expected = IkatsDaoMissingResource.class)
    public void testSearchByTsuidFailure() throws IkatsDaoMissingResource, IkatsDaoException {
        MetaDataFacade facade = new MetaDataFacade();
        // test failure: non existant tsuid
        facade.getMetaDataForTS("tsuidM01");
    }

    /**
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#persistMetaData(java.lang.String, java.lang.String, java.lang.String)}
     * .
     *
     * @throws IkatsDaoException
     * @throws IkatsDaoConflictException
     */
    @Test
    public void testCreateMetaDataDuplicate() throws IkatsDaoConflictException, IkatsDaoException {
        MetaDataFacade facade = new MetaDataFacade();
        Integer results1 = facade.persistMetaData("tsuidBX", "MDName1", "v1");
        Integer results2 = facade.persistMetaData("tsuidBX", "MDName2", "v1");
        Integer results3 = facade.persistMetaData("tsuidBY", "MDName1", "v2");
        assertTrue("Error, results1 not imported", results1 >= 0);
        assertTrue("Error, results3 not imported", results2 >= 0);
        assertTrue("Error, results4 not imported", results3 >= 0);
    }

    /**
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#removeMetaDataForTS(java.lang.String)}
     * .
     *
     * @throws IkatsDaoException
     * @throws IkatsDaoConflictException
     */
    @Test(expected = IkatsDaoMissingResource.class)
    public void testRemoveMetaDataForTS() throws IkatsDaoConflictException, IkatsDaoException {
        MetaDataFacade facade = new MetaDataFacade();

        facade.persistMetaData("tsuidC3", "MDName1", "value1");
        facade.persistMetaData("tsuidC3", "MDName2", "value1");
        facade.persistMetaData("tsuidC4", "MDName1", "value2");


        List<MetaData> result = facade.getMetaDataForTS("tsuidC3");
        assertEquals(2, result.size());
        facade.removeMetaDataForTS("tsuidC3", "MDName1");
        result = facade.getMetaDataForTS("tsuidC3");
        assertEquals(1, result.size());

        facade.removeMetaDataForTS("tsuidC4");
        // assert exception 
        facade.getMetaDataForTS("tsuidC4");
    }

    /**
     * Test method for
     * {@link fr.cs.ikats.metadata.MetaDataFacade#getMetaDataForTS(java.lang.String)}
     * .
     *
     * @throws IkatsDaoException
     * @throws IkatsDaoConflictException
     */
    @Test
    public void testGetMetaDataForTS() throws IkatsDaoConflictException, IkatsDaoException {
        MetaDataFacade facade = new MetaDataFacade();

        facade.persistMetaData("tsuidD5", "MDName1", "value1");

        List<MetaData> result = facade.getMetaDataForTS("tsuidD5");
        assertEquals(1, result.size());
        MetaData result0 = result.get(0);

        assertEquals("MDName1", result0.getName());
        assertEquals("value1", result0.getValue());
        assertNotNull(result0.getId());
    }

    /**
     * @throws IkatsDaoException
     * @throws IkatsDaoConflictException
     */
    @Test
    public void testCreateMetaDataFailure() throws IkatsDaoConflictException, IkatsDaoException {
        MetaDataFacade facade = new MetaDataFacade();
        facade.persistMetaData("tsG1", "MDName1", "21");


        try {
            facade.persistMetaData("tsG1", "MDName1", "12");
        } catch (IkatsDaoConflictException e) {
            // Good: expected error
        }

        try {
            facade.persistMetaData("tsG1", "MDName1", "21");
        } catch (IkatsDaoConflictException e) {
            // Good: expected error
        }
    }

    private void addCrit(Group formula, String critName, String critOperator, String rightOperandValue) {
        MetadataCriterion crit = new MetadataCriterion(critName, critOperator, rightOperandValue);
        Atom<MetadataCriterion> atomCriterion = new Atom<MetadataCriterion>();
        atomCriterion.setAtomicTerm(crit);
        formula.getTerms().add(atomCriterion);
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
     * Test the metadata filtering based on "in" operator with single item in operand list
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testSearchFuncId_in_single() throws IkatsDaoException {
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
        Group formula = new Group();
        formula.setConnector(Expression.ConnectorExpression.AND);
        formula.setTerms(new ArrayList<>());

        ArrayList<FunctionalIdentifier> expected = new ArrayList<>();

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
    }

    /**
     * Test the metadata filtering based on "in" operator with not right operand for "in"
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testSearchFuncId_in_noOperand() throws IkatsDaoException {
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
        Group formula = new Group();
        formula.setConnector(Expression.ConnectorExpression.AND);
        formula.setTerms(new ArrayList<>());

        // Preparing results
        addCrit(formula, "MD1", "in", "");

        // Compute
        ArrayList<FunctionalIdentifier> obtained =
                (ArrayList<FunctionalIdentifier>) facade.searchFuncId(scope, formula);

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
    }

    /**
     * Test the metadata filtering based on "in" operator with multiple items in operand list
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testSearchFuncId_in_multiple() throws IkatsDaoException {
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
        Group formula = new Group();
        formula.setConnector(Expression.ConnectorExpression.AND);
        formula.setTerms(new ArrayList<>());

        ArrayList<FunctionalIdentifier> expected = new ArrayList<>();

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
    }

    /**
     * Test the metadata filtering based on "not in" operator with multiple items in operand list
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testSearchFuncId_notin_multiple() throws IkatsDaoException {
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
        Group formula = new Group();
        formula.setConnector(Expression.ConnectorExpression.AND);
        formula.setTerms(new ArrayList<>());

        ArrayList<FunctionalIdentifier> expected = new ArrayList<>();

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
    }

    /**
     * Test the metadata filtering based on mixed "in" and "not in" operators with multiple items in operand list
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testSearchFuncId_in_notin_mixed() throws IkatsDaoException {
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
        Group formula = new Group();
        formula.setConnector(Expression.ConnectorExpression.AND);
        formula.setTerms(new ArrayList<>());

        ArrayList<FunctionalIdentifier> expected = new ArrayList<>();

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

    }

    /**
     * Test the metadata filtering based on mixed "in" and "not in" operators with multiple items in operand list
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testSearchFuncId_empty_result() throws IkatsDaoException {
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
        Group formula = new Group();
        formula.setConnector(Expression.ConnectorExpression.AND);
        formula.setTerms(new ArrayList<>());

        ArrayList<FunctionalIdentifier> expected = new ArrayList<>();

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
    }

    /**
     * Tests that implemented equals, hashcode are exact or/and robust to null values.
     */
    @Test
    public void testRobustness() {
        MetaData meta = new MetaData();
        meta.hashCode();
        assertFalse(meta.equals("toto"));
        assertTrue(meta.equals(meta));
        MetaData meta2 = new MetaData();
        meta.setTsuid("tsuid1");
        meta2.setTsuid(meta.getTsuid());
        meta.setName("name1");
        meta2.setName(meta.getName());
        meta.setDType(MetaType.number);
        meta.setValue("12");
        meta2.setDType(meta.getDType());
        meta2.setValue(meta.getValue());

        assertEquals(meta, meta2);
        meta.setValue("11");
        assertFalse(meta.equals(meta2));


    }
}
