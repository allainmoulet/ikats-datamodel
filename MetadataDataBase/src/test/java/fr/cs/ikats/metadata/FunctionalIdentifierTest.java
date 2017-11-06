package fr.cs.ikats.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import fr.cs.ikats.common.junit.CommonTest;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;

/**
 *
 */
public class FunctionalIdentifierTest extends CommonTest {
    
    /**
     * Remove all data at the end of the tests
     * 
     * @throws IkatsDaoConflictException
     * @throws IkatsDaoException
     */
    @AfterClass
    public static void tearDown() throws IkatsDaoConflictException, IkatsDaoException {
        resetDB();
    }
    
    /**
     * Remove all data before any test execution
     * 
     * @throws IkatsDaoConflictException
     * @throws IkatsDaoException
     */
    @Before
    public void setUpTest() throws IkatsDaoConflictException, IkatsDaoException {
        resetDB();
    }
    
    /**
     * Remove all funcid data 
     * 
     * @throws IkatsDaoConflictException
     * @throws IkatsDaoException
     */
    protected static void resetDB() throws IkatsDaoConflictException, IkatsDaoException {
        MetaDataFacade metaDataFacade = new MetaDataFacade();
        List<FunctionalIdentifier> funcIdList = metaDataFacade.getFunctionalIdentifiersList();
        // get only the tsuids list to be able to call the remove method
        List<String> tsuids = funcIdList.stream().map(FunctionalIdentifier::getTsuid).collect(Collectors.toList());
        metaDataFacade.removeFunctionalIdentifier(tsuids);
    }
    
	@Test
	public void testPersist() throws IkatsDaoConflictException, IkatsDaoException {

		MetaDataFacade facade = new MetaDataFacade();

		Map<String, String> values = new HashMap<String, String>();
		values.put("tsuid1", "mon_id_fonctionel1");
		int added = facade.persistFunctionalIdentifier(values);
		assertEquals(1, added);
	}

	@Test(expected=IkatsDaoConflictException.class)
	public void testPersist_DG_Doublon() throws IkatsDaoConflictException, IkatsDaoException {

		MetaDataFacade facade = new MetaDataFacade();

		int added = facade.persistFunctionalIdentifier("tsuid1_dg", "mon_id_fonctionel1");
		assertEquals(1, added);
		
		// Will throw the IkatsDaoConflictException
		added = facade.persistFunctionalIdentifier("tsuid1_dg", "mon_id_fonctionel1");
	}

	/**
	 * Test method for {@link fr.cs.ikats.metadata.MetaDataFacade#removeMetaDataForTS(java.lang.String)} .
	 * @throws IkatsDaoException 
	 * @throws IkatsDaoConflictException 
	 */
	@Test
	public void testRemove() throws IkatsDaoConflictException, IkatsDaoException {

		MetaDataFacade facade = new MetaDataFacade();

		Map<String, String> values = new HashMap<String, String>();
		values.put("tsuid2", "mon_id_fonctionel2");
		values.put("tsuid3", "mon_id_fonctionel3");
		int added = facade.persistFunctionalIdentifier(values);
		assertEquals(2, added);
	}

	/**
	 * Test method for {@link fr.cs.ikats.metadata.MetaDataFacade#getMetaDataForTS(java.lang.String)} .
	 * @throws IkatsDaoException 
	 * @throws IkatsDaoConflictException 
	 */
	@Test
	public void testlist() throws IkatsDaoConflictException, IkatsDaoException {

		MetaDataFacade facade = new MetaDataFacade();

		Map<String, String> values = new HashMap<String, String>();
		values.put("tsuid4", "mon_id_fonctionel4");
		values.put("tsuid5", "mon_id_fonctionel5");
		values.put("tsuid6", "mon_id_fonctionel6");
		values.put("tsuid7", "mon_id_fonctionel7");
		values.put("tsuid8", "mon_id_fonctionel8");
		int added = facade.persistFunctionalIdentifier(values);
		assertEquals(5, added);
		List<String> tsuids = new ArrayList<String>();
		tsuids.add("tsuid4");
		tsuids.add("tsuid5");
		tsuids.add("tsuid6");
		tsuids.add("tsuid7");
		tsuids.add("tsuid8");
		List<FunctionalIdentifier> result = facade.getFunctionalIdentifierByTsuidList(tsuids);
		assertNotNull(result);
		assertEquals(5, result.size());
		assertEquals("mon_id_fonctionel4", result.get(0).getFuncId());
		assertEquals("mon_id_fonctionel5", result.get(1).getFuncId());

		tsuids = new ArrayList<String>();
		tsuids.add("tsuid9");
		result = facade.getFunctionalIdentifierByTsuidList(tsuids);
		assertTrue(result.isEmpty());
	}

	/**
	 * Test method for {@link fr.cs.ikats.metadata.MetaDataFacade#getMetaDataForTS(java.lang.String)} .
	 * @throws IkatsDaoException 
	 * @throws IkatsDaoMissingRessource 
	 * @throws IkatsDaoConflictException 
	 */
	@Test
	public void testGetByFuncIdAndByTsuid() throws IkatsDaoConflictException, IkatsDaoMissingRessource, IkatsDaoException {

		MetaDataFacade facade = new MetaDataFacade();

		Map<String, String> values = new HashMap<String, String>();
		values.put("tsuid9", "mon_id_fonctionel9");
		values.put("tsuid10", "mon_id_fonctionel10");
		values.put("tsuid11", "mon_id_fonctionel11");
		values.put("tsuid12", "mon_id_fonctionel12");
		int added = facade.persistFunctionalIdentifier(values);
		assertEquals(4, added);
		FunctionalIdentifier result = facade.getFunctionalIdentifierByFuncId("mon_id_fonctionel9");
		assertEquals("tsuid9", result.getTsuid());
		result = facade.getFunctionalIdentifierByFuncId("mon_id_fonctionel11");
		assertEquals("tsuid11", result.getTsuid());
		result = facade.getFunctionalIdentifierByTsuid("tsuid12");
		assertEquals("mon_id_fonctionel12", result.getFuncId());
	}

	@Test(expected = IkatsDaoMissingRessource.class)
	public void testGetbyFuncIdAndByTsuid_DG() throws IkatsDaoConflictException, IkatsDaoMissingRessource, IkatsDaoException {

		MetaDataFacade facade = new MetaDataFacade();

		Map<String, String> values = new HashMap<String, String>();
		values.put("tsuid9", "mon_id_fonctionel9");
		values.put("tsuid10", "mon_id_fonctionel10");
		values.put("tsuid11", "mon_id_fonctionel11");
		values.put("tsuid12", "mon_id_fonctionel12");
		int added = facade.persistFunctionalIdentifier(values);
		assertEquals(4, added);

		FunctionalIdentifier result = facade.getFunctionalIdentifierByFuncId("mon_id_fonctionel14");
		assertNull(result);
		result = facade.getFunctionalIdentifierByTsuid("tsuid14");
		assertNull(result);
	}

	/**
	 * Test method for {@link fr.cs.ikats.metadata.MetaDataFacade#getFunctionalIdentifiersList(java.lang.String)} .
	 * @throws IkatsDaoException 
	 * @throws IkatsDaoConflictException 
	 */
	@Test
	public void testGetAllFid() throws IkatsDaoConflictException, IkatsDaoException {
		MetaDataFacade facade = new MetaDataFacade();

		List<FunctionalIdentifier> result = facade.getFunctionalIdentifiersList();
		assertEquals(0, result.size());

		Map<String, String> values = new HashMap<String, String>();
		values.put("tsuid9", "mon_id_fonctionel9");
		values.put("tsuid10", "mon_id_fonctionel10");
		values.put("tsuid11", "mon_id_fonctionel11");
		values.put("tsuid12", "mon_id_fonctionel12");
		int added = facade.persistFunctionalIdentifier(values);
		assertEquals(4, added);

		result = facade.getFunctionalIdentifiersList();
		assertEquals(4, result.size());
	}

	/**
	 * Tests that implemented equals, hashcode are exact or/and robust to null values.
	 */
	@Test
	public void testRobustness() {
		(new FunctionalIdentifier(null, null)).toString();
		(new FunctionalIdentifier(null, null)).hashCode();
		assertTrue((new FunctionalIdentifier(null, null)).equals(new FunctionalIdentifier(null, null)));
		assertTrue(!(new FunctionalIdentifier(null, null)).equals("string"));
		assertNotSame(new FunctionalIdentifier("HI", "HA"), new FunctionalIdentifier("HU", "HA"));
		assertFalse((new FunctionalIdentifier("HI", "HA")).equals(new FunctionalIdentifier("HI", "HU")));

	}
}
