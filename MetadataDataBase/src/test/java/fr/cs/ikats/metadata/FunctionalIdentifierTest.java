package fr.cs.ikats.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

	@Test
	public void testPersist() {

		String testCaseName = "testPersist";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);

			MetaDataFacade facade = new MetaDataFacade();

			Map<String, String> values = new HashMap<String, String>();
			values.put("tsuid1", "mon_id_fonctionel1");
			int added = facade.persistFunctionalIdentifier(values);
			assertEquals(1, added);
			List<String> tsuids = new ArrayList<String>();
			tsuids.add("tsuid1");
			facade.removeFunctionalIdentifier(tsuids);

			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}
	}

	@Test
	public void testPersist_DG_Doublon() {

		String testCaseName = "testPersist_DG_Doublon";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);

			MetaDataFacade facade = new MetaDataFacade();

			Map<String, String> values = new HashMap<String, String>();
			values.put("tsuid1_dg", "mon_id_fonctionel1");
			values.put("tsuid1_dg", "mon_id_fonctionel2");
			int added = facade.persistFunctionalIdentifier(values);
			assertEquals(1, added);
			List<String> tsuids = new ArrayList<String>();
			tsuids.add("tsuid1_dg");
			facade.removeFunctionalIdentifier(tsuids);

			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}

	}

	/**
	 * Test method for {@link fr.cs.ikats.metadata.MetaDataFacade#removeMetaDataForTS(java.lang.String)} .
	 */
	@Test
	public void testRemove() {

		String testCaseName = "testRemove";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);

			MetaDataFacade facade = new MetaDataFacade();

			Map<String, String> values = new HashMap<String, String>();
			values.put("tsuid2", "mon_id_fonctionel2");
			values.put("tsuid3", "mon_id_fonctionel3");
			int added = facade.persistFunctionalIdentifier(values);
			assertEquals(2, added);
			List<String> tsuids = new ArrayList<String>();
			tsuids.add("tsuid3");
			facade.removeFunctionalIdentifier(tsuids);
			added = facade.persistFunctionalIdentifier(values);
			assertEquals(1, added);

			// clean the data to avoid tests collision
			facade.removeFunctionalIdentifier(new ArrayList<String>(values.keySet()));

			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}

	}

	/**
	 * Test method for {@link fr.cs.ikats.metadata.MetaDataFacade#getMetaDataForTS(java.lang.String)} .
	 */
	@Test
	public void testlist() {

		String testCaseName = "testlist";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);

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

			// clean the data to avoid tests collision
			facade.removeFunctionalIdentifier(new ArrayList<String>(values.keySet()));

			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}

	}

	/**
	 * Test method for {@link fr.cs.ikats.metadata.MetaDataFacade#getMetaDataForTS(java.lang.String)} .
	 */
	@Test
	public void testGetByFuncIdAndByTsuid() {

		String testCaseName = "testGetByFuncIdAndByTsuid";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);

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

			// clean the data to avoid tests collision
			facade.removeFunctionalIdentifier(new ArrayList<String>(values.keySet()));

			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}

	}

	@Test
	public void testGetbyFuncIdAndByTsuid_DG() {

		String testCaseName = "testGetbyFuncIdAndByTsuid_DG";
		boolean isNominal = false;
		try {
			start(testCaseName, isNominal);

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

			// clean the data to avoid tests collision
			facade.removeFunctionalIdentifier(new ArrayList<String>(values.keySet()));

			endWithFailure(testCaseName, new Exception("Expects raised IkatsDaoMissingRessource"));

		} catch (IkatsDaoMissingRessource missingError) {
			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}

	}

	/**
	 * Test method for {@link fr.cs.ikats.metadata.MetaDataFacade#getFunctionalIdentifiersList(java.lang.String)} .
	 */
	@Test
	public void testGetAllFid() {
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

		// clean the data to avoid tests collision
		try {
			facade.removeFunctionalIdentifier(new ArrayList<String>(values.keySet()));
		} catch (IkatsDaoConflictException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IkatsDaoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
