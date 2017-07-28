package fr.cs.ikats.operators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.junit.CommonTest;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.process.data.model.ProcessData;
import fr.cs.ikats.temporaldata.business.DataSetManager;
import fr.cs.ikats.temporaldata.business.MetaDataManager;
import fr.cs.ikats.temporaldata.business.ProcessDataManager;
import fr.cs.ikats.temporaldata.business.Table;
import fr.cs.ikats.temporaldata.business.TableElement;
import fr.cs.ikats.temporaldata.business.TableInfo;
import fr.cs.ikats.temporaldata.business.TableInfo.DataLink;
import fr.cs.ikats.temporaldata.business.TableManager;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;

/**
 * Tests the operator JoinTableWithTs
 * 
 * In this test: IDS will obey to specific format describbed below
 */
public class JoinTableWithTsTest extends CommonTest {

	// ======================================================================
	// IDs Specific format in this test:
	//
	// <TSUID> = "tsuid_" + <FlightId> + "_" + <metric>
	//
	// <FuncId> = "funcId_" + <FlightId> + "_" + <metric>
	// ======================================================================

	// whole dataset: contains the entire set of TS, defined by meta/funcId
	// ---------------------------------------------------------------------
	private static final String WHOLE_DATASET_NAME = "wholeDataset_JoinTableWithTsTest";
	private final static List<String> ALL_FLIGHT_IDS = Arrays.asList("1", "2", "10", "11", "50", "51");
	private final static List<String> ALL_METRICS = Arrays.asList("HEADING", "WS1", "WS2", "WS3", "WS4", "SLOPE");

	// datasetMetadata initialized by init():
	private static List<Integer> datasetMetadata;
	// datasetFuncIds initialized by init():
	private static List<FunctionalIdentifier> datasetFuncIds;

	// selected dataset: a part of the whole dataset
	// -----------------------------------------------
	// - subset of timeseries all in WHOLE_DATASET_NAME
	private static final String SELECTED_DATASET_NAME = "subDataset_JoinTableWithTsTest";
	private final static List<String> SELECTED_DATASET_FLIGHT_IDS = Arrays.asList("1", "2", "50", "51");
	private final static List<String> SELECTED_DATASET_METRICS = Arrays.asList("WS1", "WS2", "WS3", "SLOPE");

	// selectedFuncIds initialized by init()
	// ---------------------------------------
	private static List<FunctionalIdentifier> selectedFuncIds;

	// selected table: input of the service
	// --------------------------------------
	// - some IDS belongs to selected dataset: 1, 50, 51
	// - some IDS are not in selected dataset: 10, 11
	private static final List<String> TABLE_FLIGHT_IDS = Arrays.asList("1", "10", "11", "50", "51");

	private static final String OUTPUT_TABLE_NAME = "OutputName_JoinTableWithTs_Junit";

	private static MetaDataManager metaManager = null;
	private static DataSetManager dataSetManager = null;
	private static TableManager tableManager = null;
	private static ProcessDataManager processDataManager = null;

	// selected metrics
	// -----------------
	//
	private static String SELECTED_METRICS_ALL_MATCHING = "WS1;WS3";

	/**
	 * Method executed before all the test cases: prepare the data saved in databases: Timeseries, Metadata, Dataset.
	 * 
	 * @throws IkatsDaoException
	 */
	@BeforeClass
	public static void init() throws IkatsDaoException {

		metaManager = new MetaDataManager();
		dataSetManager = new DataSetManager();
		tableManager = new TableManager();
		processDataManager = new ProcessDataManager();

		datasetFuncIds = new ArrayList<>();
		datasetMetadata = new ArrayList<>();
		selectedFuncIds = new ArrayList<>();

		// for each metric and each flightId:
		// - create the TS + Metatdata + FunctionalIdentifier
		// - and keep the created FunctionalIdentifier lists for dataset creation:
		// - the whole list of FunctionalIdentifier is datasetFuncIds
		// - the subset of FunctionalIdentifier is selectedFuncIds
		for (String metric : ALL_METRICS) {
			for (String flightId : ALL_FLIGHT_IDS) {

				String tsuid = "tsuid_" + flightId + "_" + metric;
				String funcid = "funcId_" + flightId + "_" + metric;

				FunctionalIdentifier createdFuncId = new FunctionalIdentifier(tsuid, funcid);

				metaManager.persistFunctionalIdentifier(tsuid, funcid);

				// Create required metadata
				datasetMetadata.add(metaManager.persistMetaData(tsuid, "metric", metric));
				datasetMetadata.add(metaManager.persistMetaData(tsuid, "flightId", flightId));

				datasetFuncIds.add(createdFuncId);
				if (SELECTED_DATASET_FLIGHT_IDS.contains(flightId) && SELECTED_DATASET_METRICS.contains(metric)) {
					selectedFuncIds.add(createdFuncId);
				}
			}
		}

		// Translate the FunctionalIdentifier collection into list of tsuids required for dataset creation
		// (using Stream API from java8)
		List<String> tsuids = datasetFuncIds.stream().map(FunctionalIdentifier::getTsuid).collect(Collectors.toList());
		dataSetManager.persistDataSet(WHOLE_DATASET_NAME, "JUnit dataset", tsuids);
		List<String> selectedTsuids = selectedFuncIds.stream().map(FunctionalIdentifier::getTsuid)
				.collect(Collectors.toList());
		dataSetManager.persistDataSet(SELECTED_DATASET_NAME, "JUnit dataset", selectedTsuids);

	}

	/**
	 * Integration Test on apply(): when the user selects an ID not as first column (i.e. row header), and when a target
	 * name is chosen.
	 * 
	 * This test is complementary to the test testComputeTableNominalWithTarget: - testApplyNominal tests that the
	 * computed table is correctly saved in database, not testing all details. - testComputeTableNominalWithTarget is
	 * checking produced Table content and testing details.
	 * 
	 * The other tests will focus to computeTable() step of apply()
	 * @throws Exception 
	 */
	@Test
	public void testApplyNominal() throws Exception {

		try {
			start("testApplyNominal", true);

			// Prepare a row header with unique keys, different from those from
			// theIdColumnName
			// (this should not perturb the result !)
			List<String> anotherIdList = new ArrayList<>(TABLE_FLIGHT_IDS);
			Collections.shuffle(anotherIdList);
			String theIdColumnName = "ID";
			String testedInputJoinColName = "ID";
			String testedInputJoinMetaName = "flightId";
			String theTargetColumnName = "Target";
			String testedMetrics = SELECTED_METRICS_ALL_MATCHING;

			Table selectedTable = tableManager.initEmptyTable(true, true);

			selectedTable.getColumnsHeader().addItems(theIdColumnName, "One", "Two", theTargetColumnName);
			selectedTable.getRowsHeader().addItem(null).addItems(TABLE_FLIGHT_IDS.toArray());

			selectedTable.appendRow(Arrays.asList(1.1, true, "A"));
			selectedTable.appendRow(Arrays.asList(1.2, false, "A"));
			selectedTable.appendRow(Arrays.asList(133, false, "A"));
			selectedTable.appendRow(Arrays.asList(40, true, "B"));
			selectedTable.appendRow(Arrays.asList(500, true, "C"));

			selectedTable.checkConsistency();

			String selectedJson = tableManager.serializeToJson(selectedTable.getTableInfo());

			JoinTableWithTs testedOperator = new JoinTableWithTs();
			String resId = testedOperator.apply(selectedJson, testedMetrics, SELECTED_DATASET_NAME,
					testedInputJoinColName, testedInputJoinMetaName, theTargetColumnName, OUTPUT_TABLE_NAME);

			// Temporary DAO for Table:
			// DB-table processdata
			// - name => name of the Table
			// - id => rid
			// - desc => desc of table for quick searches
			Integer intRid = -1;
			try {
				intRid = Integer.parseInt(resId);
				ProcessData writtenData = processDataManager.getProcessPieceOfData(intRid);

				assertEquals(writtenData.getProcessId(), OUTPUT_TABLE_NAME);

				TableInfo tableJson = tableManager.readFromDatabase(OUTPUT_TABLE_NAME);
				Table testedOutput = tableManager.initTable(tableJson, false);

				assertEquals(3, testedOutput.getIndexColumnHeader("WS1"));
				assertEquals(4, testedOutput.getIndexColumnHeader("WS3"));
				assertEquals(5, testedOutput.getIndexColumnHeader(theTargetColumnName));

				// selected dataset contains IDs= 1, 2, 50, 51
				// selected table contains IDs= 1, 10, 11, 50, 51
				// => join IDs = 1, 50, 51
				//
				assertEquals(Arrays.asList("funcId_1_WS1", null, null, "funcId_50_WS1", "funcId_51_WS1"),
						testedOutput.getColumn("WS1", Object.class));

				// full test on the links applied in anothor use case: testComputeTableNominalWithTarget

				endNominal("testApplyNominal");

			} catch (NumberFormatException e) {
				fail("testApplyNominal: unexpected rid: should be a parsable int: rid=" + resId);
			}
		} finally {
			processDataManager.removeProcessData(OUTPUT_TABLE_NAME);
		}
	}

	/**
	 * Tests the JoinTableWithTs operator when user as defined a target
	 * @throws Exception 
	 */
	@Test
	public void testComputeTableNominalWithTarget() throws Exception {

		// Lists of different configurations leading to same result:
		// - each list proposes different values of one parameter, in
		// respective order of tested configurations
		//
		List<List<String>> configsTest = new ArrayList<>();
		// One test config defines:
		// <column ID in prepared table> | <input joinVColName> | <input
		// joinMetaName> | <input TargetColName>
		// ---------------------------------------------------------------------------------------------------
		configsTest.add(Arrays.asList("ID", "ID", "flightId", "Target"));
		configsTest.add(Arrays.asList("ID", "", "flightId", "Target"));
		configsTest.add(Arrays.asList("flightId", "", "", "Target"));
		configsTest.add(Arrays.asList("flightId", "flightId", "", "Target"));

		start("testComputeTableNominalWithTarget", true);

		for (List<String> config : configsTest) {
			getLogger().info("--> testing config= " + config);
			String theIdColumnName = config.get(0);
			String theTargetColumnName = config.get(3);

			String testedInputJoinColName = config.get(1);
			String testedInputJoinMetaName = config.get(2);

			Table selectedTable = tableManager.initEmptyTable(true, true);

			selectedTable.getColumnsHeader().addItems(theIdColumnName, "One", "Two", theTargetColumnName);
			selectedTable.getRowsHeader().addItem(null).addItems(TABLE_FLIGHT_IDS.toArray());

			selectedTable.appendRow(Arrays.asList(1.1, true, "A"));
			selectedTable.appendRow(Arrays.asList(1.2, false, "A"));
			selectedTable.appendRow(Arrays.asList(133, false, "A"));
			selectedTable.appendRow(Arrays.asList(40, true, "B"));
			selectedTable.appendRow(Arrays.asList(500, true, "C"));

			selectedTable.checkConsistency();

			String selectedJson = tableManager.serializeToJson(selectedTable.getTableInfo());

			JoinTableWithTs testedOperator = new JoinTableWithTs();
			Table computedTable = testedOperator.computeTable(selectedJson, SELECTED_METRICS_ALL_MATCHING,
					SELECTED_DATASET_NAME, testedInputJoinColName, testedInputJoinMetaName, theTargetColumnName,
					"TestedOutput");

			assertEquals(3, computedTable.getIndexColumnHeader("WS1"));
			assertEquals(4, computedTable.getIndexColumnHeader("WS3"));
			assertEquals(5, computedTable.getIndexColumnHeader(theTargetColumnName));

			// selected dataset contains IDs= 1, 2, 50, 51
			// selected table contains IDs= 1, 10, 11, 50, 51
			// => join IDs = 1, 50, 51
			//
			// Note:
			assertEquals(Arrays.asList("funcId_1_WS1", null, null, "funcId_50_WS1", "funcId_51_WS1"),
					computedTable.getColumn("WS1", Object.class));

			List<TableElement> fetchedWS1 = computedTable.getColumn("WS1", TableElement.class);

			// quickly inspect links ...
			// - expected: <link to tsuid_1_WS1>, null, null, <link to
			// tsuid_50_WS1>, <link to tsuid_51_WS1>
			assertTrue(fetchedWS1.get(1).link == null);
			assertTrue(fetchedWS1.get(2).link == null);
			List<DataLink> links = fetchedWS1.stream().map(p -> p.link).filter(p -> p != null)
					.collect(Collectors.toList());

			assertTrue(links.size() == 3);

			assertLinkPointsToTsuid(links.get(0), "funcId_1_WS1", "tsuid_1_WS1");
			assertLinkPointsToTsuid(links.get(1), "funcId_50_WS1", "tsuid_50_WS1");
			assertLinkPointsToTsuid(links.get(2), "funcId_51_WS1", "tsuid_51_WS1");

			for (DataLink dataLink : links) {
				getLogger().info(MessageFormat.format(" - WS1 link={0}", dataLink));
			}

		}

		endNominal("testComputeTableNominalWithTarget");
	}

	/**
	 * Tests the JoinTableWithTs operator when user does not specify a target, and for a table whose join ID column is
	 * NOT the first one !
	 * @throws Exception 
	 */
	@Test
	public void testComputeTableNominalWithoutTarget() throws Exception {

		// Lists of different configurations leading to same result:
		// - each list proposes different values of one parameter, in
		// respective order of tested configurations
		//
		List<String> currentConfig = null;
		List<List<String>> configsTest = new ArrayList<>();
		// One test config defines:
		// <column ID in prepared table> | <input joinVColName> | <input joinMetaName> | <input TargetColName>
		// ---------------------------------------------------------------------------------------------------
		configsTest.add(Arrays.asList("ID", "ID", "flightId", ""));
		configsTest.add(Arrays.asList("flightId", "flightId", "", ""));
		// not tested here because the ID column is not the first one with this test:
		// configsTest.add(Arrays.asList("ID", "", "flightId", ""));
		// not tested here because the ID column is not the first one with this test:
		// configsTest.add(Arrays.asList("flightId", "", "", ""));

		start("testComputeTableNominalWithoutTarget", true);

		// Prepare a row header with unique keys, different from those from
		// theIdColumnName
		// (this should not perturb the result !)
		List<String> anotherIdList = new ArrayList<>(TABLE_FLIGHT_IDS);
		Collections.shuffle(anotherIdList);

		for (List<String> config : configsTest) {
			currentConfig = config;
			getLogger().info("--> testing config= " + currentConfig);
			String theIdColumnName = config.get(0);
			String testedInputJoinColName = config.get(1);
			String testedInputJoinMetaName = config.get(2);
			String theTargetColumnName = config.get(3);

			Table selectedTable = tableManager.initEmptyTable(true, true);

			selectedTable.getColumnsHeader().addItems("AnotherId", "IgnoredTarget", theIdColumnName, "One", "Two");

			selectedTable.getRowsHeader().addItem(null).addItems(anotherIdList.toArray());

			selectedTable.appendRow(Arrays.asList("A", TABLE_FLIGHT_IDS.get(0), 1.1, true));
			selectedTable.appendRow(Arrays.asList("A", TABLE_FLIGHT_IDS.get(1), 1.2, false));
			selectedTable.appendRow(Arrays.asList("A", TABLE_FLIGHT_IDS.get(2), 133, false));
			selectedTable.appendRow(Arrays.asList("B", TABLE_FLIGHT_IDS.get(3), 40, true));
			selectedTable.appendRow(Arrays.asList("C", TABLE_FLIGHT_IDS.get(4), 500, true));

			selectedTable.checkConsistency();

			String selectedJson = tableManager.serializeToJson(selectedTable.getTableInfo());

			JoinTableWithTs testedOperator = new JoinTableWithTs();
			Table computedTable = testedOperator.computeTable(selectedJson, SELECTED_METRICS_ALL_MATCHING,
					SELECTED_DATASET_NAME, testedInputJoinColName, testedInputJoinMetaName, theTargetColumnName,
					"TestedOutput");

			assertEquals(0, computedTable.getIndexColumnHeader("AnotherId"));
			assertEquals(1, computedTable.getIndexColumnHeader("IgnoredTarget"));
			assertEquals(2, computedTable.getIndexColumnHeader(theIdColumnName));
			assertEquals(3, computedTable.getIndexColumnHeader("One"));
			assertEquals(4, computedTable.getIndexColumnHeader("Two"));
			assertEquals(5, computedTable.getIndexColumnHeader("WS1"));
			assertEquals(6, computedTable.getIndexColumnHeader("WS3"));

			// selected dataset contains IDs= 1, 2, 50, 51
			// selected table contains IDs= 1, 10, 11, 50, 51
			// => join IDs = 1, 50, 51
			//
			// Note:
			assertEquals(Arrays.asList("funcId_1_WS1", null, null, "funcId_50_WS1", "funcId_51_WS1"),
					computedTable.getColumn("WS1", Object.class));

			List<TableElement> fetchedWS1 = computedTable.getColumn("WS1", TableElement.class);

			// quickly inspect links ...
			// - expected: <link to tsuid_1_WS1>, null, null, <link to
			// tsuid_50_WS1>, <link to tsuid_51_WS1>
			assertTrue(fetchedWS1.get(1).link == null);
			assertTrue(fetchedWS1.get(2).link == null);
			List<DataLink> links = fetchedWS1.stream().map(p -> p.link).filter(p -> p != null)
					.collect(Collectors.toList());

			assertTrue(links.size() == 3);

			assertLinkPointsToTsuid(links.get(0), "funcId_1_WS1", "tsuid_1_WS1");
			assertLinkPointsToTsuid(links.get(1), "funcId_50_WS1", "tsuid_50_WS1");
			assertLinkPointsToTsuid(links.get(2), "funcId_51_WS1", "tsuid_51_WS1");
		}

		endNominal("testComputeTableNominalWithoutTarget");
	}

	/**
	 * Tests the join when selected metrics are not in the selected dataset: thejoined table is created added metric
	 * columns with null values
	 * @throws Exception 
	 */
	@Test
	public void testComputeTableNoMetrics() throws Exception {

		// One test config defines:
		// <column ID in prepared table> | <input joinVColName> | <input
		// joinMetaName> | <input TargetColName>
		// ---------------------------------------------------------------------------------------------------

		try {
			start("testComputeTableNoMetrics", true);

			String theIdColumnName = "ID";
			String theTargetColumnName = "Target";

			String testedInputJoinColName = "ID";
			String testedInputJoinMetaName = "flightId";

			Table selectedTable = tableManager.initEmptyTable(true, true);

			selectedTable.getColumnsHeader().addItems(theIdColumnName, "One", "Two", theTargetColumnName);
			selectedTable.getRowsHeader().addItem(null).addItems(TABLE_FLIGHT_IDS.toArray());

			selectedTable.appendRow(Arrays.asList(1.1, true, "A"));
			selectedTable.appendRow(Arrays.asList(1.2, false, "A"));
			selectedTable.appendRow(Arrays.asList(133, false, "A"));
			selectedTable.appendRow(Arrays.asList(40, true, "B"));
			selectedTable.appendRow(Arrays.asList(500, true, "C"));

			selectedTable.checkConsistency();

			String selectedJson = tableManager.serializeToJson(selectedTable.getTableInfo());

			JoinTableWithTs testedOperator = new JoinTableWithTs();
			testedOperator.computeTable(selectedJson, "X;Y", SELECTED_DATASET_NAME, testedInputJoinColName,
					testedInputJoinMetaName, theTargetColumnName, "TestedOutput");

			fail("JoinTableWithTs::computeTable() should throw a ResourceNotFoundException");
		} catch (ResourceNotFoundException e) {
			assertTrue(e.getCause() != null);
			assertTrue(e.getCause() instanceof ResourceNotFoundException);
			assertTrue(e.getCause().toString()
					.indexOf(JoinTableWithTs.MSG_ERROR_SELECTED_DATASET_WITHOUT_SELECTED_METRICS) >= 0);
			endOkDegraded("testComputeTableNoMetrics", e);

		}
	}

	/**
	 * Tests the join when one part of the selected metrics are not in the selected dataset: the joined table is created
	 * with one new column for each metric in the dataset.
	 * @throws Exception 
	 */
	@Test
	public void testComputeTablePartialMetrics() throws Exception {

		// One test config defines:
		// <column ID in prepared table> | <input joinVColName> | <input
		// joinMetaName> | <input TargetColName>
		// ---------------------------------------------------------------------------------------------------

		start("testComputeTablePartialMetrics", true);

		String theIdColumnName = "ID";
		String theTargetColumnName = "Target";

		String testedInputJoinColName = "ID";
		String testedInputJoinMetaName = "flightId";

		Table selectedTable = tableManager.initEmptyTable(true, true);

		selectedTable.getColumnsHeader().addItems(theIdColumnName, "One", "Two", theTargetColumnName);
		selectedTable.getRowsHeader().addItem(null).addItems(TABLE_FLIGHT_IDS.toArray());

		selectedTable.appendRow(Arrays.asList(1.1, true, "A"));
		selectedTable.appendRow(Arrays.asList(1.2, false, "A"));
		selectedTable.appendRow(Arrays.asList(133, false, "A"));
		selectedTable.appendRow(Arrays.asList(40, true, "B"));
		selectedTable.appendRow(Arrays.asList(500, true, "C"));

		selectedTable.checkConsistency();

		String selectedJson = tableManager.serializeToJson(selectedTable.getTableInfo());

		JoinTableWithTs testedOperator = new JoinTableWithTs();
		Table computedTable = testedOperator.computeTable(selectedJson, "WS2;X;WS1", SELECTED_DATASET_NAME,
				testedInputJoinColName, testedInputJoinMetaName, theTargetColumnName, "TestedOutput");

		assertEquals(-1, computedTable.getIndexColumnHeader("X"));
		assertEquals(3, computedTable.getIndexColumnHeader("WS1"));
		assertEquals(4, computedTable.getIndexColumnHeader("WS2"));
		assertEquals(5, computedTable.getIndexColumnHeader(theTargetColumnName));

		// selected dataset contains IDs= 1, 2, 50, 51
		// selected table contains IDs= 1, 10, 11, 50, 51
		// => join IDs = 1, 50, 51
		//
		assertEquals(Arrays.asList("funcId_1_WS1", null, null, "funcId_50_WS1", "funcId_51_WS1"),
				computedTable.getColumn("WS1", Object.class));

		endNominal("testComputeTablePartialMetrics");

	}

	/**
	 * Tests that specified DataLink points to specified TS, using a list of FunctionalIdentifier compliant with ts_list
	 * json type.
	 * 
	 * @param dataLink
	 * @param funcId
	 * @param tsuid
	 */
	private void assertLinkPointsToTsuid(DataLink dataLink, String funcId, String tsuid) {

		assertTrue(dataLink != null);
		assertTrue(dataLink.val != null);
		assertTrue(dataLink.val instanceof List<?>);

		assertTrue(((List<?>) dataLink.val).size() == 1);
		FunctionalIdentifier val = null;
		try {
			val = ((List<FunctionalIdentifier>) dataLink.val).get(0);
		} catch (ClassCastException e) {
			fail("ClassCastException in assertLinkPointsToTsuid: " + e.getMessage());
		}
		assertTrue(val != null);
		assertTrue(funcId.equals(val.getFuncId()));
		assertTrue(tsuid.equals(val.getTsuid()));

	}

	// Review#158227 FTL on a structuré nos test pour utiliser une base en mémoire, pas besoin de nettoyage, code
	// inutile.
	// Review#158227 MBD je pense que pour permettre un enchainement de differents JUnit tests c'est plus propre et prudent 
	// de nettoyer les données afin d'eviter des confits de creation. Je propose de conserver cette methode. Dans le passé
	// on a deja eu des soucis de conflits en TU.
	/**
	 * Ends the junit class: clean the data prepared for the test in the database
	 * @throws Exception 
	 */
	@AfterClass
	public static void end() throws Exception {

		dataSetManager.removeDataSet(WHOLE_DATASET_NAME, false);
		dataSetManager.removeDataSet(SELECTED_DATASET_NAME, false);

		for (FunctionalIdentifier funcId : datasetFuncIds) {
			metaManager.deleteFunctionalIdentifier(funcId.getTsuid());
			metaManager.deleteMetaData(funcId.getTsuid());
		}

		// will be required: delete created tables !
		dataSetManager = null;
		metaManager = null;
		processDataManager = null;
	}

}
