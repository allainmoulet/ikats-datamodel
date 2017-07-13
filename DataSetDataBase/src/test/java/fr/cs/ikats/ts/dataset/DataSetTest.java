package fr.cs.ikats.ts.dataset;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import fr.cs.ikats.common.junit.CommonTest;
import fr.cs.ikats.metadata.MetaDataFacade;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.ts.dataset.model.DataSet;
import fr.cs.ikats.ts.dataset.model.LinkDatasetTimeSeries;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Test class for DataSet Facade
 */
public class DataSetTest extends CommonTest {

    static private DataSetFacade datasetFacade = null;
    static private MetaDataFacade metadataFacade = null;

    /**
     * Test method for
     * {@link fr.cs.ikats.ts.dataset.DataSetFacade#persistDataSet(java.lang.String, java.lang.String, java.util.List)}
     * .
     */
    @Test
    public void testPersistDataSetWithEntities() {

        String testCaseName = "testPersistDataSetWithEntities";
        boolean isNominal = true;

        try {

            start(testCaseName, isNominal);
            DataSetFacade facade = getDatasetFacade();
            String datasetNameTested = "dataSet1_" + testCaseName;

            HashMap<String, String> mapTsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "MAM", "toto", "test"}, testCaseName);

            List<String> tsuids = new ArrayList<String>(mapTsuidToFuncId.keySet());

            // pre-condition : tsuids exist with functional ids
            ArrayList<FunctionalIdentifier> fidEntities = saveFuncIds(mapTsuidToFuncId, tsuids);

            String results = facade.persistDataSetFromEntity(datasetNameTested, "Description courte du dataset cree depuis les entites", fidEntities);

            // String results = facade.persistDataSet( "dataSet1", "Description
            // courte du dataset", tsuids );

            assertEquals(datasetNameTested, results);

            DataSet dataSet = facade.getDataSet(datasetNameTested);
            getLogger().info(dataSet.toDetailedString(false));
            endNominal(testCaseName);
        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }

    }

    /**
     * Test error when tsuids are unknown: not saved in tsfunctionalidentifier
     * table
     */
    @Test
    public void testPersistDataSetWithEntities_DG() {

        String testCaseName = "testPersistDataSetWithEntities_DG";
        boolean isNominal = false;

        try {

            start(testCaseName, isNominal);
            DataSetFacade facade = getDatasetFacade();
            String datasetNameTested = "dataSet1_" + testCaseName;

            HashMap<String, String> mapTsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "MAM", "toto", "test"}, testCaseName);

            List<String> tsuids = new ArrayList<String>(mapTsuidToFuncId.keySet());

            // pre-condition : tsuids exist with functional ids
            // => not saved here !!!
            ArrayList<FunctionalIdentifier> fidEntities = new ArrayList<FunctionalIdentifier>(); // saveFuncIds(mapTsuidToFuncId,
            // tsuids);
            for (String tsuid : mapTsuidToFuncId.keySet()) {
                fidEntities.add(new FunctionalIdentifier(tsuid, mapTsuidToFuncId.get(tsuid)));
            }
            String results = facade.persistDataSetFromEntity(datasetNameTested, "Description courte du dataset cree depuis les entites", fidEntities);
            throw new Exception("Failed: expected error is not raised: unreferenced tsuid in tsfunctionalidentifier");
        }
        catch (IkatsDaoException e) {
            endOkDegraded(testCaseName, e);
        }
        catch (Throwable te) {
            endWithFailure(testCaseName, te);
        }
    }

    /**
     * Test method for
     * {@link fr.cs.ikats.ts.dataset.DataSetFacade#persistDataSet(java.lang.String, java.lang.String, java.util.List)}
     * .
     */
    @Test
    public void testPersistDataSet() {

        String testCaseName = "testPersistDataSet";
        boolean isNominal = true;

        try {
            start(testCaseName, isNominal);
            DataSetFacade facade = getDatasetFacade();
            String datasetNameTested = "dataSet1_" + testCaseName;

            HashMap<String, String> mapTsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "MAM", "toto", "test"}, testCaseName);

            List<String> tsuids = new ArrayList<String>(mapTsuidToFuncId.keySet());

            // pre-condition : tsuids exist with functional ids
            ArrayList<FunctionalIdentifier> fidEntities = saveFuncIds(mapTsuidToFuncId, tsuids);


            String results = facade.persistDataSet(datasetNameTested, "Description courte du dataset cree depuis des string tsuid", tsuids);

            assertEquals(datasetNameTested, results);

            DataSet dataSet = facade.getDataSet(datasetNameTested);

            getLogger().info(dataSet.toDetailedString(false));

            endNominal(testCaseName);
        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }

    }

    /**
     * Test error when tsuids are unknown: not saved in tsfunctionalidentifier
     * table
     */
    @Test
    public void testPersistDataSet_DG() {

        String testCaseName = "testPersistDataSet_DG";
        boolean isNominal = false;

        try {
            start(testCaseName, isNominal);
            DataSetFacade facade = getDatasetFacade();
            String datasetNameTested = "dataSet1_" + testCaseName;

            HashMap<String, String> mapTsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "MAM", "toto", "test"}, testCaseName);

            List<String> tsuids = new ArrayList<String>(mapTsuidToFuncId.keySet());

            // pre-condition : tsuids exist with functional ids
            // ArrayList<FunctionalIdentifier> fidEntities =
            // saveFuncIds(mapTsuidToFuncId, tsuids);


            String results = facade.persistDataSet(datasetNameTested, "Description courte du dataset cree depuis des string tsuid", tsuids);

            throw new Exception("Failed: expected error is not raised: unreferenced tsuid in tsfunctionalidentifier");
        }
        catch (IkatsDaoException e) {
            endNominal(testCaseName);
        }
        catch (Throwable te) {
            endWithFailure(testCaseName, te);
        }

    }

    @Test
    public void testPersistExistingDataSet_DG() {

        String testCaseName = "testPersistExistingDataSet_DG";
        boolean isNominal = true;

        try {
            start(testCaseName, isNominal);

            DataSetFacade facade = getDatasetFacade();

            HashMap<String, String> mapTsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "MAM", "toto"}, testCaseName);

            HashMap<String, String> map2TsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid2", "hello", "toto", "titi", "bye"}, testCaseName);

            List<String> tsuids = new ArrayList<String>(mapTsuidToFuncId.keySet());
            List<String> tsuids2 = new ArrayList<String>(map2TsuidToFuncId.keySet());

            // pre-condition : tsuids exist with functional ids
            ArrayList<FunctionalIdentifier> fidEntities = saveFuncIds(mapTsuidToFuncId, tsuids);
            ArrayList<FunctionalIdentifier> fidEntities2 = saveFuncIds(map2TsuidToFuncId, tsuids2);

            String dsname = "dataSet";
            String description = "Description courte du dataset";
            String results = facade.persistDataSet(dsname, description, tsuids);
            assertEquals(dsname, results);
            DataSet datasetToUpdate = facade.getDataSet(dsname);

            assertEquals(3, datasetToUpdate.getLinksToTimeSeries().size());
            assertEquals(description, datasetToUpdate.getDescription());

            // Test 1 : try to persist existing dataset !!!!
            facade.persistDataSet(dsname, null, tsuids2);

            throw new Exception("Failed test: expected a IkatsDaoConflict error: trying to create a dataset already in DB");
        }
        catch (IkatsDaoConflictException de) {
            endOkDegraded(testCaseName, de);
        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }

    }

    /**
     * Test method for
     * {@link fr.cs.ikats.ts.dataset.DataSetFacade#updateDataSet(java.lang.String, java.lang.String, java.util.List)}
     * .
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testUpdateDataSetInModeAppend() {

        String testCaseName = "testUpdateDataSetInModeAppend";
        boolean isNominal = true;

        try {
            start(testCaseName, isNominal);

            DataSetFacade facade = getDatasetFacade();

            HashMap<String, String> mapTsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "MAM", "toto"}, testCaseName);

            List<String> tsuids = new ArrayList<String>(mapTsuidToFuncId.keySet());

            // pre-condition : tsuids exist with functional ids
            ArrayList<FunctionalIdentifier> fidEntities = saveFuncIds(mapTsuidToFuncId, tsuids);

            String dsname = encodeDatasetName("dataSet_a_updater", testCaseName);
            String description = "Description courte du dataset";
            String results = facade.persistDataSet(dsname, description, tsuids);
            assertEquals(dsname, results);
            DataSet datasetToUpdate = facade.getDataSet(dsname);

            assertEquals(3, datasetToUpdate.getLinksToTimeSeries().size());
            assertEquals(description, datasetToUpdate.getDescription());

            tsuids = new ArrayList<String>();
            String addedTsuid2 = "tsuid2" + "_" + testCaseName;
            tsuids.add(addedTsuid2);
            FunctionalIdentifier prerequired = saveFuncId(addedTsuid2);

            // Test 1 : do not change description + append one tsuid
            facade.updateInAppendMode(dsname, null, tsuids);

            DataSet updatedDataset = facade.getDataSet(dsname);
            assertEquals(4, updatedDataset.getLinksToTimeSeries().size());
            assertEquals(description, updatedDataset.getDescription());
            assertEquals(true, updatedDataset.getTsuidsAsString().contains(addedTsuid2));

            // Test 2 : change description + append one tsuid
            tsuids = new ArrayList<String>();
            String updatedDescription = "This is the new description";
            String addedTsuid3 = "tsuid3" + "_" + testCaseName;
            tsuids.add(addedTsuid3);
            prerequired = saveFuncId(addedTsuid3);

            facade.updateInAppendMode(dsname, updatedDescription, tsuids);

            updatedDataset = facade.getDataSet(dsname);
            assertEquals(5, updatedDataset.getLinksToTimeSeries().size());
            assertEquals(updatedDescription, updatedDataset.getDescription());
            assertEquals(true, updatedDataset.getTsuidsAsString().contains(addedTsuid2));
            assertEquals(true, updatedDataset.getTsuidsAsString().contains(addedTsuid3));

            // facade.removeDataSet(dsname);

            endNominal(testCaseName);

        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }

    }

    /**
     * DG Test method for
     * {@link fr.cs.ikats.ts.dataset.DataSetFacade#updateDataSet(java.lang.String, java.lang.String, java.util.List)}
     * .
     */
    @Test
    public void testUpdateDataSet() {

        String testCaseName = "testUpdateDataSet";
        boolean isNominal = true;

        try {
            start(testCaseName, isNominal);

            DataSetFacade facade = getDatasetFacade();

            HashMap<String, String> mapTsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "MAM", "toto"}, testCaseName);

            HashMap<String, String> map2TsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid2", "hello", "toto", "titi", "bye"}, testCaseName);

            HashMap<String, String> map3TsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid3", "hello2", "toto", "titi"}, testCaseName);

            List<String> tsuids = new ArrayList<String>(mapTsuidToFuncId.keySet());
            List<String> tsuids2 = new ArrayList<String>(map2TsuidToFuncId.keySet());
            List<String> tsuids3 = new ArrayList<String>(map3TsuidToFuncId.keySet());

            // pre-condition : tsuids exist with functional ids
            ArrayList<FunctionalIdentifier> fidEntities = saveFuncIds(mapTsuidToFuncId, tsuids);
            ArrayList<FunctionalIdentifier> fidEntities2 = saveFuncIds(map2TsuidToFuncId, tsuids2);
            ArrayList<FunctionalIdentifier> fidEntities3 = saveFuncIds(map2TsuidToFuncId, tsuids3);

            String dsname = encodeDatasetName("dataSet_a_updater", testCaseName);
            String description = "Description courte du dataset";
            String results = facade.persistDataSet(dsname, description, tsuids);
            assertEquals(dsname, results);
            DataSet datasetToUpdate = facade.getDataSet(dsname);

            assertEquals(3, datasetToUpdate.getLinksToTimeSeries().size());
            assertEquals(description, datasetToUpdate.getDescription());

            // Test1: do not change description + append one tsuid
            facade.updateDataSet(dsname, null, tsuids2);

            DataSet updatedDataset = facade.getDataSet(dsname);
            getLogger().info(updatedDataset.toDetailedString(false));
            assertEquals("Test1: do not change description + append one tsuid", 5, updatedDataset.getLinksToTimeSeries().size());
            assertEquals("Test1: do not change description + append one tsuid", description, updatedDataset.getDescription());
            assertEquals("Test1: do not change description + append one tsuid", tsuids2, updatedDataset.getTsuidsAsString());

            // Test2: change description + unchanged tsuids
            String description2 = "Desc two";
            facade.updateDataSet(dsname, description2, tsuids2);

            DataSet updatedDataset2 = facade.getDataSet(dsname);
            getLogger().info(updatedDataset.toDetailedString(false));
            assertEquals("Test2: change description + unchanged tsuids", 5, updatedDataset2.getLinksToTimeSeries().size());
            assertEquals("Test2: change description + unchanged tsuids", description2, updatedDataset2.getDescription());
            assertEquals("Test2: change description + unchanged tsuids", tsuids2, updatedDataset2.getTsuidsAsString());

            facade.removeDataSet(dsname);

            endNominal(testCaseName);

        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }

    }

    @Test
    public void testRemoveTsFromDataSet() {

        String testCaseName = "testRemoveTsFromDataSet";
        boolean isNominal = true;

        try {
            start(testCaseName, isNominal);

            DataSetFacade facade = getDatasetFacade();

            HashMap<String, String> map2TsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid2", "hello", "toto", "titi", "bye"}, testCaseName);

            HashMap<String, String> map3TsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid3", "hello2", "toto", "titi"}, testCaseName);

            List<String> tsuids2 = new ArrayList<String>(map2TsuidToFuncId.keySet());
            List<String> tsuids3 = new ArrayList<String>(map3TsuidToFuncId.keySet());

            // pre-condition : tsuids exist with functional ids
            ArrayList<FunctionalIdentifier> fidEntities2 = saveFuncIds(map2TsuidToFuncId, tsuids2);
            ArrayList<FunctionalIdentifier> fidEntities3 = saveFuncIds(map2TsuidToFuncId, tsuids3);

            String dsname = encodeDatasetName("dataset_with_removed_ts", testCaseName);
            String description = "Description courte du dataset";
            String results = facade.persistDataSet(dsname, description, tsuids2);
            assertEquals(dsname, results);
            DataSet datasetToUpdate = facade.getDataSet(dsname);

            assertEquals(5, datasetToUpdate.getLinksToTimeSeries().size());
            assertEquals(description, datasetToUpdate.getDescription());

            // Tested service:
            facade.removeTsLinks(dsname, tsuids3);

            DataSet updatedDataset = facade.getDataSet(dsname);
            List<String> updatedTsuidLinks = updatedDataset.getTsuidsAsString();

            getLogger().info(updatedDataset.toDetailedString(false));
            assertEquals(3, updatedDataset.getLinksToTimeSeries().size());

            String msg = "Validate result of removeTsLinks on Dataset";
            assertEquals(msg, true, updatedTsuidLinks.contains(encodeTsuid("tsuid2", testCaseName)));
            assertEquals(msg, true, updatedTsuidLinks.contains(encodeTsuid("hello", testCaseName)));
            assertEquals(msg, true, updatedTsuidLinks.contains(encodeTsuid("bye", testCaseName)));
            assertEquals(msg, false, updatedTsuidLinks.contains(encodeTsuid("toto", testCaseName)));
            assertEquals(msg, false, updatedTsuidLinks.contains(encodeTsuid("titi", testCaseName)));

            facade.removeDataSet(dsname);

            endNominal(testCaseName);

        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }

    }

    /**
     * Test method for
     * {@link fr.cs.ikats.ts.dataset.DataSetFacade#removeDataSet(java.lang.String)}
     * .
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testRemoveDataset() {

        String testCaseName = "testRemoveDataset";
        boolean isNominal = true;

        try {
            start(testCaseName, isNominal);

            DataSetFacade facade = getDatasetFacade();

            HashMap<String, String> testTsuidFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "tsuid2"}, testCaseName);

            List<String> tsuids = new ArrayList<String>(testTsuidFuncId.keySet());

            saveFuncIds(testTsuidFuncId, tsuids);

            String dsname = encodeDatasetName("dataSet3", testCaseName);
            // ------- PRe-clean TU data -----------
            try {
                facade.removeDataSet(dsname);
            }
            catch (IkatsDaoMissingRessource e) {
                // nothing to do: expected behaviour
            }
            catch (Throwable e) {
                throw new Exception("removing unexisting dataSet3", e);
            }

            facade.persistDataSet(dsname, "desc courte 1", tsuids);
            // --------------------------------------

            facade.removeDataSet(dsname);
            try {
                facade.getDataSet(dsname);
            }
            catch (IkatsDaoMissingRessource e) {
                // nothing to do: expected behaviour
            }
            catch (Throwable e) {
                throw new Exception("reading unexisting dataSet3", e);
            }
            try {
                facade.removeDataSet(dsname);
            }
            catch (IkatsDaoMissingRessource e) {
                // nothing to do: expected behaviour
            }
            catch (Throwable e) {
                throw new Exception("removing unexisting dataSet3", e);
            }

            endNominal(testCaseName);

        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }

    }

    /**
     * Test method for
     * {@link fr.cs.ikats.ts.dataset.DataSetFacade#getDataSet(java.lang.String)}
     * .
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testGetDataSet() {

        String testCaseName = "testGetDataSet";
        boolean isNominal = true;

        try {
            start(testCaseName, isNominal);

            DataSetFacade facade = getDatasetFacade();

            HashMap<String, String> testTsuidFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "tsuid2"}, testCaseName);

            List<String> tsuids = new ArrayList<String>(testTsuidFuncId.keySet());

            saveFuncIds(testTsuidFuncId, tsuids);
            String encodedTsuid1 = encodeTsuid("tsuid1", testCaseName);
            String encodedTsuid2 = encodeTsuid("tsuid2", testCaseName);

            String dsname = encodeDatasetName("dataSet5", testCaseName);
            String desc = "desc courte 1";
            facade.persistDataSet(dsname, desc, tsuids);

            DataSet result = facade.getDataSet(dsname);

            getLogger().info(result);

            assertEquals("Check content of get dataset: name", dsname, result.getName());
            assertEquals("Check content of get dataset: desc", desc, result.getDescription());
            assertEquals("Check content of get dataset: length", tsuids.size(), result.getLinksToTimeSeries().size());

            assertEquals("Check content of get dataset: tsuid as string", true, result.getTsuidsAsString().contains(encodedTsuid1));

            assertEquals("Check content of get dataset: tsuid as string", true, result.getTsuidsAsString().contains(encodedTsuid2));

            assertEquals("Check content of get dataset: TimeSeries", true, result.getLinksToTimeSeries().contains(new LinkDatasetTimeSeries(encodedTsuid1, dsname)));
            assertEquals("Check content of get dataset: TimeSeries", true, result.getLinksToTimeSeries().contains(new LinkDatasetTimeSeries(encodedTsuid2, dsname)));
            assertEquals("Check content of get dataset: TimeSeries", false, result.getLinksToTimeSeries().contains(new LinkDatasetTimeSeries("outsideTs", dsname)));
            facade.removeDataSet(dsname);

            endNominal(testCaseName);

        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }

    }

    /**
     * Test method for
     * {@link fr.cs.ikats.ts.dataset.DataSetFacade#getAllDataSetSummary()}.
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testGetSummaryDataSet() {

        String testCaseName = "testGetSummaryDataSet";
        boolean isNominal = true;

        try {
            start(testCaseName, isNominal);
            DataSetFacade facade = getDatasetFacade();

            // ---------- prepare data ... -----------------------------------------
            PreparedTsReferences preparedData = new PreparedTsReferences(new String[]{"tsuid1", "tsuid2"}, testCaseName, true);

            List<DataSet> result;
            try {
                result = facade.getAllDataSetSummary();
                for (DataSet ds : result) {
                    facade.removeDataSet(ds.getName());
                }
            }
            catch (IkatsDaoMissingRessource e) {
                // Ok: no dataset found by facade.getAllDataSetSummary(): BD is cleaned
            }

            facade.persistDataSet(encodeDatasetName("dataSet6", testCaseName), "desc courte 6", preparedData.tsuids);
            facade.persistDataSet(encodeDatasetName("dataSet7", testCaseName), "desc courte 7", preparedData.tsuids);
            facade.persistDataSet(encodeDatasetName("dataSet8", testCaseName), "desc courte 8", preparedData.tsuids);
            facade.persistDataSet(encodeDatasetName("dataSet9", testCaseName), "desc courte 9", preparedData.tsuids);

            // ---------- ... prepare data ----------------------------------------

            result = facade.getAllDataSetSummary();

            for (DataSet dataSet : result) {
                getLogger().info(dataSet);
            }
            assertEquals(4, result.size());
            assertEquals(encodeDatasetName("dataSet6", testCaseName), result.get(0).getName());
            assertEquals("desc courte 6", result.get(0).getDescription());
            assertEquals(encodeDatasetName("dataSet7", testCaseName), result.get(1).getName());
            assertEquals("desc courte 7", result.get(1).getDescription());
            assertEquals(encodeDatasetName("dataSet8", testCaseName), result.get(2).getName());
            assertEquals("desc courte 8", result.get(2).getDescription());
            assertEquals(encodeDatasetName("dataSet9", testCaseName), result.get(3).getName());
            assertEquals("desc courte 9", result.get(3).getDescription());

            DataSet dsToCheck = facade.getDataSet(result.get(0).getName());

            List<String> testedListTsuid =
                    dsToCheck.getLinksToTimeSeries().
                            stream().map(LinkDatasetTimeSeries::getTsuid).
                            collect(Collectors.toList());

            assert (testedListTsuid.containsAll(preparedData.tsuids)
                    && preparedData.tsuids.containsAll(testedListTsuid));

            facade.removeDataSet(encodeDatasetName("dataSet6", testCaseName));
            facade.removeDataSet(encodeDatasetName("dataSet7", testCaseName));
            facade.removeDataSet(encodeDatasetName("dataSet8", testCaseName));
            facade.removeDataSet(encodeDatasetName("dataSet9", testCaseName));

            endNominal(testCaseName);

        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }

    }

    /**
     * Test method for
     * {@link fr.cs.ikats.ts.dataset.DataSetFacade#getDataSetNamesForTsuid(java.lang.String)}
     * .
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testGetDataSetNamesForTsuid() {

        String testCaseName = "testGetDataSetNamesForTsuid";
        boolean isNominal = true;

        try {
            start(testCaseName, isNominal);

            PreparedTsReferences preparedDataRefs_1_2 = new PreparedTsReferences(new String[]{"tsuid1", "tsuid2"},
                                                                                 testCaseName, true);

            PreparedTsReferences preparedDataRefs_3_4 = new PreparedTsReferences(new String[]{"tsuid3", "tsuid4"},
                                                                                 testCaseName, true);


            DataSetFacade facade = getDatasetFacade();
            List<String> tsuids = new ArrayList<String>(preparedDataRefs_1_2.tsuids);
            facade.persistDataSet(encodeDatasetName("dataSet_1", testCaseName), "desc courte 6", tsuids);
            facade.persistDataSet(encodeDatasetName("dataSet_2", testCaseName), "desc courte 7", tsuids);
            facade.persistDataSet(encodeDatasetName("dataSet_3", testCaseName), "desc courte 8", tsuids);
            facade.persistDataSet(encodeDatasetName("dataSet_4", testCaseName), "desc courte 9", tsuids);

            tsuids.addAll(preparedDataRefs_3_4.tsuids);

            facade.persistDataSet(encodeDatasetName("dataSet_5", testCaseName), "desc courte 9", tsuids);

            List<String> result1 = facade.getDataSetNamesForTsuid(encodeTsuid("tsuid1", testCaseName));
            List<String> result2 = facade.getDataSetNamesForTsuid(encodeTsuid("tsuid2", testCaseName));
            List<String> result3 = facade.getDataSetNamesForTsuid(encodeTsuid("tsuid3", testCaseName));
            List<String> result4 = facade.getDataSetNamesForTsuid(encodeTsuid("tsuid4", testCaseName));
            assertEquals(5, result1.size());
            assertEquals(5, result2.size());
            assertEquals(1, result3.size());
            assertEquals(1, result4.size());
            assertEquals(encodeDatasetName("dataSet_1", testCaseName), result1.get(0));
            assertEquals(encodeDatasetName("dataSet_2", testCaseName), result2.get(1));
            assertEquals(encodeDatasetName("dataSet_3", testCaseName), result1.get(2));
            assertEquals(encodeDatasetName("dataSet_4", testCaseName), result1.get(3));
            assertEquals(encodeDatasetName("dataSet_5", testCaseName), result2.get(4));
            assertEquals(encodeDatasetName("dataSet_5", testCaseName), result3.get(0));
            assertEquals(encodeDatasetName("dataSet_5", testCaseName), result4.get(0));

            List<String> result5 = facade.getDataSetNamesForTsuid(encodeTsuid("toto", testCaseName));
            assertEquals("Test unmatched testGetDataSetNamesForTsuid", true, (result5 == null) || (result5.size() == 0));

            facade.removeDataSet(encodeDatasetName("dataSet_1", testCaseName));
            facade.removeDataSet(encodeDatasetName("dataSet_2", testCaseName));
            facade.removeDataSet(encodeDatasetName("dataSet_3", testCaseName));
            facade.removeDataSet(encodeDatasetName("dataSet_4", testCaseName));
            facade.removeDataSet(encodeDatasetName("dataSet_5", testCaseName));

            endNominal(testCaseName);

        }
        catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }

    }

    static protected DataSetFacade getDatasetFacade() {
        if (datasetFacade == null) {
            datasetFacade = new DataSetFacade();
        }
        return datasetFacade;
    }

    static protected MetaDataFacade getMetadataFacade() {
        if (metadataFacade == null) {
            metadataFacade = new MetaDataFacade();
        }
        return metadataFacade;
    }

    protected ArrayList<FunctionalIdentifier> saveFuncIds(HashMap<String, String> mapTsuidToFuncId, List<String> tsuids) {
        MetaDataFacade facadeFuncId = getMetadataFacade();
        facadeFuncId.persistFunctionalIdentifier(mapTsuidToFuncId);
        ArrayList<FunctionalIdentifier> fidEntities = new ArrayList<FunctionalIdentifier>();
        for (String tsuid : tsuids) {

            FunctionalIdentifier functionalIdentifierByTsuid = facadeFuncId.getFunctionalIdentifierByTsuid(tsuid);
            fidEntities.add(functionalIdentifierByTsuid);
            getLogger().info(functionalIdentifierByTsuid);

        }
        return fidEntities;
    }

    protected FunctionalIdentifier saveFuncId(String tsuid) throws IkatsDaoException {

        String funcid = "funcId_" + tsuid;
        MetaDataFacade facadeFuncId = getMetadataFacade();
        facadeFuncId.persistFunctionalIdentifier(tsuid, funcid); // Would be
        // better to
        // have simple
        // DAO service

        return facadeFuncId.getFunctionalIdentifierByTsuid(tsuid);
    }

    /**
     * class for TU with dataset: useful to prepare and retrieve TS references:
     * use the constructor and the the public attributes
     */
    final public class PreparedTsReferences {

        private String testCaseName;
        public HashMap<String, String> mapByTsuidTheFuncId;
        public List<String> tsuids;

        public PreparedTsReferences(String[] tsuidPrefixes, String testCaseName, boolean saveFunctionalIdentifiers) {

            this.testCaseName = testCaseName;
            mapByTsuidTheFuncId = getTestedTsuidFidMap(tsuidPrefixes, testCaseName);
            tsuids = new ArrayList<String>(mapByTsuidTheFuncId.keySet());

            if (saveFunctionalIdentifiers) {
                saveFuncIds(mapByTsuidTheFuncId, tsuids);
            }
        }
    }
}
