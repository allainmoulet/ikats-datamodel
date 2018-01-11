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
 *
 */

package fr.cs.ikats.ts.dataset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingResource;
import fr.cs.ikats.common.junit.CommonTest;
import fr.cs.ikats.metadata.MetaDataFacade;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.ts.dataset.model.DataSet;
import fr.cs.ikats.ts.dataset.model.LinkDatasetTimeSeries;

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
     * @throws IkatsDaoException
     */
    @Test
    public void testPersistDataSetWithEntities() throws IkatsDaoException {

        String testCaseName = "testPersistDataSetWithEntities";

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
    }

    /**
     * Test error when tsuids are unknown: not saved in tsfunctionalidentifier
     * table
     * @throws IkatsDaoException
     */
    @Test(expected = IkatsDaoException.class)
    public void testPersistDataSetWithEntities_DG() throws IkatsDaoException {

        String testCaseName = "testPersistDataSetWithEntities_DG";

        DataSetFacade facade = getDatasetFacade();
        String datasetNameTested = "dataSet1_" + testCaseName;

        HashMap<String, String> mapTsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "MAM", "toto", "test"}, testCaseName);

        // pre-condition : tsuids exist with functional ids
        // => not saved here !!!
        ArrayList<FunctionalIdentifier> fidEntities = new ArrayList<FunctionalIdentifier>(); // saveFuncIds(mapTsuidToFuncId,
        // tsuids);
        for (String tsuid : mapTsuidToFuncId.keySet()) {
            fidEntities.add(new FunctionalIdentifier(tsuid, mapTsuidToFuncId.get(tsuid)));
        }

        // Should throw the IkatsDaoException
        facade.persistDataSetFromEntity(datasetNameTested, "Description courte du dataset cree depuis les entites", fidEntities);
    }

    /**
     * Test method for
     * {@link fr.cs.ikats.ts.dataset.DataSetFacade#persistDataSet(java.lang.String, java.lang.String, java.util.List)}
     * .
     * @throws IkatsDaoException
     */
    @Test
    public void testPersistDataSet() throws IkatsDaoException {

        String testCaseName = "testPersistDataSet";

        DataSetFacade facade = getDatasetFacade();
        String datasetNameTested = "dataSet1_" + testCaseName;

        HashMap<String, String> mapTsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "MAM", "toto", "test"}, testCaseName);

        List<String> tsuids = new ArrayList<String>(mapTsuidToFuncId.keySet());

        // pre-condition : tsuids exist with functional ids
        saveFuncIds(mapTsuidToFuncId, tsuids);

        String results = facade.persistDataSet(datasetNameTested, "Description courte du dataset cree depuis des string tsuid", tsuids);

        assertEquals(datasetNameTested, results);

        facade.removeDataSet(datasetNameTested);

    }

    /**
     * Test error when tsuids are unknown: not saved in tsfunctionalidentifier
     * table
     * @throws IkatsDaoException
     */
    @Test(expected = IkatsDaoException.class)
    public void testPersistDataSet_DG() throws IkatsDaoException {

        String testCaseName = "testPersistDataSet_DG";

        DataSetFacade facade = getDatasetFacade();
        String datasetNameTested = "dataSet1_" + testCaseName;

        HashMap<String, String> mapTsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "MAM", "toto", "test"}, testCaseName);

        List<String> tsuids = new ArrayList<String>(mapTsuidToFuncId.keySet());

        facade.persistDataSet(datasetNameTested, "Description courte du dataset cree depuis des string tsuid", tsuids);
    }

    /**
     * Test error when dataset already exist in database
     *
     * @throws IkatsDaoConflictException
     */
    @Test(expected = IkatsDaoConflictException.class)
    public void testPersistExistingDataSet_DG() throws IkatsDaoException {

        String testCaseName = "testPersistExistingDataSet_DG";

        DataSetFacade facade = getDatasetFacade();

        HashMap<String, String> mapTsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "MAM", "toto"}, testCaseName);

        HashMap<String, String> map2TsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid2", "hello", "toto", "titi", "bye"}, testCaseName);

        List<String> tsuids = new ArrayList<String>(mapTsuidToFuncId.keySet());
        List<String> tsuids2 = new ArrayList<String>(map2TsuidToFuncId.keySet());

        String dsname = "dataSet";
        String description = "Description courte du dataset";
        String results = facade.persistDataSet(dsname, description, tsuids);
        assertEquals(dsname, results);
        DataSet datasetToUpdate = facade.getDataSet(dsname);

        assertEquals(3, datasetToUpdate.getLinksToTimeSeries().size());
        assertEquals(description, datasetToUpdate.getDescription());

        // Test 1 : try to persist existing dataset !!!!
        facade.persistDataSet(dsname, null, tsuids2);
    }

    /**
     * Test method for
     * {@link fr.cs.ikats.ts.dataset.DataSetFacade#updateDataSet(java.lang.String, java.lang.String, java.util.List)}
     * .
     * @throws IkatsDaoMissingResource
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testUpdateDataSetInModeAppend() throws IkatsDaoMissingResource, IkatsDaoException {

        String testCaseName = "testUpdateDataSetInModeAppend";

        DataSetFacade facade = getDatasetFacade();

        HashMap<String, String> mapTsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "MAM", "toto"}, testCaseName);

        List<String> tsuids = new ArrayList<String>(mapTsuidToFuncId.keySet());

        // pre-condition : tsuids exist with functional ids
        saveFuncIds(mapTsuidToFuncId, tsuids);

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
        saveFuncId(addedTsuid2);

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
        saveFuncId(addedTsuid3);

        facade.updateInAppendMode(dsname, updatedDescription, tsuids);

        updatedDataset = facade.getDataSet(dsname);
        assertEquals(5, updatedDataset.getLinksToTimeSeries().size());
        assertEquals(updatedDescription, updatedDataset.getDescription());
        assertEquals(true, updatedDataset.getTsuidsAsString().contains(addedTsuid2));
        assertEquals(true, updatedDataset.getTsuidsAsString().contains(addedTsuid3));

        facade.removeDataSet(dsname);
    }

    /**
     * DG Test method for
     * {@link fr.cs.ikats.ts.dataset.DataSetFacade#updateDataSet(java.lang.String, java.lang.String, java.util.List)}
     * .
     * @throws IkatsDaoException
     */
    @Test
    public void testUpdateDataSet() throws IkatsDaoException {

        String testCaseName = "testUpdateDataSet";

        DataSetFacade facade = getDatasetFacade();

        HashMap<String, String> mapTsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "MAM", "toto"}, testCaseName);
        HashMap<String, String> map2TsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid2", "hello", "titi", "bye"}, testCaseName);
        HashMap<String, String> expectedMapTsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "MAM", "toto", "tsuid2", "hello", "titi", "bye"}, testCaseName);

        List<String> tsuids = new ArrayList<String>(mapTsuidToFuncId.keySet());
        List<String> tsuids2 = new ArrayList<String>(map2TsuidToFuncId.keySet());
        List<String> expectedTsuid = new ArrayList<String>(expectedMapTsuidToFuncId.keySet());

        // pre-condition : tsuids exist with functional ids
        saveFuncIds(mapTsuidToFuncId, tsuids);
        saveFuncIds(map2TsuidToFuncId, tsuids2);

        String dsname = encodeDatasetName("dataSet_a_updater", testCaseName);
        String description = "Description courte du dataset";
        String results = facade.persistDataSet(dsname, description, tsuids);
        assertEquals(dsname, results);
        DataSet datasetToUpdate = facade.getDataSet(dsname);

        assertEquals(3, datasetToUpdate.getLinksToTimeSeries().size());
        assertEquals(description, datasetToUpdate.getDescription());

        // Test1: do not change description + append one tsuid
        facade.updateInAppendMode(dsname, null, tsuids2);

        DataSet updatedDataset = facade.getDataSet(dsname);
        getLogger().info(updatedDataset.toDetailedString(false));
        assertEquals("Test1: do not change description + append one tsuid", 7, updatedDataset.getLinksToTimeSeries().size());
        assertEquals("Test1: do not change description + append one tsuid", description, updatedDataset.getDescription());
        Collections.sort(expectedTsuid);
        List<String> actual = updatedDataset.getTsuidsAsString();
        Collections.sort(actual);
        assertEquals("Test1: do not change description + append one tsuid", expectedTsuid, actual);

        // Test2: change description + unchanged tsuids
        String description2 = "Desc two";
        facade.updateInAppendMode(dsname, description2, tsuids2);

        DataSet updatedDataset2 = facade.getDataSet(dsname);
        getLogger().info(updatedDataset.toDetailedString(false));
        assertEquals("Test2: change description + unchanged tsuids", 7, updatedDataset2.getLinksToTimeSeries().size());
        assertEquals("Test2: change description + unchanged tsuids", description2, updatedDataset2.getDescription());
        List<String> actual2 = updatedDataset.getTsuidsAsString();
        Collections.sort(actual2);
        assertEquals("Test2: change description + unchanged tsuids", expectedTsuid, actual2);

        facade.removeDataSet(dsname);
    }

    @Test
    public void testRemoveTsFromDataSet() throws IkatsDaoException {

        String testCaseName = "testRemoveTsFromDataSet";

        DataSetFacade facade = getDatasetFacade();

        HashMap<String, String> map2TsuidToFuncId = getTestedTsuidFidMap(new String[]{"tsuid2", "hello", "toto", "titi", "bye"}, testCaseName);
        List<String> tsuids2 = new ArrayList<String>(map2TsuidToFuncId.keySet());

        // pre-condition : tsuids exist with functional ids
        saveFuncIds(map2TsuidToFuncId, tsuids2);

        String dsname = encodeDatasetName("dataset_with_removed_ts", testCaseName);
        String description = "Description courte du dataset";
        String results = facade.persistDataSet(dsname, description, tsuids2);
        assertEquals(dsname, results);
        DataSet datasetToUpdate = facade.getDataSet(dsname);

        assertEquals(5, datasetToUpdate.getLinksToTimeSeries().size());
        assertEquals(description, datasetToUpdate.getDescription());

        // Tested service:
        List<String> tsuidsToRemove = new ArrayList<String>();
        tsuids2.forEach(tsuid -> {
            if (tsuid.startsWith("toto") || tsuid.startsWith("titi")) tsuidsToRemove.add(tsuid);
        });

        facade.removeTsLinks(dsname, tsuidsToRemove);

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
    }

    /**
     * Test method for
     * {@link fr.cs.ikats.ts.dataset.DataSetFacade#removeDataSet(java.lang.String)}
     * .
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testRemoveDataset() throws IkatsDaoException {

        String testCaseName = "testRemoveDataset";

        DataSetFacade facade = getDatasetFacade();

        HashMap<String, String> testTsuidFuncId = getTestedTsuidFidMap(new String[]{"tsuid1", "tsuid2"}, testCaseName);

        List<String> tsuids = new ArrayList<String>(testTsuidFuncId.keySet());

        saveFuncIds(testTsuidFuncId, tsuids);

        String dsname = encodeDatasetName("dataSet3", testCaseName);
        // ------- PRe-clean TU data -----------
        try {
            facade.removeDataSet(dsname);
        } catch (IkatsDaoMissingResource e) {
            // nothing to do: expected behaviour
        }

        facade.persistDataSet(dsname, "desc courte 1", tsuids);
        // --------------------------------------

        facade.removeDataSet(dsname);
        try {
            facade.getDataSet(dsname);
        } catch (IkatsDaoMissingResource e) {
            // nothing to do: expected behaviour
        }

        try {
            facade.removeDataSet(dsname);
        } catch (IkatsDaoMissingResource e) {
            // nothing to do: expected behaviour
        }
    }

    /**
     * Test method for
     * {@link fr.cs.ikats.ts.dataset.DataSetFacade#getDataSet(java.lang.String)}
     * .
     * @throws IkatsDaoMissingResource
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testGetDataSet() throws IkatsDaoMissingResource, IkatsDaoException {

        String testCaseName = "testGetDataSet";

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

    }

    /**
     * Test method for
     * {@link fr.cs.ikats.ts.dataset.DataSetFacade#getAllDataSetSummary()}.
     * @throws IkatsDaoMissingResource
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testGetSummaryDataSet() throws IkatsDaoMissingResource, IkatsDaoException {

        String testCaseName = "testGetSummaryDataSet";

        DataSetFacade facade = getDatasetFacade();

        // ---------- prepare data ... -----------------------------------------
        PreparedTsReferences preparedData = new PreparedTsReferences(new String[]{"tsuid1", "tsuid2"}, testCaseName, true);

        List<DataSet> result;
        try {
            result = facade.getAllDataSetSummary();
            for (DataSet ds : result) {
                facade.removeDataSet(ds.getName());
            }
        } catch (IkatsDaoMissingResource e) {
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
    }

    /**
     * Test method for
     * {@link fr.cs.ikats.ts.dataset.DataSetFacade#getDataSetNamesForTsuid(java.lang.String)}
     * .
     * @throws IkatsDaoMissingResource
     *
     * @throws IkatsDaoException
     */
    @Test
    public void testGetDataSetNamesForTsuid() throws IkatsDaoMissingResource, IkatsDaoException {

        String testCaseName = "testGetDataSetNamesForTsuid";

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

    protected ArrayList<FunctionalIdentifier> saveFuncIds(HashMap<String, String> mapTsuidToFuncId, List<String> tsuids) throws IkatsDaoException {
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

        @SuppressWarnings("unused")
        private String testCaseName;
        public HashMap<String, String> mapByTsuidTheFuncId;
        public List<String> tsuids;

        public PreparedTsReferences(String[] tsuidPrefixes, String testCaseName, boolean saveFunctionalIdentifiers) throws IkatsDaoException {

            this.testCaseName = testCaseName;
            mapByTsuidTheFuncId = getTestedTsuidFidMap(tsuidPrefixes, testCaseName);
            tsuids = new ArrayList<String>(mapByTsuidTheFuncId.keySet());

            if (saveFunctionalIdentifiers) {
                saveFuncIds(mapByTsuidTheFuncId, tsuids);
            }
        }
    }
}
