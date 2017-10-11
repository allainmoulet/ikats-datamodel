package fr.cs.ikats.temporaldata;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.validation.constraints.AssertTrue;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import fr.cs.ikats.datamanager.client.opentsdb.ImportResult;

/**
 * @author ikats
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ImportRequestTest extends AbstractRequestTest {

	@BeforeClass
	public static void setUpBeforClass() {
		AbstractRequestTest.setUpBeforClass(ImportRequestTest.class.getSimpleName());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		AbstractRequestTest.tearDownAfterClass(ImportRequestTest.class.getSimpleName());
	}

	/**
	 * Test import of a TS from a csv simple file
	 */
	@Test
	public void importTSWithDataSetAndTags_DG() {
		String testCaseName = "importTSWithDataSetAndTags_DG";
		boolean isNominal = true; // does not throw exception
		try {
			start(testCaseName, isNominal);
 
			File file = getFileMatchingResource(testCaseName, "/data/test_import_bad_format.csv");
			 
			getLogger().info("CSV input file : " + file.getAbsolutePath());
			String metric = "testmetric";
			String url = getAPIURL() + "/ts/put/" + metric;
			utils.doImport(file, url, true, 400);

			endNominal(testCaseName);

		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}

	}

	/**
	 * Test import of a TS from a csv simple file
	 */
	@Test
	public void importTSWithDataSetAndTags() {

		String testCaseName = "importTSWithDataSetAndTags";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);

			Resource resource = new ClassPathResource("/data/test_import.csv");
			File file = null;
			try {
				file = resource.getFile();
			} catch (IOException e1) {
				getLogger().error("Error in: " + testCaseName + ": getting File for resource /data/test_import.csv",
						e1);
				throw e1;
			}
			getLogger().info("CSV input file : " + file.getAbsolutePath());
			String metric = "testmetric";
			String url = getAPIURL() + "/ts/put/" + metric;
			// utils.doImport(file, url, true, 200);
			ImportResult result = utils.doImportStubbedOpenTSDB(file, url, testCaseName, true, 200, true);
			
			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}

	}

	/**
	 * Test import of a TS - check of start/end date calculation and DB saving
	 * UPDATE agn 04/25: also check metric and tags
	 */
	@Test
	public void checkMetadataCompletenessOfImport() {

		String testCaseName = "checkMetadataCompletenessOfImport";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);

			Resource resource = new ClassPathResource("/data/test_import.csv");
			File file = null;
			try {
				file = resource.getFile();
			} catch (IOException e1) {
				getLogger().error("Error in: " + testCaseName + ": getting File for resource /data/test_import.csv",
						e1);
				throw e1;
			}

			/* import of the timeseries */
			getLogger().info("CSV input file : " + file.getAbsolutePath());
			String metric = "testmetric";
			String url = getAPIURL() + "/ts/put/" + metric;
			ImportResult retour = utils.doImportStubbedOpenTSDB(file, url, testCaseName, true, 200, true);
			
			/*
			 * retrieval of the tsuid, start_date and end_date from import task
			 */
			String tsuid = retour.getTsuid();
			getLogger().info("tsuid = " + tsuid);
			String startDateImport = Long.toString(retour.getStartDate());
			getLogger().info("start date from import = " + startDateImport);
			String endDateImport = Long.toString(retour.getEndDate());
			getLogger().info("end date from import = " + endDateImport);

			/* getting the metadata of the tsuid in database */
			url = getAPIURL() + "/metadata/list/json?tsuid=" + tsuid;
			Client client = utils.getClientWithJSONFeature();
			WebTarget target = client.target(url);

			Response response = target.request().get();
			getLogger().info("parsing response of " + target.getUri());

			/* retrieving start/end dates from response */
			ArrayList<HashMap<String, String>> result = (ArrayList<HashMap<String, String>>) response
					.readEntity(List.class);
			String startDateMeta = "";
			String endDateMeta = "";
			String metricMeta = "";
			String tag1Meta = "";
			String tag2Meta = "";
			getLogger().info(TestUtils.TAG2_K);
			getLogger().info(result.toString());
			for (HashMap<String, String> map : result) {
				if (map.get("name").equals("ikats_start_date")) {
					startDateMeta = (String) map.get("value");
					getLogger().info("start date from database = " + startDateMeta);
				}
				if (map.get("name").equals("ikats_end_date")) {
					endDateMeta = (String) map.get("value");
					getLogger().info("end date from database = " + endDateMeta);
				}
				if (map.get("name").equals("metric")) {
					metricMeta = (String) map.get("value");
					getLogger().info("metric from database = " + metricMeta);
				}
				if (map.get("name").equals(TestUtils.TAG1_K)) {
					tag1Meta = (String) map.get("value");
					getLogger().info(TestUtils.TAG1_K + " from database = " + tag1Meta);
				}
				if (map.get("name").equals(TestUtils.TAG2_K)) {
					tag2Meta = (String) map.get("value");
					getLogger().info(TestUtils.TAG2_K + " from database = " + tag2Meta);
				}
			}

			/* check status */
			assertEquals(response.getStatus(), 200);

			/* check dates from import equal dates from database */
			assertEquals(startDateImport, startDateMeta);
			assertEquals(endDateImport, endDateMeta);

			/* check metric from import equal metric from database */
			assertEquals(metric, metricMeta);

			/*
			 * check tags from import equal tags from database, defined in
			 * TestUtils
			 */
			assertEquals(tag1Meta, TestUtils.TAG1_V);
			assertEquals(tag2Meta, TestUtils.TAG2_V);

			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}

	}

	/**
	 * Test import of a TS from a csv simple file
	 */
	@Test
	public void importTSWithDataSet() {

		String testCaseName = "importTSWithDataSet";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);

			Resource resource = new ClassPathResource("/data/test_import.csv");
			File file = null;
			try {
				file = resource.getFile();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			getLogger().info("CSV input file : " + file.getAbsolutePath());
			String metric = "testmetric2";
			String url = getAPIURL() + "/ts/put/" + metric;
			 
			ImportResult result = utils.doImportStubbedOpenTSDB(file, url, testCaseName, false, 200, true);
			
			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}
	}

	/**
	 * Test import of a TS from a csv simple file
	 */
	@Test
	public void importTSWithoutDataSetAndTags() {

		String testCaseName = "importTSWithoutDataSetAndTags";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);

			Resource resource = new ClassPathResource("/data/test_import.csv");
			File file = null;
			try {
				file = resource.getFile();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			getLogger().info("CSV input file : " + file.getAbsolutePath());
			String metric = "testmetric3";
			String url = getAPIURL() + "/ts/put/" + metric;
			// utils.doImport(file, url, true, 200);
			ImportResult result = utils.doImportStubbedOpenTSDB(file, url, testCaseName, true, 200, true);
			
			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}

	}

	/**
	 * Test import of a TS from a csv simple file
	 */
	@Test
	public void importTSWithoutDataSet() {
		
		String testCaseName = "importTSWithoutDataSet";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);
			
			Resource resource = new ClassPathResource("/data/test_import.csv");
			File file = null;
			try {
				file = resource.getFile();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			getLogger().info("CSV input file : " + file.getAbsolutePath());
			String metric = "testmetric4";
			String url = getAPIURL() + "/ts/put/" + metric;
			// utils.doImport(file, url, false, 200);
			ImportResult result = utils.doImportStubbedOpenTSDB(file, url, testCaseName, false, 200, true);
			
			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}

		

	}

	/**
	 * Test import of a TS without functional identifier. must fail with a 400
	 * error code.
	 */
	@Test
	public void importTSWithoutFuncId() throws IOException {
		
		String testCaseName = "importTSWithoutFuncId";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);
		
			Resource resource = new ClassPathResource("/data/test_import.csv");
			File file = resource.getFile();
			
			getLogger().info("CSV input file : " + file.getAbsolutePath());
			String metric = "testmetric4";
			String url = getAPIURL() + "/ts/put/" + metric;
			// utils.doImport(file, url, false, 400, false);
			ImportResult result = utils.doImportStubbedOpenTSDB(file, url, testCaseName, false, 400, false);
			
			
			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}

		
		

	}

}
