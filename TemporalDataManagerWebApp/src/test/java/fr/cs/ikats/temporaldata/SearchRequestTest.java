/**
 * 
 */
package fr.cs.ikats.temporaldata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.datamanager.client.opentsdb.QueryMetaResult;
import fr.cs.ikats.datamanager.client.opentsdb.ResponseParser;
import fr.cs.ikats.temporaldata.business.TSInfo;

/**
 * 
 * @author ikats
 *
 */
public class SearchRequestTest extends AbstractRequestTest {

	@BeforeClass
	public static void setUpBeforClass() {
		AbstractRequestTest.setUpBeforClass(SearchRequestTest.class.getSimpleName());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		AbstractRequestTest.tearDownAfterClass(SearchRequestTest.class.getSimpleName());
	}

	@Test
	public void testGetMeta() {

		String testCaseName = "testGetMeta";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);

			String url = getAPIURL() + "/ts/tsuid/" + "0000110000030003F20000040003F1";

			Response response = utils.sendGETRequest(MediaType.APPLICATION_JSON, utils.getClientWithJSONFeature(), url,
					"172.28.0.56");
			getLogger().info(response.readEntity(String.class));

			// TSInfo tsInfo = response.readEntity(TSInfo.class);
			// assertEquals("0000110000030003F20000040003F1",tsInfo.getTsuid());
			// assertEquals("A320001_1_WS1",tsInfo.getfuncId());
			// assertEquals(2,tsInfo.getTags().keySet().size());
			// assertEquals("1",tsInfo.getTags().get("flightIdentifier"));
			// assertEquals("A320001",tsInfo.getTags().get("aircraftIdentifier"));
			// assertEquals("WS1",tsInfo.getMetric());
			// getLogger().info("TSInfo : "+tsInfo);

			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}

	}

	@Test
	public void testLookup() {
		
		String testCaseName = "testLookup";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);
			
			String url = getAPIURL() + "/ts/lookup/poc3.arctan?numero=00001&numero=00002";
			sendRequest(url);

			url = getAPIURL() + "/ts/lookup/poc3.arctan?numero=00001";
			sendRequest(url);

			url = getAPIURL() + "/ts/lookup/poc3.arctan?*=00001";
			sendRequest(url);

			url = getAPIURL() + "/ts/lookup/poc3.arctan?numero=*";
			sendRequest(url);

			url = getAPIURL() + "/ts/lookup/*?numero=00001";
			sendRequest(url);


			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}
	}

	@Test
	public void testLookup_DG1() {
		
		String testCaseName = "testLookup_DG1";
		boolean isNominal = false;
		try {
			start(testCaseName, isNominal);
			String url = getAPIURL() + "/ts/lookup/poc3.arctan?tot=00001&numero=00002";
			sendRequest(url);
			endWithFailure(testCaseName, new Exception( "Missing error for " + url) );
		} catch (IkatsWebClientException eOk) {
			getLogger().info( "Ok: expected error IkatsWebClientException is raised: " + eOk.getMessage() );
			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}
 
	}

	@Test
	public void testLookup_DG2() {
		
		String testCaseName = "testLookup_DG2";
		boolean isNominal = false;
		try {
			start(testCaseName, isNominal);
			String url = getAPIURL() + "/ts/lookup/poc3.arctan?at=00001";
			sendRequest(url);
			endWithFailure(testCaseName, new Exception( "Missing error for " + url) );
		} catch (IkatsWebClientException eOk) {
			getLogger().info( "Ok: expected error IkatsWebClientException is raised: " + eOk.getMessage() );
			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		} 
	}

	@Test
	public void testLookup_DG3() {
		
		String testCaseName = "testLookup_DG3";
		boolean isNominal = false;
		try {
			start(testCaseName, isNominal);
			String url = getAPIURL() + "/ts/lookup/poc3.arct?*=00001";
			sendRequest(url);
			endWithFailure(testCaseName, new Exception( "Missing error for " + url) );
		} catch (IkatsWebClientException eOk) {
			getLogger().info( "Ok: expected error IkatsWebClientException is raised: " + eOk.getMessage() );
			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}  
	}

	@Test
	public void testLookup_DG4() {
		
		String testCaseName = "testLookup_DG4";
		boolean isNominal = false;
		try {
			start(testCaseName, isNominal);
			String url = getAPIURL() + "/ts/lookup/poc3.*";
			sendRequest(url);
			endWithFailure(testCaseName, new Exception( "Missing error for " + url) );
		} catch (IkatsWebClientException eOk) {
			getLogger().info( "Ok: expected error IkatsWebClientException is raised: " + eOk.getMessage() );
			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}
	}

	/**
	 * @param url
	 * @throws IkatsWebClientException
	 */
	protected void sendRequest(String url) throws IkatsWebClientException {
		getLogger().info(url);
		Response reponse = null;
		for (int i = 0; i < 10; i++) {
			reponse = utils.sendGetRequest(url, "172.28.0.56");
			if (reponse.getStatus() <= 200) {
				QueryMetaResult result = ResponseParser.parseLookupReponse(reponse);
				getLogger().info(result.getTsuids());
			} else {
				throw new IkatsWebClientException();
			}
		}
	}

	@Test
	public void testTSLookup_DG() {
		
		String testCaseName = "testTSLookup_DG";
		boolean isNominal = false;
		try {
			start(testCaseName, isNominal);
			
			String url = getAPIURL() + "/ts/lookup/ts/test";
			sendRequest(url);
			// TODO ??? bizarre ce test avec 2 requete => 2 TU ???
			url = getAPIURL() + "/ts/lookup/ts/test_ex";
			sendRequest(url);
			
			endWithFailure(testCaseName, new Exception( "Missing error for " + url) );
		} catch (IkatsWebClientException eOk) {
			getLogger().info( "Ok: expected error IkatsWebClientException is raised: " + eOk.getMessage() );
			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		} 
	}
}
