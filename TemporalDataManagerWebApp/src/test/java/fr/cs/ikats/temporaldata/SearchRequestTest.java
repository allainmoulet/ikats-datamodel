/**
 * 
 */
package fr.cs.ikats.temporaldata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.datamanager.client.opentsdb.ImportResult;
import fr.cs.ikats.datamanager.client.opentsdb.QueryMetaResult;
import fr.cs.ikats.datamanager.client.opentsdb.ResponseParser;
import fr.cs.ikats.temporaldata.business.TSInfo;
import fr.cs.ikats.temporaldata.business.TemporalDataManager;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.resource.TimeSerieResource;

/**
 * 
 * @author ikats
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class SearchRequestTest extends AbstractRequestTest {

	@BeforeClass
	public static void setUpBeforClass() {
		AbstractRequestTest.setUpBeforClass(SearchRequestTest.class.getSimpleName());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		AbstractRequestTest.tearDownAfterClass(SearchRequestTest.class.getSimpleName());
	}


	/**
	 * checks that lookup service is returning the response from opentsdb service.
	 * @throws ResourceNotFoundException 
	 * @throws IkatsWebClientException 
	 * @throws UnsupportedEncodingException 
	 */
	@Test
	public void testLookup() throws UnsupportedEncodingException, IkatsWebClientException, ResourceNotFoundException {
        // FIXME 163211 IF DEAD CODE => remove lookup + test ?
		//
		String testCaseName = "testLookup";
		boolean isNominal = true;

		TemporalDataManager mockedTdm = Mockito.spy(TemporalDataManager.class);

		String expectedJsonResponse = "{ 'test_stubbed': 'any JSON content from opentsbdb'}";
		Mockito.doReturn(expectedJsonResponse).when(mockedTdm).getTS(Mockito.anyString(), Mockito.any());

		// TODO 163211 or later: inject mockedTdm into tested wepapp
		// and then call the client ... and read String from Response
		// ... temporary solution: call the service directly
		TimeSerieResource services = new TimeSerieResource();
		services.setTemporalDataManager(mockedTdm);
		UriInfo mockedUriInfo = Mockito.mock(UriInfo.class);
		String metric = "searchedMetric";
		String result = services.getTS(metric, mockedUriInfo);

		assertTrue(result.equals(expectedJsonResponse));
	}

	@Test
	public void testLookupNotFound() throws UnsupportedEncodingException, IkatsWebClientException {
		
		// FIXME 163211 IF DEAD CODE => remove lookup + test ? => remove this whole file SearchRequestTest
		ResourceNotFoundException expectedException = new ResourceNotFoundException(
				"Stubbed error: resource not found");
		try {

			TemporalDataManager mockedTdm = Mockito.spy(TemporalDataManager.class);
			Mockito.doThrow(expectedException).when(mockedTdm).getTS(Mockito.anyString(), Mockito.any());

			// TODO 163211 or later: inject mocledTdm into tested wepapp
			// and then call the client ... and read String from Response
			// ... temporary solution: call the service directly
			TimeSerieResource services = new TimeSerieResource();
			services.setTemporalDataManager(mockedTdm);
			UriInfo mockedUriInfo = Mockito.mock(UriInfo.class);
			String metric = "searchedMetric";
			String result = services.getTS(metric, mockedUriInfo);

		} catch (ResourceNotFoundException notFoundError) {
			assertTrue(notFoundError.getMessage().equals(expectedException.getMessage())
					|| notFoundError.getCause().getMessage().equals(expectedException.getMessage()));

		}

	}
}
