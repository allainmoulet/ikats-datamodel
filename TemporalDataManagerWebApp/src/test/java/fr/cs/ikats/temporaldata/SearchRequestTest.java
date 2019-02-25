/**
 * Copyright 2018-2019 CS Systèmes d'Information
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.cs.ikats.temporaldata;

import java.io.UnsupportedEncodingException;

import javax.ws.rs.core.UriInfo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.temporaldata.business.TemporalDataManager;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.resource.TimeSerieResource;

import static org.junit.Assert.assertTrue;

/**
 *
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
    public void testLookup() throws UnsupportedEncodingException, ResourceNotFoundException {

        TemporalDataManager mockedTdm = Mockito.spy(TemporalDataManager.class);

        String expectedJsonResponse = "{ 'test_stubbed': 'any JSON content from opentsbdb'}";
        Mockito.doReturn(expectedJsonResponse).when(mockedTdm).getTS(Mockito.anyString(), Mockito.any());

        // TODO: inject mockedTdm into tested wepapp
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

        ResourceNotFoundException expectedException = new ResourceNotFoundException(
                "Stubbed error: resource not found");
        try {

            TemporalDataManager mockedTdm = Mockito.spy(TemporalDataManager.class);
            Mockito.doThrow(expectedException).when(mockedTdm).getTS(Mockito.anyString(), Mockito.any());

            // TODO : inject mockedTdm into tested wepapp
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
