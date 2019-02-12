/**
 * Copyright 2018 CS Systèmes d'Information
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

package fr.cs.ikats.temporaldata.business;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.log4j.Logger;
import org.junit.Test;

import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;

/**
 * Test for TemporalDataManager
 */
public class TemporalDataManagerTest {

    private final Logger logger = Logger.getLogger(TemporalDataManagerTest.class);

    /**
     * Test method for {@link TemporalDataManager#validateFuncId(String)}
     */
    @Test
    public void testValidateFuncId() {
        String funcId = "azery_tTS_p09176232";
        TemporalDataManager manager = new TemporalDataManager();
        try {
            assertTrue(manager.validateFuncId(funcId));
        } catch (InvalidValueException e) {
            fail();
        }

    }

    /**
     * DG Test method for {@link TemporalDataManager#validateFuncId(String)}
     */
    @Test
    public void testValidateFuncId_dg() {
        String funcId = "azery%%%";
        TemporalDataManager manager = new TemporalDataManager();
        try {
            manager.validateFuncId(funcId);
        } catch (InvalidValueException e) {
            // OK
            assertTrue(true);
            String expected = "Value \"azery%%%\" for Parameter \"FuncId\" of type \"TimeSerie\" is not correctly formated. \"[a-zA-Z0-9_-]+\" format is expected.";
            assertEquals(expected, e.getMessage());

        }
    }
}
