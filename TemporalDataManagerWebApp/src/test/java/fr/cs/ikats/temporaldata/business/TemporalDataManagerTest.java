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
        }
        catch (InvalidValueException e) {
            System.out.println(e.getMessage());
            //assertEquals(e.getMessage())
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
        }
        catch (InvalidValueException e) {
            // OK
            System.out.println(e.getMessage());
            assertTrue(true);
            String expected = "Value \"azery%%%\" for Parameter \"FuncId\" of type \"TimeSerie\" is not correctly formated. \"[a-zA-Z0-9_]*\" format is expected.";
            assertEquals(expected,e.getMessage());
            
        }
    }
}
