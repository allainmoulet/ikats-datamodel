package fr.cs.ikats.common.junit;

import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * This abstract superclass provides basic JUnit test management for IKATS ...
 * shared by all Junit test classes in IKATS.
 * 
 * 
 * This class is not a test: it is a tool for tests: so it is under maven
 * src/main/java. Exemple of use: see DataSetTest
 * 
 * @author mberaud
 */
abstract public class CommonTest {

    /**
     * Log marker for JUnit test case (several per class)
     */
    final public static String DECO_JUNIT_LINE = "-------------------";

    /**
     * Log marker for JUnit test class
     */
    final public static String DECO_JUNIT_CLASS_LINE = "===================";

    /**
     * Log of the unit test which inherits from this class: initialized by
     * initLogger()
     */
    private Logger logger = null;
    
    /**
     * Test case shall provide its own logger, supplying this implementation.
     * 
     * @return
     */
    final protected Logger getLogger() {
        if (logger == null) {
            // Init
            logger = Logger.getLogger(this.getClass().getSimpleName());
            logger.setLevel(Level.INFO);
        }
        return logger;
    }

    /**
     * Data builder for tests: build unique couples organized into a HashMap:
     * <ul>
     * <li>keys: TSUIDs prefixed by each tsuidPrefixes and suffixed by
     * testCaseName</li>
     * <li>values: FUNCIDs: functional identifiers: "funcId_" + TSUID</li>
     * </ul>
     * 
     * @param tsuidPrefixes
     * @param testCaseName
     * @return map
     */
    protected HashMap<String, String> getTestedTsuidFidMap(String[] tsuidPrefixes, String testCaseName) {
        HashMap<String, String> map = new HashMap<String, String>();
        for (int i = 0; i < tsuidPrefixes.length; i++) {
            String tsuidKey = tsuidPrefixes[i] + "_" + testCaseName;
            String funcIdValue = "funcId_" + tsuidKey;
            map.put(tsuidKey, funcIdValue);
        }
        return map;
    }

    /**
     * Encode tsuid value according to ikats TU naming convention
     * @param tsuidRawValue readable value of tsuid: actually a prefix
     * @param testCaseName
     * @return tsuidRawValue + "_" + testCaseName;
     */
    protected String encodeTsuid(String tsuidRawValue, String testCaseName )
    {
        return tsuidRawValue + "_" + testCaseName;
    }
    
    /**
     * Encode dataset name value according to ikats TU naming convention
     * @param datasetName readable value of dataset name: actually a prefix
     * @param testCaseName
     * @return datasetName + "_" + testCaseName;
     */
    protected String encodeDatasetName(String datasetName, String testCaseName )
    {
        return datasetName + "_" + testCaseName;
    }
}
