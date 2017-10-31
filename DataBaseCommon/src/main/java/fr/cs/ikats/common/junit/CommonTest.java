package fr.cs.ikats.common.junit;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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
     * Recurrent need: build location of test case
     * 
     * @param testCaseName
     * @return
     */
    protected String getTestLocation(String testCaseName) {
        String testLocation = this.getClass().getSimpleName() + ": " + testCaseName;
        return testLocation;
    }

    /**
     * Data builder for tests: build unique ts identifiers prefixed by each
     * tsuidPrefixes and suffixed by testCaseName
     * 
     * @param tsuidPrefixes
     * @param testCaseName
     * @return
     */
    protected ArrayList<String> getTestedTsuids(String[] tsuidPrefixes, String testCaseName) {
        ArrayList<String> list = new ArrayList<String>();
        for (int i = 0; i < tsuidPrefixes.length; i++) {
            list.add(tsuidPrefixes[i] + "_" + testCaseName);
        }
        return list;
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
     * Method to be called on test case failure: logs the failure
     * 
     * @param testCaseName
     * @param e
     */
    protected void endWithFailure(String testCaseName, Throwable e) {

        String testLocation = getTestLocation(testCaseName);

        if (e instanceof AssertionError) {
            AssertionError ae = (AssertionError) e;
            getLogger().error("Failure got Assertion in " + testLocation, ae);
            getLogger().error("Failure Assertion::getMessage: " + ae.getMessage());
            getLogger().error("Failure Assertion::getLocalizedMessage: " + ae.getLocalizedMessage());
        }
        else {
            getLogger().error("Failure got error: " + testLocation, e);
        }

        // error may be long ... surrounded by test location
        getLogger().info(junitLine("Ended JUnit case: Failed: " + testLocation));
        String failMessage = testLocation + ": [";
        if ((e != null) && (e.getMessage() != null)) {
            failMessage += e.getMessage() + "]";
        }
        else {
            failMessage += "null]";
        }

        fail(failMessage);
    }

    /**
     * Method to be called, at the beginning of the test case: writes logs
     * 
     * @param testCaseName
     * @param isNominal
     */
    protected void start(String testCaseName, boolean isNominal) {
        String nominalInfo = isNominal ? "NOMINAL" : "DEGRADED";
        getLogger().info(junitLine("Started JUnit case: " + getTestLocation(testCaseName)));
        getLogger().info(junitLine("tested context is" + nominalInfo + " ... "));
    }

    /**
     * Method to be called, on nominal ending of the test case: writes logs
     * 
     * @param testCaseName
     * @param isNominal
     */
    protected void endNominal(String testCaseName) {
        getLogger().info(junitLine("Ended JUnit case: " + getTestLocation(testCaseName)));
    }
    
    /**
     * success on degraded test: when expected error is received
     * @param testCaseName
     * @param te
     */
    protected void endOkDegraded(String testCaseName, Throwable te) {
        getLogger().info("DG Test is OK, got expected error:", te);
        getLogger().info(junitLine("Ended JUnit case: " + getTestLocation(testCaseName)));
    }

    /**
     * Encode a JUNIT line wrapped with DECO_JUNIT_LINE
     * 
     * @param content
     *            the content of the line
     * @return wrapped JUNIT line for IKATS
     */
    final protected String junitLine(String content) {
        StringBuilder lBuff = new StringBuilder(DECO_JUNIT_LINE);
        if (!content.startsWith(" ")) {
            lBuff.append(" ");
        }
        lBuff.append(content);
        if (!content.endsWith(" ")) {
            lBuff.append(" ");
        }
        lBuff.append(DECO_JUNIT_LINE);
        return lBuff.toString();
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
