package fr.cs.ikats.datamanager.client.importer;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;

import fr.cs.ikats.datamanager.DataManagerException;

/**
 * Serialisation class. Read from inputStream and return a serialized String to
 * send to the underlying database. String can be in a json or Line format.
 * 
 * @author ikats
 *
 */
public interface IImportSerializer extends Cloneable {

    /**
     * initialise the Serializer with the input stream
     * 
     * @param reader the BufferedReader
     * @param fileName the fileName
     * @param metric the metric
     * @param tags the tags 
     * @param numberOfPointsByImport number of points to serialize at each next() call.
     */
    void init(BufferedReader reader, String fileName, String metric, Map<String, String> tags);

    /**
     * get the next Import String.
     * 
     * @return the next input string
     * @throws IOException 
     * @throws DataManagerException 
     */
    String next(int numberOfPoints) throws IOException, DataManagerException;

    /**
     * no more string to
     * 
     * @return false if nothing to be done 
     */
    boolean hasNext();

    /**
     * close the serailizer and all opened resources
     */
    void close();

    /**
     * return a new clone instance of the serializer.
     * To avoid mutlithreading issues, each thread ust have his own Serializer.
     * @return a clone instance
     */
    IImportSerializer clone();
    
    /**
     * return the first date and the last date of all the serialized datapoints.
     * This information is owned by the cloned instance.
     * @return teh start and stop dates
     */
    long[] getDates();

    /**
     * @param inputline
     * @return true if line can be read
     */
    boolean test(String inputline);

    /**
     * Get the total number of points read
     * @return the number of points
     */
	public long getTotalPointsRead();


}