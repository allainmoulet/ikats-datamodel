/**
 * LICENSE:
 * --------
 * Copyright 2017 CS SYSTEMES D'INFORMATION
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
 * 
 */

package fr.cs.ikats.datamanager.client.importer;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;

import fr.cs.ikats.datamanager.DataManagerException;

/**
 * Serialisation class. Read from inputStream and return a serialized String to
 * send to the underlying database. String can be in a json or Line format.
 * 
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

