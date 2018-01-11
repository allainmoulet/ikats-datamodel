/**
 * LICENSE:
 * --------
 * Copyright 2017-2018 CS SYSTEMES D'INFORMATION
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
 * @author Maxime PERELMUTER <maxime.perelmuter@c-s.fr>
 *
 */

package fr.cs.ikats.datamanager.client.opentsdb.importer;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import fr.cs.ikats.datamanager.DataManagerException;
import fr.cs.ikats.datamanager.client.importer.IImportSerializer;
import fr.cs.ikats.datamanager.client.opentsdb.generator.SimpleJsonGenerator;

/**
 *
 */
@Component
@Qualifier("Simple")
public class CSVJsonIzerForOpentsdb implements IImportSerializer {

    BufferedReader reader;
    private String dataset;
    private int period;
    private String tag;
    boolean hasNext = true;
    long totalPointsRead = 0;

    @Override
    public void init(BufferedReader reader, String fileName, String metric, Map<String, String> tags) {
        this.period = 1;
        this.tag = "numero";
    }

    /*
     * (non-Javadoc)
     *
     * @see fr.cs.ikats.temporaldata.model.importer.IJsonReader#next()
     */
    public synchronized String next(int numberOfPoints) throws IOException, DataManagerException {
        String line = reader.readLine();
        String json = null;
        if (line != null) {
            totalPointsRead++;
            SimpleJsonGenerator generateur = new SimpleJsonGenerator(dataset, period, tag);
            json = generateur.generate(line);
        } else {
            hasNext = false;
        }
        return json;
    }

    /*
     * (non-Javadoc)
     *
     * @see fr.cs.ikats.temporaldata.model.importer.IJsonReader#close()
     */
    public void close() {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean hasNext() {
        return hasNext;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#clone()
     */
    @Override
    public CSVJsonIzerForOpentsdb clone() {
        return new CSVJsonIzerForOpentsdb();
    }

    @Override
    public long[] getDates() {
        return null;
    }

    @Override
    public boolean test(String inputline) {
        return true;
    }

    public long getTotalPointsRead() {
        return totalPointsRead;
    }
}
