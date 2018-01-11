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

package fr.cs.ikats.temporaldata.business.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Singleton;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import fr.cs.ikats.datamanager.client.importer.IImportSerializer;

/**
 * Import Serializer factory.
 *
 *
 * The serializer allow writing a string JSON or Line according to the underlying temporal database.
 *
 * They generate these channels from the exchange of files provided .
 *
 * They are normally able to cut the import file to generate relatively short character strings .
 * Csv , the maximum size should be 500 000 points for opentsdb
 */
@Component
@Singleton
public class ImportSerializerFactory {

    private static Logger logger = Logger.getLogger(ImportSerializerFactory.class);

    /**
     * instance of Serializer,
     * autowired from Spring context
     */
    @Autowired
    private List<IImportSerializer> serializers;

    /**
     * get an usefull of serializer, list all the serializer and test its against the first data line ( after the header line)
     * of the given inputStream. The first serializer able to parse the line is selected.
     *
     * Note : if only one serializer is found in the classpath, use it directly without testing it.
     *
     * @param fileName name of the file to serialize
     * @param metric   metric of the file
     * @param is       inputStream to serialize
     * @param tags     list of tags common to all the points in the input file
     * @return an instance of the serializer.
     * @throws IOException if inputStream cannot be read
     */
    public IImportSerializer getBetterSerializer(String fileName, String metric, InputStream is, Map<String, String> tags) throws IOException {
        List<IImportSerializer> newSerializerList = getObjects();
        IImportSerializer result = null;
        if (newSerializerList.size() == 1) {
            result = newSerializerList.get(0);
        } else {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            // mark the reader to be able to reset it to the start of the file
            // set 500 char max before reseting is still allowed
            reader.mark(500);
            // consume and log the first line because it is the header line
            logger.info("header : " + reader.readLine());
            // read the first line to test the serializers
            String testLine = reader.readLine();
            logger.debug("testLine = " + testLine);
            // reset the reader to the previsously marked position.
            reader.reset();

            for (IImportSerializer newSerializer : newSerializerList) {
                logger.info("testing : " + newSerializer.getClass().getName());
                // test the serializer against the testline
                boolean test = newSerializer.test(testLine);
                if (test) {
                    // test succeeded, so init the serializer and break
                    logger.info("Serializer found : " + newSerializer.getClass().getName());
                    newSerializer.init(reader, fileName, metric, tags);
                    result = newSerializer;
                    break;
                }
            }
        }
        return result;
    }

    /**
     * get a clone objet of serializer
     *
     * @return
     */
    private IImportSerializer getObject(int index) {
        IImportSerializer serializer = this.serializers.get(index).clone();
        return serializer;
    }

    /**
     * get a clone objet of serializer
     *
     * @return
     */
    private List<IImportSerializer> getObjects() {
        List<IImportSerializer> serializers = new ArrayList<IImportSerializer>();
        for (int index = 0; index < this.serializers.size(); index++) {
            serializers.add(getObject(index));
        }
        return serializers;
    }

}
