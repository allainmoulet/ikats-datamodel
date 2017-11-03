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

package fr.cs.ikats.temporaldata.business.internal;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import fr.cs.ikats.datamanager.client.importer.IImportSerializer;
import fr.cs.ikats.temporaldata.application.TemporalDataApplication;

/**
 *
 */
public class ImportSerializerFactoryTest {

    private static Logger logger = Logger.getLogger(ImportSerializerFactoryTest.class);
    
    /**
     * Test method for {@link fr.cs.ikats.temporaldata.business.internal.ImportSerializerFactory#getBetterSerializer(java.lang.String, java.lang.String, java.lang.String, java.io.InputStream, java.util.Map)}.
     */
    @Test
    public void testGetBetterSerializer() {
        IImportSerializer serializer;
        File file = null;
        Resource resource = null;
        try {
            resource = new ClassPathResource("/data/test_import.csv");
            file = resource.getFile();
            serializer = doGetBetterSerializer(file);
            logger.info("Serializer "+serializer.getClass()+" produces :");
            logger.info(serializer.next(10));
            logger.info(serializer.next(10));
            resource = new ClassPathResource("/data/airbus_format.csv");
            file = resource.getFile();
            serializer = doGetBetterSerializer(file);
            logger.info("Serializer "+serializer.getClass()+" produces :");
            logger.info(serializer.next(4));
            logger.info(serializer.next(4));
            logger.info(serializer.next(4));
            logger.info(serializer.next(3));
            resource = new ClassPathResource("/data/edf_format.csv");
            file = resource.getFile();
            serializer = doGetBetterSerializer(file);
            logger.info("Serializer "+serializer.getClass()+" produces :");
            logger.info(serializer.next(15));
            logger.info(serializer.next(15));
            logger.info(serializer.next(15));
            
        }
        catch (Exception e1) {
            e1.printStackTrace();
        }

            
    }
    
    
    
    private IImportSerializer doGetBetterSerializer(File file) {

    	IImportSerializer serializer = null;
        try {
            ImportSerializerFactory factory = TemporalDataApplication.getApplicationConfiguration().getSpringContext().getBean(ImportSerializerFactory.class);
            logger.info("CSV input file : " + file.getAbsolutePath());
            String metric = "testmetric";
            InputStream is;
            
            is = new FileInputStream(file);
            
            Map<String, String> tags = new HashMap<String, String>();
            tags.put("tag1", "valeur1");
            
            serializer = factory.getBetterSerializer(file.getName(), metric, is, tags );
            assertNotNull(serializer);
            
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        return serializer;
        
    }

}

