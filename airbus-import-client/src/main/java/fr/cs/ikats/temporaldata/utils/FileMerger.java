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
 *
 */

package fr.cs.ikats.temporaldata.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Class to merge files into one.
 */
public class FileMerger {

    private final Logger logger = Logger.getLogger(FileMerger.class);

    /**
     * merge files in directory 
     * @param metric name of the metric
     * @param directory the input directory
     * @param targetDirectory the target directory
     * @throws IOException if files cannot be read or written
     */
    public void mergeFiles(String metric, String directory,String targetDirectory) throws IOException {

        File rootDir = new File(directory);

        if (rootDir.exists()) {
            File targetFile = new File(targetDirectory+"/"+"raw_0.csv");
            if (targetFile.exists()) {
                targetFile.delete();
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(targetFile));
            int count = 0;
            try {
                writer.write("TIMESTAMP;" + metric);
                writer.newLine();
                for (File file : rootDir.listFiles()) {

                    BufferedReader reader = new BufferedReader(new FileReader(file));
                    // read the first line
                    try {
                        String line = reader.readLine();
                        while ((line = reader.readLine()) != null) {
                            writer.write(line);
                            writer.newLine();
                            count++;
                        }
                    }
                    finally {
                        reader.close();
                    }
                }
            }
            finally {
                writer.close();
            }
            logger.info(targetFile.getAbsolutePath()+" File written with "+count+" lines");
        }
        else {
            logger.warn("Directory " + directory + " not found or not readable");
        }
    }
    
    
    

}
