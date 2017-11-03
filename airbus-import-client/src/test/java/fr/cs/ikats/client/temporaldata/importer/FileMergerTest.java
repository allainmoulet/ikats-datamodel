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

package fr.cs.ikats.client.temporaldata.importer;

import java.io.IOException;

import fr.cs.ikats.temporaldata.utils.FileMerger;

/**
 * Test pour le fileMerger
 */
public class FileMergerTest {

    /**
     * Test method for {@link FileMerger#mergeFiles(String, String, String)}
     */
    public void testMergeFiles() {
        FileMerger merger = new FileMerger();
        try {
            merger.mergeFiles("WS1", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/WS1","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/WS1/");
            merger.mergeFiles("WS2", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/WS2","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/WS2/");
            merger.mergeFiles("WS3", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/WS3","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/WS3/");
            merger.mergeFiles("WS4", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/WS4","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/WS4/");
            merger.mergeFiles("WS5", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/WS5","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/WS5/");
            merger.mergeFiles("WS6", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/WS6","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/WS6/");
            merger.mergeFiles("WS7", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/WS7","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/WS7/");
            merger.mergeFiles("WS8", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/WS8","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/WS8/");
            merger.mergeFiles("BRK_PRESS_1", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/BRK_PRESS_1","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/BRK_PRESS_1/");
            merger.mergeFiles("BRK_PRESS_2", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/BRK_PRESS_2","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/BRK_PRESS_2/");
            merger.mergeFiles("BRK_PRESS_3", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/BRK_PRESS_3","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/BRK_PRESS_3/");
            merger.mergeFiles("BRK_PRESS_4", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/BRK_PRESS_4","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/BRK_PRESS_4/");
            merger.mergeFiles("BRK_PRESS_5", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/BRK_PRESS_5","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/BRK_PRESS_5/");
            merger.mergeFiles("BRK_PRESS_6", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/BRK_PRESS_6","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/BRK_PRESS_6/");
            merger.mergeFiles("BRK_PRESS_7", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/BRK_PRESS_7","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/BRK_PRESS_7/");
            merger.mergeFiles("BRK_PRESS_8", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/BRK_PRESS_8","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/BRK_PRESS_8/");
            merger.mergeFiles("N1_TARGET", "/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A320200/N1_TARGET","/home/pcazes/TRAVAIL/IKATS/DEV/data/AIRBUS/DATASET_BIG_TS/DAR/A380099/N1_TARGET/");
            
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

}

