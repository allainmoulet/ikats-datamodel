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

package fr.cs.ikats.temporaldata.airbus;

import java.io.File;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.junit.Test;

import fr.cs.ikats.temporaldata.ImportRequestTest;
import fr.cs.ikats.temporaldata.utils.Chronometer;

/**
 *
 */
public class ImportRequestTestAirbus extends ImportRequestTest {

    private static Logger logger = Logger.getLogger(ImportRequestTest.class);

    @Test
    public void importNewAPIForAirbusData() {
        Chronometer chrono = new Chronometer("import", true);
        int count = 0;
        for (int j = 0; j < 6; j++) {
            for (int i = 0; i < 2000; i++) {
                File file = new File("/home/ikats/DEV/data/AIRBUS/DATASET_1/DAR/A320001/WS" + j + "/raw_" + i + ".csv");
                if (file.exists()) {
                    launchImportRequestForAirbusFile(new File("/home/ikats/DEV/data/AIRBUS"), "DATASET_1/DAR/A320001/WS" + j + "/raw_" + i + ".csv");
                    count++;
                }
            }
        }
        logger.info(count + " files imported");
        chrono.stop(logger);
    }

    @Test
    public void importOneFileNewAPIForAirbusData() {
        importOneFileNewAPIForAirbusData("WS7", 1, 1);
    }

    @Test
    public void importTenFilesNewAPIForAirbusData() {
        importOneFileNewAPIForAirbusData("WS7", 2, 11);
    }

    @Test
    public void importThousandFilesNewAPIForAirbusData() {
        importOneFileNewAPIForAirbusData("WS7", 1, 2000);
    }

    private void importOneFileNewAPIForAirbusData(String METRIC, int fileIndexStart, int fileIndexStop) {
        Chronometer chrono = new Chronometer("import", true);
        for (int i = fileIndexStart; i <= fileIndexStop; i++) {
            File file = new File("/home/ikats/DEV/data/AIRBUS/DATASET_1/DAR/A320001/" + METRIC + "/raw_" + i + ".csv");
            if (file.exists()) {
                launchImportRequestForAirbusFile(new File("/home/ikats/DEV/data/AIRBUS"), "DATASET_1/DAR/A320001/" + METRIC + "/raw_" + i + ".csv");
            } else {
                logger.info("fichier non trouve : " + file.getAbsolutePath());
            }
        }
        logger.info((fileIndexStop - fileIndexStart + 1) + " files imported");
        chrono.stop(logger);
    }

    private void launchImportRequestForAirbusFile(File rootDirectory, String relativePath) {
        String[] subdirs = StringUtils.splitPreserveAllTokens(relativePath, "/");

        String dataset = subdirs[0];
        String aircraftId = subdirs[2];
        String metric = subdirs[3];
        String flightId = subdirs[4].substring("raw_".length(), subdirs[4].lastIndexOf(".csv"));

        String url = "http://localhost:8180/myapp/import/put/" + dataset + "/" + metric;
        File file = new File(rootDirectory, relativePath);
        FileDataBodyPart bodyPart = new FileDataBodyPart("file", file);
        final FormDataMultiPart multipart = new FormDataMultiPart();
        multipart.bodyPart(bodyPart);
        Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();
        WebTarget target = client.target(url).queryParam("aircraftIdentifier", aircraftId).queryParam("flightIdentifier", flightId);
        logger.info("sending url : " + url);
        Response response = target.request().post(Entity.entity(multipart, multipart.getMediaType()));
        logger.info("parsing response of " + url);
        logger.info(response);
    }
}
