/**
 * Copyright 2018-2019 CS Systèmes d'Information
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.cs.ikats.process.data;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.process.data.model.ProcessData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ProcessDataTest {


    @Test
    public void testInsert() throws IOException, IkatsDaoException {
        ProcessDataFacade facade = new ProcessDataFacade();
        InputStream stream = new ByteArrayInputStream("Ceci est le contenu du fichier de test".getBytes());
        ProcessData data = new ProcessData("execId1", "JSON", "Chaine_char");

        facade.importProcessData(data, stream, -1);

        List<ProcessData> result = facade.getProcessData("execId1");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("JSON", result.get(0).getDataType());

        String resultData = getDataFromResult(result.get(0));

        assertNotNull(resultData);
        assertEquals("Ceci est le contenu du fichier de test", resultData);
        facade.removeProcessData("execId1");

    }


    /**
     * Test the persistence of a string to database
     */
    @Test
    public void testInsertAny() throws IkatsDaoException {
        ProcessDataFacade facade = new ProcessDataFacade();

        // Random bytes generation
        SecureRandom random = new SecureRandom();
        byte[] dataToInsert = new byte[200];
        random.nextBytes(dataToInsert);

        ProcessData data = new ProcessData("execId1", "ANY", "test_pdata1");

        facade.importProcessData(data, dataToInsert);

        List<ProcessData> result = facade.getProcessData("execId1");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("ANY", result.get(0).getDataType());

        byte[] resultData = result.get(0).getData();
        assertNotNull(resultData);
        assertTrue(Arrays.equals(dataToInsert, resultData));
        facade.removeProcessData("execId1");
    }

    /**
     * @param processData
     * @return
     */
    private String getDataFromResult(ProcessData processData) {
        return new String(processData.getData(), Charset.defaultCharset());
    }

    @Test
    public void testGetForProcessId() throws IOException, IkatsDaoException {
        ProcessDataFacade facade = new ProcessDataFacade();
        InputStream stream = new ByteArrayInputStream("Ceci est le contenu du fichier de test".getBytes());
        InputStream stream2 = new ByteArrayInputStream("Ceci est le contenu du fichier de test 1".getBytes());
        InputStream stream3 = new ByteArrayInputStream("Ceci est le contenu du fichier de test 2".getBytes());


        ProcessData data = new ProcessData("execId1", "JSON", "Chaine_char");
        ProcessData data2 = new ProcessData("execId1", "JSON", "Chaine_char");
        ProcessData data3 = new ProcessData("execId1", "JSON", "Chaine_char");
        facade.importProcessData(data, stream, -1);
        facade.importProcessData(data2, stream2, -1);
        facade.importProcessData(data3, stream3, -1);

        List<ProcessData> result = facade.getProcessData("execId1");
        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals("Ceci est le contenu du fichier de test", getDataFromResult(result.get(0)));
        assertEquals("Ceci est le contenu du fichier de test 1", getDataFromResult(result.get(1)));
        assertEquals("Ceci est le contenu du fichier de test 2", getDataFromResult(result.get(2)));

        facade.removeProcessData("execId1");

    }

    @Test
    public void testGetWithFileContent() throws IOException, IkatsDaoException {
        ProcessDataFacade facade = new ProcessDataFacade();
        Resource resource = new ClassPathResource("/data/matrice_distance.csv");
        File file = resource.getFile();
        FileInputStream stream = new FileInputStream(file);
        ProcessData data = new ProcessData("execId1", "CSV", "matrice_distance.csv");
        facade.importProcessData(data, stream, -1);
        List<ProcessData> result = facade.getProcessData("execId1");
        int dataId = result.get(0).getId();
        ProcessData result1 = facade.getProcessPieceOfData(dataId);
        facade.removeProcessData("execId1");
    }

    @Test
    public void testDelete() throws IOException, IkatsDaoException {
        ProcessDataFacade facade = new ProcessDataFacade();
        InputStream stream = new ByteArrayInputStream("Ceci est le contenu du fichier de test".getBytes());
        ProcessData data = new ProcessData("execId1", "JSON", "Chaine_char");
        facade.importProcessData(data, stream, -1);
        facade.removeProcessData("execId1");
        List<ProcessData> result = facade.getProcessData("execId1");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetById() throws IOException, IkatsDaoException {
        ProcessDataFacade facade = new ProcessDataFacade();
        InputStream stream = new ByteArrayInputStream("Ceci est le contenu du fichier de test".getBytes());
        ProcessData data = new ProcessData("execId1", "JSON", "Chaine_char");
        String id = facade.importProcessData(data, stream, -1);
        ProcessData result = facade.getProcessPieceOfData(Integer.parseInt(id));
        assertNotNull(result);
        assertEquals(new Integer(id), result.getId());
        assertEquals(data.getProcessId(), result.getProcessId());
        assertEquals(data.getDataType(), result.getDataType());
        assertEquals("Ceci est le contenu du fichier de test", getDataFromResult(result));
        facade.removeProcessData("execId1");
    }
}
