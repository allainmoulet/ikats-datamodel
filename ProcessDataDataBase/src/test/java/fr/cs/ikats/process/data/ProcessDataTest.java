package fr.cs.ikats.process.data;

import fr.cs.ikats.process.data.model.ProcessData;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.*;

public class ProcessDataTest {


    @Test
    public void testInsert() throws IOException {
        ProcessDataFacade facade = new ProcessDataFacade();
        InputStream stream = new ByteArrayInputStream("Ceci est le contenu du fichier de test".getBytes());
        ProcessData data = new ProcessData("execId1", "JSON", "Chaine_char");

        facade.importProcessData(data, stream, -1);

        List<ProcessData> result = facade.getProcessData("execId1");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("JSON", result.get(0).getDataType());

        String resultData = getDataFromResult(result.get(0));

        System.out.println("BLOB content : ");
        System.out.println(resultData);
        System.out.println("END OB BLOB content : ");
        assertNotNull(resultData);
        assertEquals("Ceci est le contenu du fichier de test", resultData);
        facade.removeProcessData("execId1");

    }


    /**
     * Test the persistence of a string to database
     */
    @Test
    public void testInsertAny() {
        ProcessDataFacade facade = new ProcessDataFacade();

        String dataToInsert = "This is a content to store";
        ProcessData data = new ProcessData("execId1", "ANY", "test_pdata");

        facade.importProcessData(data, dataToInsert);

        List<ProcessData> result = facade.getProcessData("execId1");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("ANY", result.get(0).getDataType());

        String resultData = getDataFromResult(result.get(0));

        System.out.println("BLOB content : ");
        System.out.println(resultData);
        System.out.println("END OB BLOB content : ");
        assertNotNull(resultData);
        assertEquals(dataToInsert, resultData);
        facade.removeProcessData("execId1");
    }

    /**
     * @param result
     * @return
     */
    private String getDataFromResult(ProcessData processData) {
        String resultData = null;
        InputStream inS;
        try {
            inS = processData.getData().getBinaryStream();
            byte[] buff = new byte[512];
            int read = 0;
            StringBuffer strBuff = new StringBuffer();

            while ((read = inS.read(buff)) != -1) {
                strBuff.append(new String(buff, Charset.defaultCharset()));
            }
            resultData = strBuff.toString().trim();
        } catch (SQLException e1) {
            e1.printStackTrace();
            fail();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        return resultData;
    }


    @Test
    // Review#156651 ajout du throw IOException pour changement signature de ProcessDataFacade.importProcessData(ProcessData, InputStream, int) 
    public void testGetForProcessId() throws IOException {
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
    public void testGetWithFileContent() {
        ProcessDataFacade facade = new ProcessDataFacade();
        Resource resource = new ClassPathResource("/data/matrice_distance.csv");
        File file = null;
        try {
            file = resource.getFile();
            FileInputStream stream = new FileInputStream(file);
            ProcessData data = new ProcessData("execId1", "CSV", "matrice_distance.csv");
            facade.importProcessData(data, stream, -1);
            List<ProcessData> result = facade.getProcessData("execId1");
            System.out.println(getDataFromResult(result.get(0)));
            int dataId = result.get(0).getId();
            ProcessData result1 = facade.getProcessPieceOfData(dataId);
            System.out.println(getDataFromResult(result1));
            facade.removeProcessData("execId1");

        } catch (IOException e1) {
            e1.printStackTrace();
        }

    }

    @Test
    public void testDelete() throws IOException {
        ProcessDataFacade facade = new ProcessDataFacade();
        InputStream stream = new ByteArrayInputStream("Ceci est le contenu du fichier de test".getBytes());
        ProcessData data = new ProcessData("execId1", "JSON", "Chaine_char");
        facade.importProcessData(data, stream, -1);
        facade.removeProcessData("execId1");
        List<ProcessData> result = facade.getProcessData("execId1");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetById() throws IOException {
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
