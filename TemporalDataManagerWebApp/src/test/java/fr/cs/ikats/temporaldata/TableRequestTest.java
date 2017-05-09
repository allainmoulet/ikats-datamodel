package fr.cs.ikats.temporaldata;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.*;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;

/**
 * JUNit class testing TableResource services.
 * <p>
 * Review:MBD:156259 il manquerait un test dégradé: quand nombre de champs d'une ligne du csv n'est pas constant => erreur
 */
public class TableRequestTest extends AbstractRequestTest {


    /**
     * test of table creation from a csv file
     * case : nominal (http code 200 returned)
     */
    @Test
    public void testImportTablefromCSVFileNominal() {
        String testCaseName = "testImportTablefromCSVFile";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            File file = getFileMatchingResource(testCaseName, "/data/test_import_table_nominal.csv");

            getLogger().info("CSV table file : " + file.getAbsolutePath());
            String tableName = "tableTestNominal";
            String url = getAPIURL() + "/table";
            doImport(url, file, "CSV", 200, "timestamp", tableName);

            endNominal(testCaseName);
        } catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    /**
     * test of table creation from a csv file
     * case : table name provided with illegal characters (http code 406 returned)
     */
    @Test
    public void testImportTableIncorrectName() {
        String testCaseName = "testImportTablefromCSVFile";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            File file = getFileMatchingResource(testCaseName, "/data/test_import_table_nominal.csv");

            getLogger().info("CSV table file : " + file.getAbsolutePath());
            String tableName = "TableIncorrectName%";
            String url = getAPIURL() + "/table";
            doImport(url, file, "CSV", 406, "timestamp", tableName);

            endNominal(testCaseName);
        } catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    /**
     * test of table creation from a csv file
     * case : table already exists (http code 409 returned)
     */
    @Test
    public void testImportTableAlreadyExists() {

        String testCaseName = "testImportTablefromCSVFile";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            File file = getFileMatchingResource(testCaseName, "/data/test_import_table_nominal.csv");

            getLogger().info("CSV table file : " + file.getAbsolutePath());
            String tableName = "tableTestAlreadyExists";
            String url = getAPIURL() + "/table";

            // fist import : status 200 (ok)
            doImport(url, file, "CSV", 200, "timestamp", tableName);

            // second import : status 409 (conflict)
            doImport(url, file, "CSV", 409, "timestamp", tableName);

            endNominal(testCaseName);
        } catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }


    /**
     * test of table creation from a csv file
     * case : table contains doubloons (http code 400 returned)
     */
    @Test
    public void testImportTablefromCSVFileWithDoubloon() {

        String testCaseName = "testImportTablefromCSVFile";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            File file = getFileMatchingResource(testCaseName, "/data/test_import_table_doublon.csv");

            getLogger().info("CSV table file : " + file.getAbsolutePath());
            String tableName = "tableTestDoublon";
            String url = getAPIURL() + "/table";
            doImport(url, file, "CSV", 400, "timestamp", tableName);

            endNominal(testCaseName);
        } catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    /**
     * test of table creation from a csv file
     * case : table contains incorrect line length (http code 400 returned)
     */
    @Test
    public void testImportTablefromIncorrectCSVFile() {

        String testCaseName = "testImportTablefromIncorrectCSVFile";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            File file = getFileMatchingResource(testCaseName, "/data/test_import_table_incorrect_line_length.csv");

            getLogger().info("CSV table file : " + file.getAbsolutePath());
            String tableName = "TableTestIncorrectCSVFile";
            String url = getAPIURL() + "/table";
            doImport(url, file, "CSV", 400, "timestamp", tableName);

            endNominal(testCaseName);
        } catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    protected String doImport(String url, File file, String dataType, int statusExpected, String rowName, String tableName) {
        Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).register(JacksonFeature.class)
                .build();
        WebTarget target = client.target(url);

        // build form param
        final FormDataMultiPart multipart = new FormDataMultiPart();

        FileDataBodyPart fileBodyPart = new FileDataBodyPart("file", file);
        multipart.bodyPart(fileBodyPart);
        multipart.field("fileType", dataType);
        multipart.field("rowName", rowName);
        multipart.field("tableName", tableName);

        getLogger().info("sending url : " + url);
        Response response = target.request().post(Entity.entity(multipart, multipart.getMediaType()));
        getLogger().info("parsing response of " + url);
        getLogger().info(response);
        int status = response.getStatus();
        String result = response.readEntity(String.class);
        getLogger().info(result);
        // check expected status
        assertEquals(statusExpected, status);
        return result;
    }

    protected File doGetDataDownload(String id) throws IOException {
        Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).register(JacksonFeature.class)
                .build();
        String url = getAPIURL() + "/processdata/id/download/" + id;
        WebTarget target = client.target(url);
        Response response = target.request().get();
        response.bufferEntity();
        ByteArrayInputStream output = (ByteArrayInputStream) response.getEntity();
        File outputFile = File.createTempFile("ikats", "dogetTestResult.csv");
        outputFile.deleteOnExit();
        FileWriter fos = new FileWriter(outputFile);
        try {
            byte[] buff = new byte[512];
            while ((output.read(buff)) != -1) {
                fos.write(new String(buff, Charset.defaultCharset()));
            }
        } finally {
            fos.close();
        }

        getLogger().info("Result written in file " + outputFile.getAbsolutePath());
        return outputFile;
    }

}
