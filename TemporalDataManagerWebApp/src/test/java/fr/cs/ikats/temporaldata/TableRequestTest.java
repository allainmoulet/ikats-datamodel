package fr.cs.ikats.temporaldata;

import fr.cs.ikats.datamanager.client.RequestSender;
import fr.cs.ikats.metadata.MetaDataFacade;
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
 */
public class TableRequestTest extends AbstractRequestTest {


    /**
     * test of table creation from a csv file
     * case : nominal (http code 200 returned)
     */
    @Test
    public void testChangeKeyNominal() {
        String testCaseName = "testAddPopMetaNominal";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            File file = getFileMatchingResource(testCaseName, "/data/test_addPopMetaTable.csv");

            getLogger().info("CSV table file : " + file.getAbsolutePath());
            String tableName = "tabletest";
            String url = getAPIURL() + "/table";
            doImport(url, file, "CSV", 200, "funcId", tableName);

            doFuncIdImport("tsuidTest1", "funcidTest1", false, 1);
            doFuncIdImport("tsuidTest2", "funcidTest2", false, 1);
            doFuncIdImport("tsuidTest3", "funcidTest3", false, 1);
            doFuncIdImport("tsuidTest4", "funcidTest4", false, 1);
            launchMetaDataImport("tsuidTest1", "flightId", "1");
            launchMetaDataImport("tsuidTest2", "flightId", "1");
            launchMetaDataImport("tsuidTest3", "flightId", "2");
            launchMetaDataImport("tsuidTest4", "flightId", "2");
            launchMetaDataImport("tsuidTest1", "metric", "M1");
            launchMetaDataImport("tsuidTest2", "metric", "M2");
            launchMetaDataImport("tsuidTest3", "metric", "M1");
            launchMetaDataImport("tsuidTest4", "metric", "M2");

            doChangeKey("tabletest", "metric", "flightId", "outputTableTest", 200);

            endNominal(testCaseName);
        } catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

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

    protected File doGetDataDownload(String tableName) throws IOException {
        Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).register(JacksonFeature.class)
                .build();
        String url = getAPIURL() + "/table/" + tableName;
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

    protected String doChangeKey(String tableName, String metaName, String populationId, String outputTableName, int statusExpected) throws IOException {
        Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).register(JacksonFeature.class)
                .build();
        String url = getAPIURL() + "/table/changekey";
        WebTarget target = client.target(url);

        // build form param
        final FormDataMultiPart multipart = new FormDataMultiPart();

        multipart.field("metaName", metaName);
        multipart.field("populationId", populationId);
        multipart.field("tableName", tableName);
        multipart.field("outputTableName", outputTableName);

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
}
