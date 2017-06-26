package fr.cs.ikats.temporaldata;

import fr.cs.ikats.temporaldata.business.Table;
import fr.cs.ikats.temporaldata.business.TableManager;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.junit.Test;

import javax.validation.constraints.AssertFalse;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * JUNit class testing TableResource services.
 */
public class TableRequestTest extends AbstractRequestTest {


    /**
     * test of table change key use case
     * case : nominal (http code 200 returned)
     */
    @Test
    public void testChangeKeyNominal() {
        String testCaseName = "testChangeKeyNominal";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            File file = getFileMatchingResource(testCaseName, "/data/test_changeKeyTable.csv");

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
            launchMetaDataImport("tsuidTest3", "metric", "M2");
            launchMetaDataImport("tsuidTest4", "metric", "M1");

            doChangeKey("tabletest", "metric", "flightId", "outputTableTest", 200);

            String jsonTable = doGetDataDownload("outputTableTest");
            TableManager tableManager = new TableManager();
            Table table = tableManager.loadFromJson(jsonTable);

            assertEquals(table.headers.col.data, Arrays.asList(null, "M1_B1_OP1",
                    "M1_B2_OP1", "M1_B1_OP2", "M1_B2_OP2", "M2_B1_OP1", "M2_B2_OP1", "M2_B1_OP2", "M2_B2_OP2"));
            assertEquals(table.headers.row.data, Arrays.asList("flightId", "1", "2"));
            assertEquals(table.content.cells.get(0), Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8"));
            assertEquals(table.content.cells.get(1), Arrays.asList("13", "14", "15", "16", "9", "10", "11", "12"));

            endNominal(testCaseName);
        } catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    /**
     * test of table download
     * case : nominal (http code 200 returned)
     */
    @Test
    public void testTableDownload() {
        String testCaseName = "testTableDownload";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            File file = getFileMatchingResource(testCaseName, "/data/test_import_table_nominal.csv");

            getLogger().info("CSV table file : " + file.getAbsolutePath());
            String tableName = "testTableDownload";
            String url = getAPIURL() + "/table";
            doImport(url, file, "CSV", 200, "timestamp", tableName);

            String jsonTable = doGetDataDownload(tableName);
            TableManager tableManager = new TableManager();
            Table table = tableManager.loadFromJson(jsonTable);

            assertEquals(table.headers.col.data, Arrays.asList(null, "timestamp", "value"));
            assertEquals(table.headers.row.data, Arrays.asList(
                    "2015-12-10T14:55:30.5"
                    , "2015-12-10T14:55:31.0"
                    , "2015-12-10T14:55:31.5"
                    , "2015-12-10T14:55:32.0"
                    , "2015-12-10T14:55:23.512"
                    , "2015-12-10T14:56:20.0"
                    , "2015-12-10T14:56:31.5"
                    , "2015-12-10T14:56:33.0"
                    , "2015-12-10T14:56:34.5"
                    , "2015-12-10T14:56:76.0"
                    , "2015-12-10T14:56:37.5"
                    , "2015-12-10T14:56:59.0"
                    , "2015-12-10T14:56:40.5"));

//            assertEquals(table.content.cells.get(0), Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8"));
//            assertEquals(table.content.cells.get(1), Arrays.asList("13", "14", "15", "16", "9", "10", "11", "12"));

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

    protected String doGetDataDownload(String tableName) throws IOException {
        Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).register(JacksonFeature.class)
                .build();
        String url = getAPIURL() + "/table/" + tableName;
        WebTarget target = client.target(url);
        Response response = target.request().get();
        response.bufferEntity();
        ByteArrayInputStream output = (ByteArrayInputStream) response.getEntity();
        String stringJson = convertStreamToString(output);

        return stringJson;
    }

    static String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
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
