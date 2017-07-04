package fr.cs.ikats.temporaldata;

import fr.cs.ikats.temporaldata.business.Table;
import fr.cs.ikats.temporaldata.business.TableInfo;
import fr.cs.ikats.temporaldata.business.TableManager;

import fr.cs.ikats.temporaldata.resource.TableResource;
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
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * JUNit class testing TableResource services.
 */
public class TableRequestTest extends AbstractRequestTest {


    /**
     *
     * case : nominal (http code 200 returned)
     */
    @Test
    public void testTrainTsSplitNominal() {
        String testCaseName = "testTrainTestSplitNominal";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            String jsonTableIn="{}";
            String tableContent = "MainId;Target\n"
                    + "1;A\n"
                    + "1;A\n"
                    + "2;B\n"
                    + "3;C\n"
                    + "4;D\n"
                    + "42;A\n"
                    + "6;B\n"
                    + "7;C\n"
                    + "8;D\n";

            TableManager tableManager = new TableManager();
            TableInfo tableIn = tableManager.loadFromJson(jsonTableIn);

            TableResource tableResource= new TableResource();
            tableResource.trainTestSplit(tableManager.serializeToJson(tableIn), "target");

            String jsonTableOut = doGetDataDownload("outputTableTest");
            TableInfo tableOut = tableManager.loadFromJson(jsonTableOut);

            assertEquals(Arrays.asList(null,
                    "M1_B1_OP1", "M1_B2_OP1", "M1_B1_OP2", "M1_B2_OP2",
                    "M2_B1_OP1", "M2_B2_OP1", "M2_B1_OP2", "M2_B2_OP2"), tableOut.headers.col.data);
            assertEquals(Arrays.asList("flightId", "1", "2"), tableOut.headers.row.data);
            assertEquals(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8"), tableOut.content.cells.get(0));
            assertEquals(Arrays.asList("13", "14", "15", "16", "9", "10", "11", "12"), tableOut.content.cells.get(1));

            endNominal(testCaseName);
        } catch (Throwable e) {
            endWithFailure(testCaseName, e);
        }
    }

    /**
     * test of table ts2feature use case
     * case : nominal (http code 200 returned)
     */
    @Test
    public void testTs2FeatureNominal() {
        String testCaseName = "testTs2FeatureNominal";
        boolean isNominal = true;
        try {
            start(testCaseName, isNominal);

            File file = getFileMatchingResource(testCaseName, "/data/test_ts2FeatureTable.csv");

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

            String jsonTableIn = doGetDataDownload(tableName);
            TableManager tableManager = new TableManager();
            TableInfo tableIn = tableManager.loadFromJson(jsonTableIn);

            System.out.println( "IN ...");
            System.out.println( tableManager.serializeToJson(tableIn) );
            
            doTs2Feature(tableManager.serializeToJson(tableIn), "metric", "flightId", "outputTableTest", 200);

            String jsonTableOut = doGetDataDownload("outputTableTest");
            TableInfo tableOut = tableManager.loadFromJson(jsonTableOut);

            System.out.println( "OUT ...");
            System.out.println( tableManager.serializeToJson(tableOut) );
            
            assertEquals(Arrays.asList(null,
                    "M1_B1_OP1", "M1_B2_OP1", "M1_B1_OP2", "M1_B2_OP2",
                    "M2_B1_OP1", "M2_B2_OP1", "M2_B1_OP2", "M2_B2_OP2"), tableOut.headers.col.data);
            assertEquals(Arrays.asList("flightId", "1", "2"), tableOut.headers.row.data);
            assertEquals(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8"), tableOut.content.cells.get(0));
            assertEquals(Arrays.asList("13", "14", "15", "16", "9", "10", "11", "12"), tableOut.content.cells.get(1));

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
            TableInfo tableInfo = tableManager.loadFromJson(jsonTable);
            Table table = tableManager.initTable(tableInfo, false);

            assertEquals(Arrays.asList("timestamp", "value"), table.getColumnsHeader().getItems(String.class));
            assertEquals(Arrays.asList(null,
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
                    , "2015-12-10T14:56:40.5"), table.getRowsHeader().getItems(String.class));

            assertEquals(Arrays.asList("6"
                    , "3"
                    , "2"
                    , "5"
                    , "8"
                    , "5"
                    , "6"
                    , "8"
                    , "5"
                    , "2"
                    , "6"
                    , "9"
                    , "2"), table.getColumn(1, String.class));

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
        boolean isNominal = false;
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
        boolean isNominal = false;
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
        boolean isNominal = false;
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
        boolean isNominal = false;
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

        return convertStreamToString(output);
    }

    private String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }

    private String doTs2Feature(String tableJson, String metaName, String populationId, String outputTableName, int statusExpected) throws IOException {
        Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).register(JacksonFeature.class)
                .build();
        String url = getAPIURL() + "/table/ts2feature";
        WebTarget target = client.target(url);

        // build form param
        final FormDataMultiPart multipart = new FormDataMultiPart();

        multipart.field("metaName", metaName);
        multipart.field("populationId", populationId);
        multipart.field("tableJson", tableJson);
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
