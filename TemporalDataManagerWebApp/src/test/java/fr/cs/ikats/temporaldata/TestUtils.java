package fr.cs.ikats.temporaldata;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.json.simple.JSONObject;

import com.fasterxml.jackson.databind.util.ISO8601DateFormat;

import fr.cs.ikats.datamanager.client.RequestSender;
import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.datamanager.client.opentsdb.QueryResult;
import fr.cs.ikats.datamanager.client.opentsdb.ResponseParser;
import fr.cs.ikats.temporaldata.exception.IkatsException;

public class TestUtils {

    private static final Logger LOGGER = Logger.getLogger(TestUtils.class);
    final public static DateFormat AIRBUS_RAW_CSV_DATA_FORMAT = getDateFormat();
    static String metriquesInt[] = { "poc3.arctan", "poc3.cosinus", "poc3.sinusoide" };
    static String metriquesDev[] = { "poc.cosinus.lineaire", "poc3.cosinus", "poc3.sinusoide" };
    static String HOST_DEV = "172.28.0.56";
    static String HOST_INT = "172.28.15.81";

    static String TAG1_K = "tag1";
    static String TAG1_V = "val1";
    static String TAG2_K = "tag2";
    static String TAG2_V = Long.toString(System.currentTimeMillis());

    Random myRandom;

    final private static DateFormat getDateFormat() {
        return new DateFormat() {

            /**
             * 
             */
            private static final long serialVersionUID = 7495728837748985475L;

            @Override
            public Date parse(String source, ParsePosition pos) {
                Date date = null;
                try {
                    DateFormat format = new ISO8601DateFormat();
                    date = format.parse(source, pos);
                    if (date == null) {
                        format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                        date = format.parse(source, pos);
                    }
                    if (date == null) {
                        format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                        date = format.parse(source, pos);
                    }
                    if (date == null) {
                        format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S");
                        date = format.parse(source, pos);
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                return date;
            }

            @Override
            public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
                DateFormat format = new ISO8601DateFormat();
                return format.format(date, toAppendTo, fieldPosition);
            }
        };
    }
    
    public TestUtils() {
        myRandom = new Random();
    }

    public String getRandomMetric(String host) {
        String result = "poc.sinusoide";
        if (host.equals(HOST_DEV)) {
            result = metriquesDev[myRandom.nextInt(metriquesDev.length - 1)];
        }
        else if (host.equals(HOST_INT)) {
            result = metriquesInt[myRandom.nextInt(metriquesInt.length - 1)];
        }
        return result;

    }

    public String getMetric(String host, int idx) {
        String result = "poc.sinusoide";
        if (host.equals(HOST_DEV)) {
            result = metriquesDev[idx];
        }
        else if (host.equals(HOST_INT)) {
            result = metriquesInt[idx];
        }
        return result;
    }

    public QueryResult launchNewAPISearchRequest(String metrique, String startDate, String endDate, String tags, String aggregator,
            String downsampler, String downsamplerperiod, String options, boolean additionnalInformation) throws IkatsWebClientException {
        StringBuilder sb = new StringBuilder(AbstractRequestTest.getAPIURL() + "/ts/extract/metric/" + metrique);
        sb.append("?");
        sb.append("ag=").append(aggregator);
        if (startDate != null) {
            sb.append("&sd=" + startDate);
        }
        if (endDate != null) {
            sb.append("&ed=" + endDate);
        }

        if (tags != null) {
            sb.append("&t=" + tags);
        }
        if (options != null) {
            sb.append("&o=" + options);
        }
        if (downsampler != null) {
            sb.append("&ds=" + downsampler + "&dp=" + downsamplerperiod);
        }
        if (additionnalInformation) {
            sb.append("&di=" + additionnalInformation);
        }
        QueryResult resultat = null;
        try {
            String url = sb.toString();
            LOGGER.info("NEW : sending request : " + url);
            Response response = RequestSender.sendGETRequest(url, "172.28.0.56");
            LOGGER.info("NEW : parsing response of " + url);
            resultat = ResponseParser.parseQueryReponse(response, true);
            LOGGER.info(resultat.afficheMeta());
        }
        catch (IkatsWebClientException e) {
            LOGGER.error("", e);
            throw e;
        }
        return resultat;

    }

    public QueryResult launchNewAPISearchTSUIDRequest(List<String> tsuid, String startDate, String endDate, String aggregator, String options) {
        StringBuilder sb = new StringBuilder(AbstractRequestTest.getAPIURL() + "/ts/extract/tsuid/");
        sb.append("?");
        if (aggregator != null) {
            sb.append("ag=").append(aggregator);
        }
        if (startDate != null) {
            sb.append("&sd=" + startDate);

        }
        if (endDate != null) {
            sb.append("&ed=" + endDate);
        }
        sb.append("&o=show_tsuids");
        sb.append("&tsuid=");
        for (Iterator<String> iter = tsuid.iterator(); iter.hasNext();) {
            String uid = iter.next();
            sb.append(uid);
            if (iter.hasNext()) {
                sb.append(",");
            }
        }
        QueryResult resultat = null;
        try {
            String url = sb.toString();
            LOGGER.info("NEW : sending request : " + url);
            Response response = RequestSender.sendGETRequest(url, "172.28.0.56");
            LOGGER.info("NEW : parsing response of " + url);
            resultat = ResponseParser.parseQueryReponse(response, true);
            LOGGER.info(resultat.afficheMeta());
        }
        catch (IkatsWebClientException e) {
            LOGGER.error("", e);
        }
        return resultat;
    }

    public QueryResult launchNativeAPISearchRequest(String metrique) {
        // String url =
        String url = "http://172.28.15.81:4242/api/query?start=2015/03/01-00:00:00&m=sum:" + metrique + "%7Bnumero=00001%7D&show_tsuids";
        Response response = null;
        QueryResult resultat = null;
        try {
            LOGGER.info("NATIVE : sending request " + url);
            response = RequestSender.sendGETRequest(url, "172.28.0.56");
            LOGGER.info("NATIVE : parsing response of " + url);
            resultat = ResponseParser.parseQueryReponse(response, true);
            LOGGER.info(resultat.afficheMeta());
        }
        catch (IkatsWebClientException e) {
            LOGGER.error("", e);
        }
        return resultat;
    }

    public Client getClientWithJSONFeature() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(MultiPartFeature.class).register(JacksonFeature.class);
        clientConfig.register(LoggingFilter.class);
        JerseyClient client = JerseyClientBuilder.createClient(clientConfig);
        return client;
    }

    public Response sendGetRequest(String url, String host) {
        Client client = ClientBuilder.newBuilder().build();
        WebTarget target = client.target(url);
        LOGGER.info("sending url : " + url);
        Response response = target.request().get();
        return response;
    }

    /**
     * send GET request and return JSON format response
     * 
     * @param url
     * @param host
     * @return
     * @throws IkatsWebClientException
     */
    public Response sendGETRequest(String mediaType, Client client, String url, String host) throws IkatsWebClientException {
        LOGGER.debug("Sending GET request to url : " + url);
        Response response = null;
        try {
            WebTarget target = client.target(url);
            if (mediaType != null) {
                response = target.request(mediaType).get();
            }
            else {
                response = target.request().get();
            }
        }
        catch (Exception e) {
            LOGGER.error("", e);
        }
        return response;
    }

    public Response sendDeleteRequest(Client client, String url) throws IkatsWebClientException {
        LOGGER.debug("Sending DELETE request to url : " + url);
        Response response = null;
        try {
            WebTarget target = client.target(url);
            response = target.request().delete();
        }
        catch (Exception e) {
            LOGGER.error("", e);
        }
        return response;
    }

    /**
     * send GET request and return JSON format response
     * 
     * @param url
     * @param host
     * @return
     * @throws IkatsWebClientException
     */
    public Response sendPUTRequest(Entity<?> entity, String mediaType, Client client, String url, String host) throws IkatsWebClientException {
        LOGGER.debug("Sending PUT request to url : " + url);
        Response response = null;
        try {
            WebTarget target = client.target(url);
            if (mediaType != null) {
                response = target.request(mediaType).put(entity);
            }
            else {
                response = target.request().put(entity);
            }
        }
        catch (Exception e) {
            LOGGER.error("", e);
        }
        return response;
    }

    /**
     * @see TestUtils#doLaunch(File, String, boolean, int, boolean) with
     *      addFuncId set to true by default
     */
    protected String doImport(File file, String url, boolean withTags, int statusExpected) {
        return doImport(file, url, withTags, statusExpected, true);
    }

    /**
     * @param file
     *            : the file to import
     * @param url
     *            : url to reach
     * @param withTags
     *            : add the tags
     * @param statusExpected
     *            : the expected return status
     * @param addFuncId
     *            : true to add the funcId part into request
     */
    protected String doImport(File file, String url, boolean withTags, int statusExpected, boolean addFuncId) {
        Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).register(JacksonFeature.class).build();
        WebTarget target = client.target(url);

        // build form param
        final FormDataMultiPart multipart = new FormDataMultiPart();

        FileDataBodyPart fileBodyPart = new FileDataBodyPart("file", file);
        multipart.bodyPart(fileBodyPart);
        if (withTags) {
            multipart.field(TAG1_K, TAG1_V);
            multipart.field(TAG2_K, TAG2_V);
        }
        if (addFuncId) {
            // add the multipart field funcId, required
            multipart.field("funcId", "FuncID_" + Long.toString(System.currentTimeMillis()));
        }
        LOGGER.info("sending url : " + url);
        Response response = target.request().post(Entity.entity(multipart, multipart.getMediaType()));
        LOGGER.info("parsing response of " + url);
        LOGGER.info(response);
        int status = response.getStatus();
        String result = response.readEntity(String.class);
        LOGGER.info(result);
        // check expected status
        assertEquals(statusExpected, status);

        return result;
    }

    /**
     * 
     * @param tsuid
     *            the tsuid
     * @param startDate
     *            the start date
     * @param endDate
     *            the end date
     * @return the JSON representation of TS data
     * @throws Exception
     *             if error occurs
     */
    public String getTSFromFile(String filePath, String startDate, String endDate, String metric) throws Exception {
        String response;
        JSONObject jo = new JSONObject();
        jo.put("metric", metric);
        jo.put("tags", "");
        jo.put("aggregateTags", "");
        jo.put("dps", ExtractCSVDataToJson(filePath, startDate, endDate));
        response = "[" + jo.toJSONString() + "]";
        return response;
    }

    public final static JSONObject ExtractCSVDataToJson(String filePath, String startDate, String endDate) throws IkatsException {

        Long start = Long.valueOf(startDate).longValue();
        Long end = Long.valueOf(endDate).longValue();
        JSONObject dps = new JSONObject();
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(filePath));

            // read the first line because it is the header line
            String currentLine = reader.readLine();
            long lineNumber = 1;

            Long currentDate = -1l;
            boolean out_of_range = false;

            // Number will be either Long or Double ... according to the parsed
            // value
            Number currentValue = null;

            long nbPointsEvaluated = 0;
            long nbsuccess = 0;

            while (currentLine != null) {
                boolean hasParsedCurrentPointWithSuccess = true;
                currentLine = reader.readLine();
                lineNumber++;
                if ((currentLine != null) && (currentLine.trim().length() > 0)) {
                    nbPointsEvaluated++;
                    StringTokenizer fields = new StringTokenizer(currentLine, ";");
                    String rawDate = fields.nextToken();
                    String rawValue = fields.nextToken();

                    // Parse the date ...
                    try {
                        currentDate = AIRBUS_RAW_CSV_DATA_FORMAT.parse(rawDate).getTime();
                        if ((currentDate < start) || (currentDate > end)) {
                            out_of_range = true;
                        }
                        else {
                            out_of_range = false;
                        }
                    }
                    catch (Throwable e) {
                        String msg = "Error parsing date field from CSV at line=" + lineNumber + " date=" + rawDate;
                        hasParsedCurrentPointWithSuccess = false;
                        LOGGER.error(msg, e);
                    }

                    // Parse the value: either a long or a double ...
                    // => deal with Long
                    try {
                        // pass previous value currentValue to the method: will
                        // determine the parsed type after
                        // the first point
                        currentValue = parseValue(rawValue, currentValue);
                    }
                    catch (IkatsException e) {
                        String msg = "Error parsing value field from CSV at line=" + lineNumber + " value=" + rawValue;
                        hasParsedCurrentPointWithSuccess = false;
                        LOGGER.error(msg, e);
                    }

                    // update statistics

                    if (hasParsedCurrentPointWithSuccess) {
                        nbsuccess++; // nb of points successfully parsed
                        if (!out_of_range) {
                            String currentTimeStamp = currentDate.toString();
                            dps.put(currentTimeStamp, currentValue);
                        }
                    }
                }
                else if (currentLine == null) // EOF only
                {
                    LOGGER.info("<<<< extracted CSV: filename=" + filePath);
                    LOGGER.info("  - nb lines=" + lineNumber);
                    LOGGER.info("  - evaluated points=" + nbPointsEvaluated);
                    LOGGER.info("  - nb point successfully parsed=" + nbsuccess);
                    LOGGER.info("  - nb point kept (in range)=" + dps.size());
                    LOGGER.info("extracted CSV: filename=" + filePath + ">>>>");
                    if (dps.size() == 0) {
                        dps.put("WARNING", "--- aucune donnee extraite ---");
                    }
                }
            }
        }
        catch (Throwable e) {
            LOGGER.error("Caught throwable while parsing CSV", e);
            dps.put("error", e.getMessage());
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (Throwable e) {
                    // Do not try more
                }
            }
        }

        return dps;
    }

    private static Number parseValue(String rawValue, Number previousValue) throws IkatsException {
        Number parsedValue = null;
        if (previousValue == null) {
            try {
                // attempt from Long
                parsedValue = Long.parseLong(rawValue);

            }
            catch (NumberFormatException e) {
                try {
                    // attempt from Double
                    parsedValue = Double.parseDouble(rawValue);
                }
                catch (NumberFormatException e2) {
                    throw new IkatsException("Failed to parse the value from CSV: first value");
                }
            }
        }
        else // once previousValue is defined: keep same type of Number
        {
            try {
                if (previousValue instanceof Long) {
                    parsedValue = Long.parseLong(rawValue);
                }
                else if (previousValue instanceof Double) {
                    parsedValue = Double.parseDouble(rawValue);
                }
                else {
                    throw new IkatsException("Failed to parse the value from CSV: unexpected instance of previousValue");
                }
            }
            catch (NumberFormatException e) {
                throw new IkatsException("Failed to parse the value from CSV: after first value");
            }
        }
        return parsedValue;
    }

}
