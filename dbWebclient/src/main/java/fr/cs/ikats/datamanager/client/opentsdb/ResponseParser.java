package fr.cs.ikats.datamanager.client.opentsdb;

import java.util.Iterator;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import fr.cs.ikats.datamanager.client.RequestSender;

/**
 * this class parses the openTSDB response to a query/import request
 * 
 * @author ikats
 *
 */
public class ResponseParser {

    private static final Logger LOGGER = Logger.getLogger(ResponseParser.class);

    /**
     * constants for the responses
     */
    static final String KEY_TAGS = "tags";
    static final String KEY_TAG_1 = "profil";
    static final String KEY_TAG_2 = "numero";
    static final String KEY_TSUIDS = "tsuids";
    static final String KEY_TSUID = "tsuid";
    static final String KEY_VALUES = "dps";
    static final String KEY_NB_DATAPOINTS = "totalDatapoints";

    /**
     * constants for the detailed response.
     */
    static final String KEY_ERRORS = "errors";
    static final String KEY_FAILED = "failed";
    static final String KEY_SUCCESS = "success";
    static final String KEY_ERROR = "error";
    static final String KEY_DATAPOINT = "datapoint";
    static final String KEY_TIMESTAMP = "timestamp";
    static final String KEY_SUMMARY = "summary";
    static final String KEY_DETAIL = "detailed";
    static final String KEY_CODE = "code";
    static final String KEY_MESSAGE = "message";
    static final String KEY_TRACE = "trace";

    /**
     * default constructor
     */
    public ResponseParser() {
    }

    /**
     * parse opentsdb import response retrieve errors and return result
     * 
     * @param response the http response
     * @param code the expected code
     * @return the ImportResult
     * @throws IkatsWebClientException if request is in error
     * @throws ParseException is response cannot be parsed
     */
    public static ImportResult parseImportResponse(Response response, int code) throws ParseException {
        // result recovery
        ImportResult result = new ImportResult();
        JSONParser parser = new JSONParser();
        JSONObject returnedJSON = null;
        Object resultObject = parser.parse(getJSONFromResponse(response));
        // FIXME FTO : needed ?
        if (resultObject instanceof JSONObject) {
            returnedJSON = (JSONObject) resultObject;
        }
        
        result.setReponseCode(code);
        
        switch (code) {
			case RequestSender.CODE_404_ERR_BAD_END_POINT:
			case RequestSender.CODE_500_ERR_MISSING_VALUE:
			case RequestSender.CODE_501_ERR_NOT_IMPLEMENTED:
			case RequestSender.CODE_408_ERR_TIMEOUT:
			case RequestSender.CODE_413_ERR_REQUEST_TOO_LARGE:
			case RequestSender.CODE_503_ERR_OVERLOAD:
				// generic error case, no points imported
				JSONObject error = (JSONObject) returnedJSON.get(KEY_ERROR);
				result.setSummary((String) error.get(KEY_MESSAGE));
                result.addError("details", error.toString());
                break;
			case RequestSender.CODE_200_OK:
				// OpenTSDB should not return that code in case of /api/put request !
				LOGGER.warn("OpenTSDB returned a HTTP 200 code on /api/put request");
				// FIXME : provide more details about the original data. Should request object be passed to the method call ? Maybe a solution for errors traceability
			case RequestSender.CODE_204_OK_IMPORT:
			case RequestSender.CODE_400_ERR_BAD_REQUEST:
				// In case of on data point failure the 204 return code is supperseed by code 400
				// @see http://opentsdb.net/docs/build/html/api_http/put.html#response
	            result.setSummary(printSummary(returnedJSON));
	            result.setNumberOfSuccess(getNumberOfSuccess(returnedJSON));
	            result.setNumberOfFailed(getNumberOfFailed(returnedJSON));
	            if (returnedJSON != null && returnedJSON.get(KEY_ERRORS) != null) {
	                JSONArray errors = (JSONArray) returnedJSON.get(KEY_ERRORS);
	                for (Object object : errors) {
	                    JSONObject pointEnErreur = (JSONObject) object;
	                    result.addError(((Long) ((JSONObject) pointEnErreur.get(KEY_DATAPOINT)).get(KEY_TIMESTAMP)).toString(),
	                            (String) pointEnErreur.get(KEY_ERROR));
	                }
	            }
	            break;
			default:
				LOGGER.error("OpenTSDB returned a non managed HTTP code: " + code);
				break;
		}
        
        return result;
    }

    private static String printSummary(JSONObject obj) {
        StringBuilder sb = new StringBuilder("Stored points: ");
        
        sb.append(getNumberOfSuccess(obj));
        sb.append(" (success) /");
        sb.append(getNumberOfFailed(obj));
        sb.append(" (failed)");

        return sb.toString();
    }

    private static long getNumberOfSuccess(JSONObject obj) {
        long ret = 0L;
        if (obj.get(KEY_SUCCESS) != null) {
            ret = Long.parseLong(obj.get(KEY_SUCCESS).toString());
        }
        return ret;
    }
    
    private static long getNumberOfFailed(JSONObject obj) {
        long ret = 0L;
        if (obj.get(KEY_FAILED) != null) {
            ret = Long.parseLong(obj.get(KEY_FAILED).toString());
        }
        return ret;
    }

    /**
     * analyse request response to retrieve values of a set of series
     * @param response the response
     * @param forTest true if only for test
     * 
     * @return a QueryResult object
     * @throws IkatsWebClientException if request is in error
     */
    public static QueryResult parseQueryReponse(Response response, boolean forTest) throws IkatsWebClientException {
        LOGGER.debug("Parsing the QueryReponse");
        QueryResult result = new QueryResult();
        try {
            JSONObject serie = null;
            JSONParser parser = new JSONParser();
            String tag1 = null;
            String tag2 = null;
            JSONObject points = null;
            String tsuid = null;
            if(response.getStatus()<204) {
                Object resultObject = parser.parse(getJSONFromResponse(response));
                if (resultObject instanceof JSONArray) {
                    JSONArray res = (JSONArray) resultObject;
                    for (int i = 0; i < res.size(); i++) {
                        serie = (JSONObject) res.get(i);
                        tsuid = (String) ((JSONArray) serie.get(KEY_TSUIDS)).get(0);
                        tag1 = (String) ((JSONObject) serie.get(KEY_TAGS)).get(KEY_TAG_1);
                        tag2 = (String) ((JSONObject) serie.get(KEY_TAGS)).get(KEY_TAG_2);
                        points = (JSONObject) serie.get(KEY_VALUES);
                        if (LOGGER.isTraceEnabled()) {
                            Iterator<?> it = points.values().iterator();
                            while (it.hasNext()) {
                                LOGGER.trace("Valeur = " + it.next());
                            }
                        }
                        if (!forTest && Pattern.matches("^[a-zA-Z]+[0-9]{1}$", tag1) && Pattern.matches("^[0-9]+$", tag2)) {
                            result.addSerie(tsuid, Integer.valueOf(tag1.substring(tag1.length() - 1) + tag2), points);
                        }
                        else {
                            result.addSerie(tsuid, points);
                        }
                    }
                }
            } else {
                String msg = "Serveru returned an error "+response.getStatus()+" on request";
                LOGGER.error(msg);
                throw new IkatsWebClientException(msg);
            }

        }
        catch (ParseException e) {
            LOGGER.error("Error parsing the QueryReponse", e);
            throw new IkatsWebClientException(e.getMessage());
        }
        return result;
    }

    /**
     * analyse request response to retrieve all series for a given metric, by
     * meaning the tag number do not retrieve used values because bug in lookup
     * request
     * 
     * @param response the response
     * @return a QueryResult object
     * @throws IkatsWebClientException if request is in error
     */
    public static QueryResult parseQueryTsuidsReponse(Response response) throws IkatsWebClientException {
        LOGGER.debug("Parsing the TsuidsReponse");
        QueryResult resultat = new QueryResult();
        try {
            JSONParser parser = new JSONParser();
            Object resultObject = parser.parse(getJSONFromResponse(response));
            if (resultObject instanceof JSONArray) {
                if (((JSONArray) resultObject).size() > 0) {
                    JSONObject res = (JSONObject) ((JSONArray) resultObject).get(0);
                    String tag1 = (String) ((JSONObject) res.get(KEY_TAGS)).get(KEY_TAG_1);
                    JSONArray tsuids = (JSONArray) res.get(KEY_TSUIDS);
                    resultat.setTsuids(tsuids);
                    resultat.setTag(tag1);
                }
            }

        }
        catch (ParseException e) {
            LOGGER.error("Error parsing the TsuidsReponse", e);
            throw new IkatsWebClientException(e.getMessage());
        }
        return resultat;
    }

    /**
     * analyse lookup request response (bugged in version 2.1RC1)
     * 
     * @param response the response
     * @return a QueryMetaResult object
     * @throws IkatsWebClientException if request is in error
     */
    public static QueryMetaResult parseLookupReponse(Response response) throws IkatsWebClientException {
        LOGGER.debug("Parsing the LookupReponse");
        QueryMetaResult result = new QueryMetaResult();
        JSONObject serie = null;
        String tag1 = null;
        String tag2 = null;
        String tsuid = null;
        int nbSeries = 0;
        JSONParser parser = new JSONParser();
        // Object resultObject;
        try {
            JSONObject resultObject = (JSONObject) parser.parse(getJSONFromResponse(response));
            if (resultObject.get("results") instanceof JSONArray) {
                JSONArray res = (JSONArray) resultObject.get("results");
                for (int i = 0; i < res.size(); i++) {
                    serie = (JSONObject) res.get(i);
                    tag1 = (String) ((JSONObject) serie.get(KEY_TAGS)).get(KEY_TAG_1);
                    tag2 = (String) ((JSONObject) serie.get(KEY_TAGS)).get(KEY_TAG_2);
                    tsuid = (String) serie.get(KEY_TSUID);
                    result.addSerie(tsuid, tag1, tag2);
                }
            }
        }
        catch (ParseException e) {
            LOGGER.error("Error parsing the LookupReponse", e);
            throw new IkatsWebClientException("Error parsing the lookup response", e);
        }
        return result;
    }

    /**
     * TODO : NON UTILISABLE !!!! analyse la réponse de la requête pour
     * retrouver les metadata d'une série
     * 
     * @param response the response
     * @return a QueryResult object : TODO create specific object
     * @throws IkatsWebClientException if request is in error
     */
    public static QueryResult parseQueryTsmetaReponse(Response response) throws IkatsWebClientException {
        LOGGER.debug("Parsing the QueryTsmetaReponse");
        QueryResult result = new QueryResult();
        try {
            JSONParser parser = new JSONParser();
            JSONObject resultObject = (JSONObject) parser.parse(getJSONFromResponse(response));
            String tsuid = (String) resultObject.get(KEY_TSUID);
            Integer nbDataPoints = (Integer) resultObject.get(KEY_NB_DATAPOINTS);

            result.addSerie(tsuid, null);

        }
        catch (ParseException e) {
            LOGGER.error("Error parsing the QueryTsmetaReponse", e);
            throw new IkatsWebClientException("Error parsing the QueryTsmetaReponse", e);
        }
        return result;
    }

    /**
     * read JSON from responses
     * @param response the response
     * @return a string 
     */
    public static String getJSONFromResponse(Response response) {
        String str = "";
        if (response.hasEntity()) {
            str = response.readEntity(String.class);
        }
        else {
            str = response.getStatusInfo().getReasonPhrase();
        }
        return str;
    }
}
