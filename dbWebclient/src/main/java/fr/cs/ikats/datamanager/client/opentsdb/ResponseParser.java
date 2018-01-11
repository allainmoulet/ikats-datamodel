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
 * @author Mathieu BERAUD <mathieu.beraud@c-s.fr>
 * @author Maxime PERELMUTER <maxime.perelmuter@c-s.fr>
 *
 */

package fr.cs.ikats.datamanager.client.opentsdb;

import java.io.IOException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import fr.cs.ikats.datamanager.client.opentsdb.ApiResponse.Error;

/**
 * this class parses the openTSDB response to a query/import request
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
     * @return the ImportResult
     * @throws IkatsWebClientException if request is in error
     * @throws ParseException
     */
    public static ImportResult parseImportResponse(Response response) throws IkatsWebClientException, ParseException {
        // result recovery
        ImportResult result = new ImportResult();
        // Get the status code
        int status = response.getStatus();
        result.setStatusCode(status);

        ApiStatus apiStatus;
        try {
            apiStatus = ApiStatus.valueOf(status);
        } catch (IllegalArgumentException e) {
            // the response status is not compliant to OpenTSDB specification
            String error = "OpenTSDB returned a non managed HTTP code: " + status;
            LOGGER.error(error);
            result.setSummary("Unable to determine import status");
            result.addError(Integer.toString(status), error);
            return result;
        }

        // extract response information in JSON object
        JSONParser parser = new JSONParser();
        JSONObject returnedJSON = (JSONObject) parser.parse(getJSONFromResponse(response));

        switch (apiStatus) {
            case CODE_200:
                // OpenTSDB should not return that code in case of /api/put request !
                // seems to be a documentation bug : http://opentsdb.net/docs/build/html/api_http/put.html#response
                LOGGER.warn("OpenTSDB returned a HTTP 200 code on /api/put request");
                break;
            case CODE_204:
                // All good !
                result.setSummary("All points imported with no error");
                result.setNumberOfSuccess(getNumberOfSuccess(returnedJSON));
                break;
            case CODE_400:
                // Some points were not imported
                long nbSuccess = getNumberOfSuccess(returnedJSON);
                long nbFailed = getNumberOfFailed(returnedJSON);
                JSONArray errors = (JSONArray) returnedJSON.get(KEY_ERRORS);
                for (Object object : errors) {
                    JSONObject pointWithError = (JSONObject) object;
                    result.addError(((Long) ((JSONObject) pointWithError.get(KEY_DATAPOINT)).get(KEY_TIMESTAMP)).toString(),
                            (String) pointWithError.get(KEY_ERROR));
                }
                result.setSummary("Bad request when putting points : Success " + nbSuccess + "/ Failed " + nbFailed
                        + " | Nb Errors details : " + errors.size());
                break;
            default:
                // check if there is any error in the response
                Error parseForError = parseForError(response);
                if (parseForError != null) {

                    // set error information in the results
                    result.setError(parseForError);
                    result.setSummary(parseForError.message);

                } else {
                    // Should not pass here due to call to ApiStatus.valueOf and its try/catch
                }
                break;
        }
        return result;
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
     * read JSON from responses
     *
     * @param response the response
     * @return a string
     */
    public static String getJSONFromResponse(Response response) {
        String str = "";
        if (response.hasEntity()) {
            str = response.readEntity(String.class);
        } else {
            str = response.getStatusInfo().getReasonPhrase();
        }
        return str;
    }

    /**
     * Return a parsed response for {@link ApiResponse.Error} content using Jackon JSON mapper
     *
     * @param response
     * @return
     * @throws IkatsWebClientException
     */
    public static ApiResponse.Error parseForError(Response response) throws IkatsWebClientException {

        if (ApiStatus.isError(response.getStatus())) {

            // Test whether we receive a response content in JSON
            MediaType mediaType = response.getMediaType();
            if (mediaType != MediaType.APPLICATION_JSON_TYPE) {
                throw new IkatsWebClientException("OpenTSDB doesn't return a JSON response for error");
            }

            // Read the content as a string
            String content = response.readEntity(String.class);

            try {
                ObjectMapper jsonMapper = new ObjectMapper();
                Error errorContent = jsonMapper.readValue(content, ApiResponse.Error.class);

                return errorContent;

            } catch (JsonParseException | JsonMappingException e) {
                throw new IkatsWebClientException("Error while parsing OpenTSDB JSON response", e);
            } catch (IOException e) {
                throw new IkatsWebClientException("System error while parsing OpenTSDB JSON response", e);
            }

        } else {

            // The response is not an error, so we could not return anything
            return null;
        }
    }

}
