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

package fr.cs.ikats.datamanager.client.opentsdb.generator;

import java.text.ParseException;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * JSon generator from input csv file generate a JSON from a CSV line using a
 * SplittedLineReader with his own configuration. The configuration is used to
 * read the single splitted values of timestamps
 */
public class AdvancedJsonGenerator {

    static final String DATA_SET_TAG_NAME = "dataset";
    static final int DIGITS_SECONDE = 10;
    static final int DIGITS_MILLISECONDE = 13;

    /**
     * list of tags
     */
    Map<String, String> tags;

    /**
     * metric
     */
    String metric;

    /**
     * input line with points
     */
    private String pointList;

    /**
     * splitted input line.
     */
    private String[] splittedLine;

    /**
     * start date of the timeseries
     */
    private Long lowestTimeStampValue;

    /**
     * end date of the timeseries
     */
    private Long highestTimeStampValue;

    /**
     * line reader
     */
    private SplittedLineReader lineReader;

    /**
     * constructor
     *
     * @param reader the line reader
     * @param metric the metric
     * @param tags   the tags
     */
    public AdvancedJsonGenerator(SplittedLineReader reader, String metric, Map<String, String> tags) {
        this.metric = metric;
        this.tags = tags;
        lineReader = reader;
        this.highestTimeStampValue = 0L;
        this.lowestTimeStampValue = 0L;
    }

    /**
     * Converts input string to output JSON array acording to csv colums
     * configuration. Performs timestamp format check.
     *
     * @param input string from csv file
     * @return string JSON
     * @throws ParseException
     */
    @SuppressWarnings("unchecked")
    public String generate(String input) throws ParseException {
        // split ligne
        this.pointList = input;
        lineToArray();
        int maxIndice = splittedLine.length / lineReader.configuration.getColumnConfigurations().size();
        if (maxIndice > 0) {

            // JSON array creation
            JSONArray points = new JSONArray();
            JSONObject p = new JSONObject();
            initTags(p);

            // loop JSON filling
            long date;
            long minDate = Long.MAX_VALUE;
            long maxDate = Long.MIN_VALUE;

            for (int i = 0; i < maxIndice; i++) {
                date = lineReader.fillObject(p, splittedLine, i * lineReader.configuration.getColumnConfigurations().size());
                points.add(p.clone());

                /* setting temporary start and end dates during parsing */
                if (date < minDate) {
                    minDate = date;
                }
                if (date > maxDate) {
                    maxDate = date;
                }
            }
            /* update start and end dates attributes */
            setHighestTimeStampValue(maxDate);
            setLowestTimeStampValue(minDate);
            return points.toJSONString();
        } else {
            return null;
        }


    }

    /**
     * set the tags values and metric values from the instance attributes metric
     * and tags.
     *
     * @param p objet point
     */
    @SuppressWarnings("unchecked")
    private void initTags(JSONObject p) {

        p.put(JsonConstants.KEY_METRIQUE, metric);
        // gestion de 2 etiquettes
        JSONObject jsonTags = new JSONObject();
        if (tags != null && !tags.isEmpty()) {
            for (String tagKey : tags.keySet()) {
                jsonTags.put(tagKey, tags.get(tagKey));
            }
        } else {
            jsonTags.put("metric", metric);
        }
        p.put(JsonConstants.KEY_TAGS, jsonTags);
    }

    /**
     * split line into tokens. authorized separators : ; OR : OR space
     */
    private void lineToArray() {
        String sep;
        if (pointList.contains(";"))
            sep = ";";
        else if (pointList.contains(":"))
            sep = ":";
        else {
            sep = " ";
        }
        // replacement of , by . in numbers
        if (pointList.contains(","))
            this.splittedLine = pointList.replaceAll("\\s+", " ").replace(',', '.').split(sep);
        else
            this.splittedLine = pointList.replaceAll("\\s+", " ").split(sep);

        pointList = null;
    }

    /**
     * @return the lowestTimeStampValue
     */
    public Long getLowestTimeStampValue() {
        return lowestTimeStampValue;
    }

    /**
     * @param lowestTimeStampValue the lowestTimeStampValue to set
     */
    public void setLowestTimeStampValue(Long lowestTimeStampValue) {
        this.lowestTimeStampValue = lowestTimeStampValue;
    }

    /**
     * @return the highestTimeStampValue
     */
    public Long getHighestTimeStampValue() {
        return highestTimeStampValue;
    }

    /**
     * @param highestTimeStampValue the highestTimeStampValue to set
     */
    public void setHighestTimeStampValue(Long highestTimeStampValue) {
        this.highestTimeStampValue = highestTimeStampValue;
    }

}
