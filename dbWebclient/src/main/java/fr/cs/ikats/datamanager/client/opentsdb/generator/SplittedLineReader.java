/**
 * LICENSE:
 * --------
 * Copyright 2017 CS SYSTEMES D'INFORMATION
 *
 * Licensed to CS SYSTEMES D'INFORMATION under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. CS licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
 */

package fr.cs.ikats.datamanager.client.opentsdb.generator;

import java.text.ParseException;

import org.json.simple.JSONObject;

import fr.cs.ikats.datamanager.client.opentsdb.generator.ReaderConfiguration.ColumnConfiguration;

/**
 * to read splitted line from csv input file according to columns configuration
 */
public class SplittedLineReader {

    ReaderConfiguration configuration;

    /**
     * constructor
     *
     * @param configuration the reader configuration
     */
    public SplittedLineReader(ReaderConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * fill a simple JSON object from the index of the splitted line according
     * to columns configuration
     *
     * @param p            JSON object
     * @param splittedLine the splittedline to read
     * @param index        of splittedline element
     * @return the dateValue
     * @throws ParseException if line cannot be parsed
     */
    @SuppressWarnings("unchecked")
    public Long fillObject(JSONObject p, String[] splittedLine, int index) throws ParseException {
        Long dateValue = null;
        for (ColumnConfiguration colConfig : configuration.getColumnConfigurations()) {
            int colIndex = configuration.getColumnConfigurations().indexOf(colConfig);
            if (colConfig != null) {
                if (colConfig.isTimeStampColumn) {
                    dateValue = getDateValue(splittedLine, colConfig, index + colIndex);
                    p.put(JsonConstants.KEY_TIME, dateValue);
                } else if (colConfig.isValueColumn) {
                    p.put(JsonConstants.KEY_VAL, splittedLine[index + colIndex]);
                }
            }
        }
        return dateValue;
    }

    /**
     * retrieve Epoch time from given timestamp format
     *
     * @param splittedLine the splitted line to read
     * @param colConfig    the col configuration fo the data
     * @param colIndex     the col index of the date
     * @return the date
     * @throws ParseException if date is not paresable
     */
    protected Long getDateValue(String[] splittedLine, ColumnConfiguration colConfig, int colIndex) throws ParseException {
        String value = splittedLine[colIndex];
        Long longValue = colConfig.timestampFormat.parse(value).getTime();
        return longValue;
    }

}

