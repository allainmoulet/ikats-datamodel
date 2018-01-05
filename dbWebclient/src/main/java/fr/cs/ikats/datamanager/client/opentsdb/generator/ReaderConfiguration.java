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

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * this class is used to store and read csv columns configuration
 *
 *
 */
public class ReaderConfiguration {

    List<ColumnConfiguration> colConfigs;

    /**
     * default constructor
     */
    public ReaderConfiguration() {
        colConfigs = new ArrayList<ColumnConfiguration>();
    }

    /**
     * to add a column configuration
     *
     * @param tagName
     *            name of the tag
     * @param valueFormat
     *            format of the value
     * @param timestampFormat
     *            format of the timestamp
     * @param isValueColumn
     *            if column contents value
     * @param isTimeStampColumn
     *            if column contents timestamp
     */
    public void addColumnConfiguration(String tagName, String valueFormat, DateFormat timestampFormat, boolean isValueColumn,
                                       boolean isTimeStampColumn) {
        colConfigs.add(new ColumnConfiguration(tagName, valueFormat, timestampFormat, isValueColumn, isTimeStampColumn));
    }

    /**
     * to add a non read column in the configuration
     */
    public void addNonReadColumnConfiguration() {
        colConfigs.add(null);
    }

    /**
     * retrieve columns configuration
     *
     * @return the configurations
     */
    public List<ColumnConfiguration> getColumnConfigurations() {
        return colConfigs;
    }

    /**
     * retrieve the number of columns in configuration
     *
     * @return number of columns.
     */
    public int getNumberOfColumns() {
        return colConfigs.size();
    }

    /**
     * class describing csv columns configuration
     *
     *
     */
    public class ColumnConfiguration {
        String tagName;
        String valueFormat;
        DateFormat timestampFormat;
        boolean isValueColumn;
        boolean isTimeStampColumn;

        /**
         *
         * @param tagName
         *            name of the tag
         * @param valueFormat
         *            format of the value
         * @param timestampFormat
         *            format of the timestamp
         * @param isValueColumn
         *            if column contents value
         * @param isTimeStampColumn
         *            if column contents timestamp
         */
        public ColumnConfiguration(String tagName, String valueFormat, DateFormat timestampFormat, boolean isValueColumn, boolean isTimeStampColumn) {
            this.tagName = tagName;
            this.valueFormat = valueFormat;
            this.timestampFormat = timestampFormat;
            this.isValueColumn = isValueColumn;
            this.isTimeStampColumn = isTimeStampColumn;
        }

    }

}

