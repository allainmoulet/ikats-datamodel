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
 * 
 */

package fr.cs.ikats.datamanager.client.opentsdb.importer;


import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import fr.cs.ikats.datamanager.client.importer.IImportSerializer;
import fr.cs.ikats.datamanager.client.opentsdb.generator.ReaderConfiguration;
import fr.cs.ikats.datamanager.client.opentsdb.generator.SplittedLineReader;

/**
 * sets up csv columns configuration for a common csv file
 * 
 *
 */
// Review#147170 expliquer l'usage du @Qualifier "Common"
@Component
@Qualifier("Common")
public class CommonDataJsonIzer extends AbstractDataJsonIzer {

    private static SplittedLineReader commonReader = createCommonReader();

    public SplittedLineReader getReader() {
        return commonReader;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#clone()
     */
    @Override
    public IImportSerializer clone() {
        return new CommonDataJsonIzer();
    }
    // Review#147170 javadoc incomplete ci dessous: merite d'etre completee
    // Review#147170  - format attendu <timestamp> <sep> <valeur> ...
    /**
     * defines csv input columns content timestamp format :
     * 2013-05-03T05:30:34,8 yyyy-MM-ddThh:mm:ss.S
     * 
     * @return
     */
    private static SplittedLineReader createCommonReader() {
        ReaderConfiguration configuration = new ReaderConfiguration();
        configuration.addColumnConfiguration(null, null, getDateFormat(), false, true);
        configuration.addColumnConfiguration(null, "Long", null, true, false);
        SplittedLineReader reader = new SplittedLineReader(configuration);
        return reader;
    }

    
    
    @SuppressWarnings("serial")
	static DateFormat getDateFormat() {
        return new DateFormat() {

            @Override
            public Date parse(String source, ParsePosition pos) {
                Date date = null;
                try {
                    DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
                    format.setTimeZone(TimeZone.getTimeZone("GMT"));
                    date = format.parse(source, pos);
                    if (date == null) {
                        format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                        format.setTimeZone(TimeZone.getTimeZone("GMT"));
                        date = format.parse(source, pos);
                    }
                    if (date == null) {
                        format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                        format.setTimeZone(TimeZone.getTimeZone("GMT"));
                        date = format.parse(source, pos);
                    }
                    if (date == null) {
                        format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S");
                        format.setTimeZone(TimeZone.getTimeZone("GMT"));
                        date = format.parse(source, pos);
                    }
                    if (date == null) {
                        format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
                        format.setTimeZone(TimeZone.getTimeZone("GMT"));
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
                DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
                return format.format(date, toAppendTo, fieldPosition);
            }
        };
    }

}

