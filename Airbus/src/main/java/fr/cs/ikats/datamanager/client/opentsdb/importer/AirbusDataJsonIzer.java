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
import java.util.Date;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import fr.cs.ikats.datamanager.client.importer.IImportSerializer;
import fr.cs.ikats.datamanager.client.opentsdb.generator.ReaderConfiguration;
import fr.cs.ikats.datamanager.client.opentsdb.generator.SplittedLineReader;

// Review#147170 globalement un peu plus de javadoc ... classe + methodes publiques

// Review#147170 comment added

// Keep spring annotations @Component @Qualifier until TemporalDataManager 'import' services require them:
// TODO annotations to be removed once these services are suppressed from TemporalDataManager
@Component
@Qualifier("Airbus")
public class AirbusDataJsonIzer extends AbstractDataJsonIzer {
	
	private static SplittedLineReader airbusReader = createAirbusReader();
	
	public SplittedLineReader getReader() {
		return airbusReader;
	}
	

	private static SplittedLineReader createAirbusReader() {
		ReaderConfiguration configuration = new ReaderConfiguration();
		configuration.addColumnConfiguration(null, null, getDateFormat(), false, true);
		configuration.addColumnConfiguration(null, "Long", null, true, false);
		SplittedLineReader reader = new SplittedLineReader(configuration);
		return reader;
	}
	
	// Review#147170 pourquoi non-Javadoc ? => javadoc
	/**
	 * @see java.lang.Object#clone()
	 */
	@Override
	public IImportSerializer clone()  {
		return new AirbusDataJsonIzer();
	}
	
	private static DateFormat getDateFormat() {
	    return new DateFormat() {
            
            /**
             * 
             */
            private static final long serialVersionUID = -373121725779963294L;

            @Override
            public Date parse(String source, ParsePosition pos) {
                return parse(source);
            }
            
            @Override
            public Date parse(String source) {
                Double doubleValue = new Double(source)/1000d;
                return new Date(doubleValue.longValue());
            }
            
            @Override
            public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
                toAppendTo.append(Long.toString(date.getTime()));
                return toAppendTo;
            }
        };
	}

}

