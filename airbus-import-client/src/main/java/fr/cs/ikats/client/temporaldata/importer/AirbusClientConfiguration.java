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
 * 
 */

package fr.cs.ikats.client.temporaldata.importer;

import java.util.List;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.log4j.Logger;

/**
 * reflects the configuration file properties
 */
public class AirbusClientConfiguration {

	private static final Logger LOGGER = Logger.getLogger(AirbusClientConfiguration.class);
	CompositeConfiguration config;
	
	String propertiesFile = "client.properties";
	
	/**
	 * NO_SET value for tags
	 */
	public final static String NOT_SET = "NOT_SET";
	
	/**
	 * init the Composite configuration 
	 */
	public AirbusClientConfiguration() {		
		config = new CompositeConfiguration();
		config.addConfiguration(new SystemConfiguration());
		try {
			config.addConfiguration(new PropertiesConfiguration(propertiesFile));
		} catch (ConfigurationException e) {
			LOGGER.error("Error loading properties file "+propertiesFile);
		}
		
	}
	/**
	 * Get the config
	 * @return the config
	 */
	public CompositeConfiguration getConfiguration() {
		return config;
	}
	
	/**
     * read an int value from config
     * @param key the property key
     * @return the value
     */
	public int getIntValue(String key) {
		return config.getInt(key);
	}
	
	/**
     * read an long value from config
     * @param key the property key
     * @return the value
     */
	public long getLongValue(String key) {
		return config.getLong(key);
	}
	
	/**
     * read an String value from config
     * @param key the property key
     * @return the value
     */
	public String getStringValue(String key) {
		return config.getString(key);
	}
	
	/**
     * read an String multi value from config
     * @param key the property key
     * @return the values
     */
	public List<Object> getStringList(String key) {
		return config.getList(key);
	}
}

