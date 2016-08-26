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
