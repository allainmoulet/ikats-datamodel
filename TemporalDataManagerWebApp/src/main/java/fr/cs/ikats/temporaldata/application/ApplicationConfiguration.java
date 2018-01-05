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

package fr.cs.ikats.temporaldata.application;

import java.util.List;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Application Configuration Class,
 * container for Spring configuration, Jersey configuration and labels configuration
 */
public class ApplicationConfiguration {

    private static Logger logger = Logger.getLogger(ApplicationConfiguration.class);

    /**
     * Configuration keys CONSTANTS
     */
    public static final String HOST_DB_API = "host.db.api";
    @SuppressWarnings("javadoc")
    public static final String URL_DB_API_BASE = "url.db.api.base";
    @SuppressWarnings("javadoc")
    public static final String URL_DB_API_QUERY = "url.db.api.query";
    @SuppressWarnings("javadoc")
    public static final String URL_DB_API_LOOKUP = "url.db.api.lookup";
    @SuppressWarnings("javadoc")
    public static final String URL_DB_API_IMPORT = "url.db.api.import";
    @SuppressWarnings("javadoc")
    public static final String URL_DB_API_UID_META = "url.db.api.uid.tsmeta";
    @SuppressWarnings("javadoc")
    public static final String REQUEST_SEARCH_DEFAULT_START_TIME = "request.search.defaultStartTime";
    @SuppressWarnings("javadoc")
    public static final String REQUEST_SEARCH_DEFAULT_NB_SERIES = "request.search.defaultNbSeries";
    @SuppressWarnings("javadoc")
    public static final String REQUEST_SEARCH_OPTIONS = "request.search.options";
    @SuppressWarnings("javadoc")
    public static final String IMPORT_EXECUTOR_SERVICE_SIZE = "import.executorService.size";
    @SuppressWarnings("javadoc")
    public static final String IMPORT_EXECUTOR_POOL_SIZE = "import.executor.pool.size";
    @SuppressWarnings("javadoc")
    public static final String IMPORT_THREAD_POOL_NAME = "IkatsTDMImportExecutor";
    @SuppressWarnings("javadoc")
    public static final String IMPORT_QUALIFIER = "importer.qualifier";
    @SuppressWarnings("javadoc")
    public static final String IMPORT_NB_POINTS_BY_BATCH = "import.nb.points.batch";
    @SuppressWarnings("javadoc")
    public static final String DB_API_MSRESOLUTION = "db.api.msResolution";
    @SuppressWarnings("javadoc")
    public static final String DB_FLUSHING_INTERVAL = "db.flushing.interval";


    /**
     * configuration of the webapp
     */
    CompositeConfiguration config;

    /**
     * srping application context.
     */
    ApplicationContext context;

    /**
     * init configuration and Spring application context using Annotations.
     */
    public ApplicationConfiguration() {
        String propertiesFile = "application.properties";
        config = new CompositeConfiguration();
        config.addConfiguration(new SystemConfiguration());
        try {
            config.addConfiguration(new PropertiesConfiguration(propertiesFile));
        } catch (ConfigurationException e) {
            logger.error("Error loading properties file " + propertiesFile);
        }

    }

    // GETTERS for configuration

    /**
     * return the spring Context, init it if necessary.
     *
     * @return the spring context
     */
    public ApplicationContext getSpringContext() {
        if (context == null) {
            context = new AnnotationConfigApplicationContext("fr.cs.ikats");
        }
        return context;
    }

    /**
     * get the jersey configuration
     *
     * @return config
     */
    public CompositeConfiguration getConfiguration() {
        return config;
    }

    /**
     * get Integer Value From configuration
     *
     * @param key configuration key
     * @return int
     */
    public int getIntValue(String key) {
        return config.getInt(key);
    }

    /**
     * get Long Value From configuration
     *
     * @param key configuration key
     * @return Long
     */
    public long getLongValue(String key) {
        return config.getLong(key);
    }

    /**
     * get String Value From configuration
     *
     * @param key configuration key
     * @return String
     */

    public String getStringValue(String key) {
        return config.getString(key);
    }

    /**
     * get String list Value From configuration
     *
     * @param key configuration key
     * @return List of Object
     */

    public List<Object> getStringList(String key) {
        return config.getList(key);
    }

}

