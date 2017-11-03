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

package fr.cs.ikats.temporaldata;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import fr.cs.ikats.temporaldata.application.TemporalDataApplication;

/**
 * Main class.
 *
 */
public class ServerMain {
    // Base URI the Grizzly HTTP server will listen on
    private static final Logger LOGGER = Logger.getLogger(ServerMain.class);

    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this
     * application.
     * 
     * @return Grizzly HTTP server.
     */
    public static HttpServer startServer(String baseUri) {
        // create a resource config that scans for JAX-RS resources and
        // providers
        // in fr.cs.ikats.temporaldata package
        // final ResourceConfig rc = new
        // ResourceConfig().packages("fr.cs.ikats.temporaldata").register(MultiPartFeature.class);
        final ResourceConfig app = new TemporalDataApplication();
        //app.register(LoggingFilter.class);    
        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        return GrizzlyHttpServerFactory.createHttpServer(URI.create(baseUri), app);
    }

    public static void stopServer(HttpServer server) {
        server.shutdownNow();
    }

    /**
     * Main method.
     * 
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // init test configuration
        String propertiesFile = "test.properties";
        CompositeConfiguration testConfig = new CompositeConfiguration();
        testConfig.addConfiguration(new SystemConfiguration());
        try {
            testConfig.addConfiguration(new PropertiesConfiguration(propertiesFile));
        }
        catch (ConfigurationException e) {
            LOGGER.error("Error loading properties file " + propertiesFile);
        }
        final HttpServer server = startServer(testConfig.getString("testAPIURL"));
        System.out.println(String.format("Jersey app started with WADL available at " + "%sapplication.wadl\nHit enter to stop it...",
                testConfig.getString("testAPIURL")));
        System.in.read();
        server.shutdownNow();
    }
}

