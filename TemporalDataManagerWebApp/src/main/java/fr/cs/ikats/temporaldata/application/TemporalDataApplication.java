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

import javax.annotation.PreDestroy;
import javax.ws.rs.ApplicationPath;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

import fr.cs.ikats.temporaldata.utils.ExecutorManager;

/**
 * Application Class for temporal data Webapp.
 * Path is root
 */
@ApplicationPath("/")
public class TemporalDataApplication extends ResourceConfig {

    /**
     * configuration for this application
     */
    private static ApplicationConfiguration CONFIGURATION = new ApplicationConfiguration();

    /**
     * get jersey CONFIGURATION
     *
     * @return CONFIGURATION
     */
    public static ApplicationConfiguration getApplicationConfiguration() {
        return CONFIGURATION;
    }

    /**
     * add resource package as Jersey resources classes
     * register the import executor pool.
     */
    public TemporalDataApplication() {

        // register multipart feature and jackson JSON for all resources.
        packages("fr.cs.ikats.temporaldata.resource").register(MultiPartFeature.class).register(JacksonFeature.class);
        packages("fr.cs.ikats.temporaldata.exception");
        // registering thread pool for import
        ExecutorManager.getInstance().registerExecutorPool(ApplicationConfiguration.IMPORT_THREAD_POOL_NAME, getImportExecutorServiceSize(), getImportExecutorPoolSize());
        if (CONFIGURATION == null) {
            CONFIGURATION = new ApplicationConfiguration();
        }
        CONFIGURATION.getSpringContext();
    }

    /**
     * get the Import thread numbers.
     *
     * @return
     */
    private int getImportExecutorServiceSize() {
        return TemporalDataApplication.getApplicationConfiguration().getIntValue(ApplicationConfiguration.IMPORT_EXECUTOR_SERVICE_SIZE);
    }

    /**
     * get the Import executor Pool Size.
     *
     * @return
     */
    private int getImportExecutorPoolSize() {
        return TemporalDataApplication.getApplicationConfiguration().getIntValue(ApplicationConfiguration.IMPORT_EXECUTOR_POOL_SIZE);
    }

    /**
     * {@inheritDoc}
     */
    @Override

    protected void finalize() throws Throwable {
        super.finalize();
    }

    /**
     * destroy the Application
     */
    @PreDestroy
    public void destroy() {
        ExecutorManager.getInstance().stopExecutors();
    }


}

