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
 * @author Mathieu BERAUD <mathieu.beraud@c-s.fr>
 */

package fr.cs.ikats.temporaldata.resource;

import fr.cs.ikats.temporaldata.business.MetaDataManager;
import fr.cs.ikats.temporaldata.business.TemporalDataManager;

/**
 * Asbtract class for convenient method to get configuration
 *
 *
 */
public abstract class AbstractResource {

    /**
     * the metadataManager containing business logic for metadata
     */
    protected MetaDataManager metadataManager;

    /**
     * the temporalDatamanager containing business logic for time series
     */
    protected TemporalDataManager temporalDataManager;

    /**
     * default constructor, init the two managers
     */
    protected AbstractResource() {
        metadataManager = new MetaDataManager();
        temporalDataManager = new TemporalDataManager();
    }

    /**
     * Getter
     * @return the metadataManager
     */
    public final MetaDataManager getMetadataManager() {
        return metadataManager;
    }

    /**
     * Getter
     * @return the temporalDataManager
     */
    public final TemporalDataManager getTemporalDataManager() {
        return temporalDataManager;
    }

    /**
     * Setter
     * @param metadataManager the metadataManager to set
     */
    public final void setMetadataManager(MetaDataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    /**
     * Setter
     * @param temporalDataManager the temporalDataManager to set
     */
    public final void setTemporalDataManager(TemporalDataManager temporalDataManager) {
        this.temporalDataManager = temporalDataManager;
    }
}

