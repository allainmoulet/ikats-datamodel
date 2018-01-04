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

package fr.cs.ikats.temporaldata.business;

import java.util.ArrayList;
import java.util.List;

import fr.cs.ikats.metadata.model.FunctionalIdentifier;

/**
 * Provides a dataset combining
 */
public class DataSetWithFids {

    /**
     * name of the dataset
     */
    private String name;

    /**
     * a short description of the dataset
     */
    private String description;

    /**
     * list of time series identifiers
     */
    private List<FunctionalIdentifier> fids;

    /**
     * public constructor
     *
     * @param name
     *            name of the dataset
     * @param description
     *            a short description of the dataset
     */
    public DataSetWithFids(String name, String description, List<FunctionalIdentifier> fids) {
        this.name = name;
        this.description = description;
        this.fids = fids;
    }

    /**
     * Default constructor
     */
    public DataSetWithFids() {
        super();
    }

    /**
     * Getter
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Getter
     *
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Getter
     *
     * @return the {@link FunctionalIdentifier} list
     */
    public List<FunctionalIdentifier> getFids() {
        return fids;
    }

    /**
     * return a List of string with tsuids
     * @return a list
     */
    public List<String> getTsuidsAsString() {
        List<String> stringList = new ArrayList<String>();
        if (fids != null) {
            for (FunctionalIdentifier fid : fids) {
                stringList.add(fid.getTsuid());
            }
        }
        return stringList;
    }

    /**
     * Return the {@link FunctionalIdentifier} retrieved from a tsuid
     * @return a string for the fid
     */
    public String getFid(String tsuid) {
        for (FunctionalIdentifier functionalIdentifier : fids) {
            if (functionalIdentifier.getTsuid().equals(tsuid)) {
                return functionalIdentifier.getFuncId();
            }
        }

        return null;
    }

    /**
     * @return String representation: short version without tsuids
     */
    public String toString() {
        return "DatasetWithFids name=[" + name + "] description=[" + description + "]";
    }

    /**
     *
     * @return String representation: detailed version with tsuids listed
     */
    public String toDetailedString() {
        StringBuilder lBuff = new StringBuilder(toString());
        lBuff.append(" timeseries=[ ");

        if (fids != null) {
            for (FunctionalIdentifier fid : fids) {
                lBuff.append(fid.getTsuid());
                lBuff.append(" with funcId=");
                lBuff.append(fid.getFuncId());
            }
        }
        lBuff.append("]");
        return lBuff.toString();
    }

}

