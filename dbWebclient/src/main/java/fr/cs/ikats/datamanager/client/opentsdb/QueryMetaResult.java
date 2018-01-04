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

package fr.cs.ikats.datamanager.client.opentsdb;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * result of meta query
 */
public class QueryMetaResult implements Serializable {

    private static final long serialVersionUID = 8623274622284411448L;
    /**
     * key : tsuid / vals : tags values
     */
    private Map<String, String[]> series;

    /**
     * constructorLogger
     */
    public QueryMetaResult() {
        series = new LinkedHashMap<String, String[]>();
    }

    /**
     * add serie 
     * @param tsuid the tsuid
     * @param tag1 a tag
     * @param tag2 a tag
     */
    public void addSerie(String tsuid, String tag1, String tag2) {
        series.put(tsuid, new String[] { tag1, tag2 });
    }

    /**
     * @return the series
     */
    public Map<String, String[]> getSeries() {
        return series;
    }

    /**
     * Getter 
     * @return the tsuids of the series
     */
    public Set<String> getTsuids() {
        return series.keySet();
    }

}

