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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

/**
 * Query Result get from opentsdb
 */
public class QueryResult implements Serializable {
    private static final Logger logger = Logger.getLogger(QueryResult.class);

    private static final long serialVersionUID = 8590227526827248120L;
    private Map<Integer, Map<Object, Object>> series;
    private List<String> tsuids;
    private String tag;
    Integer key = 0;

    /**
     * default constructor, init the maps and lists
     */
    public QueryResult() {
        series = new LinkedHashMap<Integer, Map<Object, Object>>();
        tsuids = new ArrayList<String>();
    }

    /**
     * ajout serie en passant la clé de series
     * 
     * @param tsuid the tsuid
     * @param key teh key of the serie
     * @param datapoints the datapoints
     */
    public void addSerie(String tsuid, Integer key, Map<Object, Object> datapoints) {
        series.put(key, datapoints);
        tsuids.add(tsuid);
    }

    /**
     * ajout serie. la gestion de la clé est geree en interne
     * @param tsuid the tsuid
     * @param datapoints datapoints 
     * 
     */
    public void addSerie(String tsuid, Map<Object, Object> datapoints) {
        logger.debug("Add Serie " + tsuid + " containing " + datapoints.size() + " points  with key " + this.key);
        series.put(this.key, datapoints);
        if (tsuid != null)
            tsuids.add(tsuid);
        key++;
    }

    /**
     * @return the meta
     */
    public String afficheMeta() {
        StringBuilder sb = new StringBuilder();
        Object[] values = null;
        int i = 0;
        Iterator<Integer> it = series.keySet().iterator();
        while (it.hasNext()) {
            sb.append(tsuids.get(i)).append(':');
            values = series.get(it.next()).values().toArray();
            sb.append(values.length).append(" points / ");
            sb.append(values[0]).append(" / ").append(values[values.length - 1]);
            sb.append(System.getProperty("line.separator"));
            i++;
        }
        return sb.toString();
    }

    /**
     * return the values
     * @return values of the serie
     */
    public String afficheValeurs() {
        StringBuilder sb = new StringBuilder();
        for (Entry<Integer, Map<Object, Object>> serie : series.entrySet()) {
            sb.append(serie.getKey()).append(':');
            Object[] vals = serie.getValue().values().toArray();
            for (int i = 0; i < vals.length; i++) {
                sb.append(vals[i]).append(',');
            }
            sb.append(" / ");
        }
        return sb.toString();
    }

    /**
     * @return the series
     */
    public Map<Integer, Map<Object, Object>> getSeries() {
        return series;
    }

    /**
     * @return le nombre de series
     */
    public int getNbSeries() {
        return tsuids.size();
    }

    /**
     * get the number of points in serie
     * @param serieKey the key
     * @return an integer, or 0 if serie is not found.
     */
    public int getNbPointsForSerie(Integer serieKey) {
        int points = 0;
        if (series.containsKey(serieKey)) {
            logger.debug("getNbPointsForSerie");
            points = series.get(serieKey).size();
        }
        return points;
    }

    /**
     * get points for serie
     * @param serieKey the key
     * @return an array of objects
     */
    public Object[] getPointsForSerie(Integer serieKey) {
        return series.get(serieKey).values().toArray();
    }

    /**
     * Geter
     * @return the tsuids
     */
    public List<String> getTsuids() {
        return tsuids;
    }

    /**
     * Getter 
     * @param pos the position
     * @return the tsuid
     */
    public String getTsuid(int pos) {
        return tsuids.get(pos);
    }

    /**
     * @param tsuids
     *            the tsuids to set
     */
    public void setTsuids(List<String> tsuids) {
        this.tsuids = tsuids;
    }

    /**
     * @return the tag
     */
    public String getTag() {
        return tag;
    }

    /**
     * @param tag
     *            the tag to set
     */
    public void setTag(String tag) {
        this.tag = tag;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((tsuids == null) ? 0 : tsuids.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QueryResult other = (QueryResult) obj;
        if (tsuids == null) {
            if (other.tsuids != null)
                return false;
        }
        else if (!tsuids.equals(other.tsuids))
            return false;
        return true;
    }

}

