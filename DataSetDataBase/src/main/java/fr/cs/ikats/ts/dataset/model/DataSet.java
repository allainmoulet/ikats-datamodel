/**
 * LICENSE:
 * --------
 * Copyright 2017-2018 CS SYSTEMES D'INFORMATION
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
 * @author Maxime PERELMUTER <maxime.perelmuter@c-s.fr>
 *
 */

package fr.cs.ikats.ts.dataset.model;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.annotations.LazyCollection;
import org.hibernate.annotations.LazyCollectionOption;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * DataSet model class, link a named dataset with a list of tsuids.
 */
@Entity
@Table(name = "TSDataSet")
public class DataSet {

    /**
     * all dataset name and description request
     */
    public final static String LIST_ALL_DATASETS = "SELECT tsdataset.name as name, tsdataset.description as " +
            "description, COUNT(timeseries_dataset.tsuid) AS nb_ts FROM tsdataset, timeseries_dataset WHERE " +
            "tsdataset.name = timeseries_dataset.dataset_name GROUP BY tsdataset.name, tsdataset.description";

    /**
     * name of the dataset
     */
    @Id
    @Column(name = "name")
    private String name;

    /**
     * a short description of the dataset
     */
    @Column(name = "description")
    private String description;

    /**
     * the number of TS composing the dataset
     */
    private Long nb_ts;

    /**
     * list of links between this dataset and its timeseries
     */
    @OneToMany(cascade = {CascadeType.ALL}, mappedBy = "dataset")
    @LazyCollection(LazyCollectionOption.TRUE)
    private List<LinkDatasetTimeSeries> linksToTimeSeries;

    /**
     * public constructor
     *
     * @param name                 name of the dataset
     * @param description          a short description of the dataset
     * @param theLinksToTimeSeries list of the links to the timeseries belonging to this dataset
     */
    public DataSet(String name, String description, List<LinkDatasetTimeSeries> theLinksToTimeSeries) {
        this.name = name;
        this.linksToTimeSeries = theLinksToTimeSeries;
        this.description = description;
        this.nb_ts = 0L;
        if (theLinksToTimeSeries != null) {
            this.nb_ts = (long) theLinksToTimeSeries.size();
        }
    }

    /**
     * default constructor, for hibernate instantiation.
     */
    public DataSet() {
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
     * Setter
     *
     * @param datasetName the name to set
     */
    public void setName(String datasetName) {
        name = datasetName;

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
     * @return the links between this dataset container and its timeseries elements
     */
    @JsonIgnore
    public List<LinkDatasetTimeSeries> getLinksToTimeSeries() {
        return linksToTimeSeries;
    }

    /**
     * return a List of string with tsuids
     *
     * @return a list
     */
    public List<String> getTsuidsAsString() {
        List<String> stringList = new ArrayList<String>();
        if (linksToTimeSeries != null) {
            for (LinkDatasetTimeSeries ts : linksToTimeSeries) {
                stringList.add(ts.getTsuid());
            }
        }
        return stringList;
    }

    /**
     * setter for description
     *
     * @param description the description ot the dataset
     */
    public void setDescription(String description) {
        this.description = description;

    }

    /**
     * @return String representation: short version without tsuids
     */
    public String toString() {
        return "Dataset name=[" + name + "] description=[" + description + "]";
    }

    /**
     * @return String representation: detailed version with tsuids listed
     */
    public String toDetailedString(boolean lazy) {
        StringBuilder lBuff = new StringBuilder(toString());
        lBuff.append(" tsuids=[ ");
        boolean lStart = true;
        if (!lazy) {
            getLinksToTimeSeries();
        }
        if (linksToTimeSeries != null) {
            for (LinkDatasetTimeSeries timeSeries : linksToTimeSeries) {
                if (!lStart) {
                    lBuff.append(", ");
                } else {
                    lStart = false;
                }
                timeSeries.getFuncIdentifier();
                timeSeries.getDataset();
                lBuff.append(timeSeries.getTsuid());
            }
        }

        lBuff.append("]");
        return lBuff.toString();
    }

    /**
     * equals operator is required using the Collection API.
     * <br/>
     * Beware that in our context: equals(...) is True when the operands are targeting the same dataset,
     * but they may require changes.
     * <br/>
     * Using Hibernate: advised to implement equals: see §13.1.3
     * http://docs.jboss.org/hibernate/orm/3.6/reference/en-US/html_single/#transactions-demarcation
     *
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DataSet) {
            DataSet dsObj = (DataSet) obj;
            return this.name.equals(dsObj.name);
        } else {
            return false;
        }
    }

    /**
     * Using Hibernate: advised to implement hashcode: see §13.1.3
     * http://docs.jboss.org/hibernate/orm/3.6/reference/en-US/html_single/#transactions-demarcation
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {

        return ("" + name + "DS").hashCode();
    }

    public Long getNb_ts() {
        return nb_ts;
    }

    public void setNb_ts(Long nb_ts) {
        this.nb_ts = nb_ts;
    }
}
