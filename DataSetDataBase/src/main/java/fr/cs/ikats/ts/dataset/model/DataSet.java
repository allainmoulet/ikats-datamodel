package fr.cs.ikats.ts.dataset.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.hibernate.annotations.LazyCollection;
import org.hibernate.annotations.LazyCollectionOption;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

/**
 * DataSet model class, link a named dataset with a list of tsuids.
 */
@Entity
@Table(name = "TSDataSet")
public class DataSet {

    /**
     * all dataset name and description request
     */
    public final static String LIST_ALL_DATASETS = "SELECT tsdataset.name as name, tsdataset.description as description, COUNT(timeseries_dataset.tsuid) AS nb_ts FROM tsdataset, timeseries_dataset WHERE tsdataset.name = timeseries_dataset.dataset_name GROUP BY tsdataset.name, tsdataset.description";

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
        this.nb_ts = (long) theLinksToTimeSeries.size();
    }

    /**
     * default contructor, for hibernate instanciation.
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
     * @param datasetname
     */
    public void setName(String datasetname) {
        name = datasetname;

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
            for (LinkDatasetTimeSeries timeSerie : linksToTimeSeries) {
                if (!lStart) {
                    lBuff.append(", ");
                }
                else {
                    lStart = false;
                }
                timeSerie.getFuncIdentifier();
                timeSerie.getDataset();
                lBuff.append(timeSerie.getTsuid());
            }
        }

        lBuff.append("]");
        return lBuff.toString();
    }

    /**
     * equals operator is required using the Collection API.
     * <br/>
     * Beware that in our context: equals(...) is True when the operands are targetting the same dataset,
     * but they may require changes.
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DataSet) {
            DataSet dsObj = (DataSet) obj;
            // if ( this.name != null )
            // {
            return this.name.equals(dsObj.name);
            // }
            // else
            // {
            // return false; // weird
            // }

        }
        else {
            return false;
        }
    }

    public Long getNb_ts() {
        return nb_ts;
    }

    public void setNb_ts(Long nb_ts) {
        this.nb_ts = nb_ts;
    }
}
