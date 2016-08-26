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

/**
 * DataSet model class, link a named dataset with a list of tsuids.
 * 
 */
@Entity
@Table(name = "TSDataSet")
public class DataSet {

    /**
     * all dataset name and description request
     */
    public final static String LIST_ALL_DATASETS = "select ds from DataSet ds";

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
     * list of time series identifiers
     */
    @OneToMany(cascade = { CascadeType.ALL }, mappedBy = "dataset")
    @LazyCollection(LazyCollectionOption.FALSE)
    private List<TimeSerie> tsuids;

    /**
     * public constructor
     * 
     * @param name
     *            name of the dataset
     * @param description
     *            a short description of the dataset
     * @param tsuids
     *            list of time series identifiers
     */
    public DataSet(String name, String description, List<TimeSerie> tsuids) {
        this.name = name;
        this.tsuids = tsuids;
        this.description = description;
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
     * @return the tsuids
     */
    public List<TimeSerie> getTsuids() {
        return tsuids;
    }

    /**
     * return a List of string with tsuids
     * 
     * @return a list
     */
    public List<String> getTsuidsAsString() {
        List<String> stringList = new ArrayList<String>();
        if (tsuids != null) {
            for (TimeSerie ts : tsuids) {
                stringList.add(ts.getTsuid());
            }
        }
        return stringList;
    }

    /**
     * setter for description
     * 
     * @param description
     *            the description ot the dataset
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
     * 
     * @return String representation: detailed version with tsuids listed
     */
    public String toDetailedString(boolean lazy) {
        StringBuilder lBuff = new StringBuilder(toString());
        lBuff.append(" tsuids=[ ");
        boolean lStart = true;
        if (!lazy) {
            getTsuids();
        }
        if (tsuids != null) {
            for (TimeSerie timeSerie : tsuids) {
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
        else
        {
            return false;
        } 
    }
}
