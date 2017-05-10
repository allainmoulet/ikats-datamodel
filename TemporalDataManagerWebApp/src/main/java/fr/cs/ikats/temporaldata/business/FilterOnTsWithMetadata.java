package fr.cs.ikats.temporaldata.business;

import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.metadata.model.MetadataCriterion;

import java.util.Collections;
import java.util.List;

/**
 * This Class is JSONizable: defines a filter on timeseries with tsmetadata criteria
 */
public class FilterOnTsWithMetadata {

    private String datasetName = "";

    private List<FunctionalIdentifier> tsList = Collections.emptyList();

    private List<MetadataCriterion> criteria = Collections.emptyList();

    /**
     * Getter
     * @return the tsList
     */
    public List<FunctionalIdentifier> getTsList() {
        return tsList;
    }

    /**
     * Setter
     * @param tsList the tsList to set
     */
    public void setTsList(List<FunctionalIdentifier> tsList) {
        this.tsList = tsList;
    }

    /**
     * Getter
     * @return the criteria
     */
    public List<MetadataCriterion> getCriteria() {
        return criteria;
    }

    /**
     * Setter
     *
     * @param criteria
     *            the criteria to set
     */
    public void setCriteria(List<MetadataCriterion> criteria) {
        this.criteria = criteria;
    }

    /**
     * Getter
     * @return the datasetName
     */
    public String getDatasetName() {
        return datasetName;
    }

    /**
     * Setter
     * @param datasetName the datasetName to set
     */
    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }
}
