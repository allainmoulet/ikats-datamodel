/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 8 juin 2016 : Creation 
 *
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.temporaldata.business;

import fr.cs.ikats.ts.dataset.model.DataSet;

/**
 * Dataset Wrapper for JSON interface
 */
public class DataSetInfo {

    
    final private String name;
    
    final private String description;
    
    final private Integer nb_ts;
    
    public DataSetInfo( DataSet dataset )
    {
        name = dataset.getName();
        description = dataset.getDescription();
        nb_ts = dataset.getLinksToTimeSeries().size();
    }

    /**
     * Getter
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Getter
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Getter
     * @return the nb_ts
     */
    public Integer getNb_ts() {
        return nb_ts;
    }
    
    
}
