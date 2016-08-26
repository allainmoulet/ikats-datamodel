package fr.cs.ikats.temporaldata.resource;

import fr.cs.ikats.temporaldata.business.MetaDataManager;
import fr.cs.ikats.temporaldata.business.TemporalDataManager;

/**
 * Asbtract class for convenient method to get configuration
 * 
 * @author ikats
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
}
