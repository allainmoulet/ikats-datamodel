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
