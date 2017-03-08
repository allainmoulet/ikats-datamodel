package fr.cs.ikats.ts.dataset;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PreDestroy;

import org.apache.log4j.Logger;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.ts.dataset.dao.DataSetDAO;
import fr.cs.ikats.ts.dataset.model.DataSet;
import fr.cs.ikats.ts.dataset.model.LinkDatasetTimeSeries;

/**
 * Management facade for datasets 
 */
@Component("DataSetFacade")
@Scope("singleton")
public class DataSetFacade {

    /**
     * the logger instance for this class
     */
    private static final Logger LOGGER = Logger.getLogger(DataSetFacade.class);

    /**DataSetFacade
     * the DAO for acces to MetaData storage
     */
    private DataSetDAO dao;

    /**
     * 
     */
    public DataSetFacade() {
        init();
    }

    /**
     * init the dao and its mapping : use the hibernate.cfg.xml file + add
     * package and classes where annotations are set.
     */
    private void init() {
        LOGGER.info("init of hibernate configuration with classpath resource file /dataSetHibernate.cfg.xml");
        dao = new DataSetDAO();
        dao.init("/dataSetHibernate.cfg.xml");

        dao.addAnotatedPackage("fr.cs.ikats.ts.dataset.model");
        dao.addAnnotatedClass(DataSet.class);
        dao.addAnnotatedClass(LinkDatasetTimeSeries.class);
        dao.completeConfiguration();

    }

    /**
     * persist dataset 
     * @param name name of the dataset
     * @param description his description
     * @param tsuids the list of tsuids
     * @return the identifier of the dataset
     */
    public String persistDataSet(String name,String description,List<String> tsuids) throws IkatsDaoException {
        List<LinkDatasetTimeSeries> ts = new ArrayList<LinkDatasetTimeSeries>();
        for(String tsuid : tsuids) {
            ts.add(new LinkDatasetTimeSeries(tsuid, name));
        }
        DataSet md = new DataSet(name,description,ts);
        return dao.persist(md);
    }
    
    public String persistDataSetFromEntity(String name,String description,
                                           List<FunctionalIdentifier> funcIdList ) throws IkatsDaoException {
        
        List<LinkDatasetTimeSeries> ts = new ArrayList<LinkDatasetTimeSeries>();
        DataSet md = new DataSet(name,description,ts);
        for(FunctionalIdentifier fid : funcIdList) {
            LinkDatasetTimeSeries asso = new LinkDatasetTimeSeries(fid, md);
            
            ts.add( asso );
        }
        
        return dao.persist(md);
    }
    
    
     
    /**
     * get Dataset for TS
     * 
     * @param name name of the dataset
     * @return the dataset
     */
    public DataSet getDataSet(String name) throws IkatsDaoMissingRessource,IkatsDaoException{
        return dao.getDataSet(name);
    }

    /**
     * remove dataset for name
     * 
     * @param name name of the dataset
     * @throws IkatsDaoException 
     */
    public void removeDataSet(String name) throws IkatsDaoMissingRessource,IkatsDaoException {
        dao.removeDataSet(name);
    }

    /**
     * Despite its name: returns all the datasets
     *  
     * @return all the datasets
     */
    public List<DataSet> getAllDataSetSummary() throws IkatsDaoException {
        return dao.getAllDataSets();
    }

    /**
     * 
     * @param tsuid the requested tsuid
     * @return the found list or null if empty
     */
    public List<String> getDataSetNamesForTsuid(String tsuid) throws IkatsDaoException {
        return dao.getDataSetNamesForTsuid(tsuid);
    }
    
    /**
     * delete the link betweend tsuid and datasetName
     * @param tsuid the tsuid to detach from dataset
     * @param datasetName the dataset
     */
    public void removeTSFromDataSet(String tsuid,String datasetName) throws IkatsDaoException {
        dao.removeTSFromDataSet(tsuid, datasetName);
    }
    
    /**
     * Delete the dataset links Timeserie matched by the tsuidList, for the specified dataset name
     * @param datasetName the dataset name
     * @param tsuidList the tsuid list defining the  removed links
     * @throws IkatsDaoException
     */
    public void removeTsLinks(String datasetName, List<String> tsuidList ) throws IkatsDaoException {
        
        ArrayList<LinkDatasetTimeSeries> listTsLinkEntities= new ArrayList<LinkDatasetTimeSeries>();
        for (String tsuid : tsuidList) {
            
            listTsLinkEntities.add( new LinkDatasetTimeSeries(tsuid, datasetName ) );
        }
        
        dao.removeTsLinks(datasetName, listTsLinkEntities);
    }
    
    /**
     * destroy the facade
     */
    @PreDestroy
    public void destroy() {
        System.out.println("Destroying DataSetFacade");
        dao.stop();
    }

    /**
     * update the dataset in mode replace (classicle CRUD) 
     *
     * @param datasetName the name of the dataset to update
     * @param description the new description: specific values: null if unchanged, "" if emptied.
     * @param tsuidList a list of tsuid to add to dataset, can be null
     * @return the number of TS added while updating
     */
    public int updateDataSet(String datasetName, String description, List<String> tsuidList) throws IkatsDaoMissingRessource, IkatsDaoException  {
        DataSet theDataset = dao.getDataSet( datasetName );
        String theDescription = (description != null ) ? description : theDataset.getDescription();
        
        List<LinkDatasetTimeSeries> tsList = new ArrayList<LinkDatasetTimeSeries>();
        for(String tsuid : tsuidList) {
            LinkDatasetTimeSeries ts = new LinkDatasetTimeSeries(tsuid, datasetName);
            tsList.add(ts);
        }
        return dao.update(datasetName,theDescription,tsList);
    }
    
    /**
     * update dataset in mode append:
     * <ul>
     * <li>update the description unless description is null</li>
     * <li>update the dataset content, adding the non already contained ts into the dataset.</li>
     * </ul>
     * Note: it is up to the managers to test integrity of tsuidList: no test performed at this level.
     *
     * @param datasetName the name of the dataset to update: unique reference to existing dataset
     * @param description the new description: specific values: null if unchanged, "" if emptied.
     * @param tsuidList a list of tsuid to add to dataset, can be null
     * @return the number of TS added while updating
     */
    public int updateInAppendMode(String datasetName, String description, List<String> tsuidList) throws IkatsDaoMissingRessource, IkatsDaoException {
        DataSet theDataset = dao.getDataSet( datasetName );
        String theDescription = (description != null ) ? description : theDataset.getDescription();
        
        List<LinkDatasetTimeSeries> tsList = new ArrayList<LinkDatasetTimeSeries>();
        for(String tsuid : tsuidList) {
            LinkDatasetTimeSeries ts = new LinkDatasetTimeSeries(tsuid, datasetName);
            tsList.add(ts);
        }
        return dao.updateAddingTimeseries(datasetName,theDescription,tsList);
    }
}
