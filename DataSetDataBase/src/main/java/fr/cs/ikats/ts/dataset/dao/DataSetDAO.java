package fr.cs.ikats.ts.dataset.dao;

import fr.cs.ikats.common.dao.DataBaseDAO;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import fr.cs.ikats.ts.dataset.model.DataSet;
import fr.cs.ikats.ts.dataset.model.LinkDatasetTimeSeries;
import org.apache.log4j.Logger;
import org.hibernate.*;
import org.hibernate.transform.Transformers;

import java.util.Iterator;
import java.util.List;

/**
 * DAO class for dataSet model. use underlying database with hibernate
 * configuration.
 */
public class DataSetDAO extends DataBaseDAO {

    private static final Logger LOGGER = Logger.getLogger(DataSetDAO.class);

    /**
     * public constructor
     */
    public DataSetDAO() {

    }

    /**
     * persist the dataset
     *
     * @param ds the dataset
     *
     * @return the internal identifier if dataset has been correctly persisted,
     */
    public String persist(DataSet ds) throws IkatsDaoException {
        Session session = getSession();
        Transaction tx = null;
        String mdId = null;
        String name = "null";
        try {
            name = ds.getName();

            tx = session.beginTransaction();
            for (LinkDatasetTimeSeries ts : ds.getLinksToTimeSeries()) {
                ts.setDataset(ds);
                session.save(ts);
            }
            mdId = (String) session.save(ds);
            session.flush();
            tx.commit();
            LOGGER.debug("DataSet stored " + ds.getName() + ";" + ds.getDescription() + ";" + ds.getLinksToTimeSeries());
        }
        catch (HibernateException e) {
            // build IkatsDaoConflictException or ... or IkatsDaoException
            // according to received cause
            IkatsDaoException error = buildDaoException("Failed to create DataSet named=" + name, e);
            rollbackAndThrowException(tx, error);
        }
        catch (Throwable te) {
            IkatsDaoException error = new IkatsDaoException("Unexpected error: failed to create DataSet named=" + name, te);
            rollbackAndThrowException(tx, error);
        }
        finally {
            session.close();
        }
        return mdId;
    }

    /**
     * update the dataset,
     *
     * @param name        the name of the dataset to update
     * @param description the new description
     * @param tsList      a list of ts to add to dataset, must not be null
     *
     * @return the number of TS added while updating
     */
    public int update(String name, String description, List<LinkDatasetTimeSeries> tsList) throws IkatsDaoException {
        Session session = getSession();
        int count = 0;

        Transaction tx = null;
        try {

            LOGGER.info("Updating TS list size=" + tsList.size() + " for dataset with name=" + name);

            tx = session.beginTransaction();

            DataSet mergedDs = (DataSet) session.get(DataSet.class, name);
            if (mergedDs != null) {
                Iterator<LinkDatasetTimeSeries> iterTS = mergedDs.getLinksToTimeSeries().iterator();
                while (iterTS.hasNext()) {
                    LinkDatasetTimeSeries ts = iterTS.next();
                    session.delete(ts);
                    iterTS.remove();
                }
            }
            else {
                throw new IkatsDaoMissingRessource("Update dataset failed: Dataset not found with name=" + name);
            }

            mergedDs.setDescription(description);
            session.update(mergedDs);

            for (LinkDatasetTimeSeries ts : tsList) {
                ts.setDataset(mergedDs);
                mergedDs.getLinksToTimeSeries().add(ts);
                count++;
            }
            session.update(mergedDs);
            tx.commit();
        }
        catch (IkatsDaoMissingRessource me) {
            rollbackAndThrowException(tx, me);
        }
        catch (HibernateException e) {
            IkatsDaoException error = new IkatsDaoException(
                    "HibernateException occurred => failed to update dataset using mode replace, with name=" + name, e);
            rollbackAndThrowException(tx, error);
        }
        catch (Throwable te) {
            IkatsDaoException error = new IkatsDaoException(
                    te.getClass().getSimpleName() + "unexpectedly occurred => Failed to update dataset using mode replace, with name=" + name, te);
            rollbackAndThrowException(tx, error);
        }
        finally {
            session.close();
        }
        return count;
    }

    /**
     * update the dataset : add only one timeseries
     *
     * @param tsuid       the identifier of the timeseries to add
     * @param datasetName the name of the dataset to update
     */
    public void updateAddOneTimeseries(String tsuid, String datasetName) throws IkatsDaoException {
        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            LinkDatasetTimeSeries linkDatasetTimeSeries = new LinkDatasetTimeSeries(tsuid, datasetName);
            session.save(linkDatasetTimeSeries);
            tx.commit();
        }
        catch (HibernateException e) {
            IkatsDaoException error = new IkatsDaoException(
                    "Failed to add the timeseries " + tsuid + " to dataset " + datasetName, e);
            rollbackAndThrowException(tx, error);
        }
        catch (Throwable te) {
            IkatsDaoException error = new IkatsDaoException(
                    te.getClass().getSimpleName() + "unexpectedly occured => Failed to add the timeserie " + tsuid + " to dataset " + datasetName, te);
            rollbackAndThrowException(tx, error);
        }
        finally {
            session.close();
        }
    }

    /**
     * update the dataset, in mode "append": add the time series to the dataset,
     * if not already in the content of the dataset
     *
     * @param name        the name of the dataset to update
     * @param description the new description
     * @param tsList      a list of ts to add to dataset, must not be null
     *
     * @return the number of TS added while updating
     */
    public int updateAddingTimeseries(String name, String description, List<LinkDatasetTimeSeries> tsList) throws IkatsDaoException {
        Session session = getSession();
        int count = 0;
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            DataSet ds = (DataSet) session.get(DataSet.class, name);
            for (LinkDatasetTimeSeries ts : tsList) {
                if (!ds.getTsuidsAsString().contains(ts.getTsuid())) {
                    ts.setDataset(ds);
                    session.saveOrUpdate(ts);
                    ds.getLinksToTimeSeries().add(ts);
                    count++;
                }
            }
            ds.setDescription(description);
            session.saveOrUpdate(ds);
            tx.commit();
        }
        catch (HibernateException e) {
            IkatsDaoException error = new IkatsDaoException(
                    "HibernateException occured => failed to update dataset using mode append, with name=" + name, e);
            rollbackAndThrowException(tx, error);
        }
        catch (Throwable te) {
            IkatsDaoException error = new IkatsDaoException(
                    te.getClass().getSimpleName() + "unexpectedly occured => Failed to update dataset using mode append, with name=" + name, te);
            rollbackAndThrowException(tx, error);
        }
        finally {
            session.close();
        }
        return count;
    }

    /**
     * Return a DataSet instance from database, null if no dataset is found.
     *
     * @param name the name of the dataset
     *
     * @return a DataSet or null if no dataset is found.
     */
    public DataSet getDataSet(String name) throws IkatsDaoMissingRessource, IkatsDaoException {
        DataSet result;
        Session session = getSession();
        try {
            result = (DataSet) session.get(DataSet.class, name);
            if (result == null) {
                throw new IkatsDaoMissingRessource("DataSet with name=" + name);
            }
            Hibernate.initialize(result.getLinksToTimeSeries());
            result.getLinksToTimeSeries();
        }
        catch (IkatsDaoMissingRessource me) {
            throw me;
        }
        catch (HibernateException e) {
            IkatsDaoMissingRessource error = new IkatsDaoMissingRessource("DataSet with name=" + name, e);
            throw error;
        }
        catch (Throwable te) {
            IkatsDaoException error = new IkatsDaoException("Unexpected error: Get DataSet with name=" + name, te);
            throw error;
        }
        finally {
            session.close();
        }
        return result; // never null
    }

    /**
     * Return a DataSet summary instance from database, null if no dataset is found.
     * linksToTimeSeries won't be gathered
     *
     * @param name the name of the dataset
     *
     * @return a DataSet or null if no dataset is found.
     */
    public DataSet getDataSetSummary(String name) throws IkatsDaoMissingRessource, IkatsDaoException {
        DataSet result;
        Session session = getSession();
        try {
            result = (DataSet) session.get(DataSet.class, name);
            if (result == null) {
                throw new IkatsDaoMissingRessource("DataSet with name=" + name);
            }
        }
        catch (IkatsDaoMissingRessource me) {
            throw me;
        }
        catch (HibernateException e) {
            IkatsDaoMissingRessource error = new IkatsDaoMissingRessource("DataSet with name=" + name, e);
            throw error;
        }
        catch (Throwable te) {
            IkatsDaoException error = new IkatsDaoException("Unexpected error: Get DataSet with name=" + name, te);
            throw error;
        }
        finally {
            session.close();
        }
        return result; // never null
    }


    /**
     * Remove the dataset from database.
     *
     * @param name the dataset name to remove
     *
     * @throws IkatsDaoMissingRessource error when the dataset is not found
     * @throws IkatsDaoException        another error
     */
    public void removeDataSet(String name) throws IkatsDaoMissingRessource, IkatsDaoException {
        Session session = getSession();
        Transaction tx = null;
        DataSet ds;
        try {
            tx = session.beginTransaction();
            ds = (DataSet) session.get(DataSet.class, name);
            if (ds != null) {
                for (LinkDatasetTimeSeries ts : ds.getLinksToTimeSeries()) {
                    session.delete(ts);
                }
                session.delete(ds);
            }
            else {
                throw new IkatsDaoMissingRessource("Dataset not found in database: " + name);
            }
            tx.commit();

        }
        catch (HibernateException e) {
            String msg = "Deleting dataset: " + name + ": unexpected HibernateException";
            LOGGER.error(msg, e);
            rollbackAndThrowException(tx, new IkatsDaoException(msg, e));
        }
        catch (IkatsDaoMissingRessource e) {

            LOGGER.error(e);
            rollbackAndThrowException(tx, e);
        }
        catch (Exception anotherError) {
            // deals with null pointer exceptions ...
            String msg = "Deleting dataset: " + name + ": unexpected Exception";
            LOGGER.error(msg, anotherError);

            rollbackAndThrowException(tx, new IkatsDaoException(msg, anotherError));
        }
        finally {
            session.close();
        }
    }

    /**
     * Return all the dataset found in database.
     *
     * @return all corresponding datasets
     */
    public List<DataSet> getAllDataSets() throws IkatsDaoMissingRessource, IkatsDaoException {
        List<DataSet> result;
        Session session = getSession();
        try {
            Query q = session.createSQLQuery(DataSet.LIST_ALL_DATASETS)
                    .addScalar("name", Hibernate.STRING)
                    .addScalar("description", Hibernate.STRING)
                    .addScalar("nb_ts", Hibernate.LONG)
                    .setResultTransformer(Transformers.aliasToBean(DataSet.class));
            result = (List<DataSet>) q.list();
        }
        catch (HibernateException e) {
            throw new IkatsDaoMissingRessource("Hibernate error: Get all DataSets ", e);
        }
        catch (Throwable te) {
            throw new IkatsDaoException("Unexpected error: Get all DataSets", te);
        }
        finally {
            session.close();
        }

        return result;
    }

    /**
     * @param tsuid the requested tsuid
     *
     * @return the found list or null if empty
     */
    public List<String> getDataSetNamesForTsuid(String tsuid) throws IkatsDaoException {
        Session session = getSession();
        List<String> result;
        try {
            Query q = session.createQuery(LinkDatasetTimeSeries.LIST_DATASET_NAMES_FOR_TSUID);
            q.setString("tsuid", tsuid);
            result = q.list();

            // Note: TODO TBC should we handle IkatsDaoMissingRessource here ?
            // if ((result == null) || (result.size() == 0)) {
            // throw new IkatsDaoMissingRessource("Empty result: Get All DataSet
            // names");
            // }
        }
        // Note: TODO TBC should we handle IkatsDaoMissingRessource here ?
        // catch (IkatsDaoMissingRessource me) {
        // throw me;
        // }
        catch (HibernateException e) {
            IkatsDaoException error = new IkatsDaoMissingRessource("Hibernate error: Get all DataSet names", e);
            throw error;

        }
        catch (Throwable te) {
            IkatsDaoException error = new IkatsDaoException("Unexpected error: Get all DataSet names", te);
            throw error;

        }
        finally {
            session.close();
        }
        return result;
    }

    /**
     * delete the link between tsuid and datasetName
     *
     * @param tsuid       the tsuid to detach from dataset
     * @param datasetName the dataset
     */
    public void removeTSFromDataSet(String tsuid, String datasetName) throws IkatsDaoException {
        Session session = getSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            Query q = session.createQuery(LinkDatasetTimeSeries.DELETE_TS_FROM_DATASET);
            q.setString("dataset", datasetName);
            q.setString("tsuid", tsuid);
            q.executeUpdate();
            tx.commit();
        }
        catch (HibernateException e) {
            IkatsDaoException error = new IkatsDaoException("HibernateException occured => failed to delete dataset with name=" + datasetName, e);
            rollbackAndThrowException(tx, error);
        }
        catch (Throwable te) {
            IkatsDaoException error = new IkatsDaoException(
                    te.getClass().getSimpleName() + "unexpectedly occured => failed to delete dataset with name=" + datasetName, te);
            rollbackAndThrowException(tx, error);
        }
        finally {
            session.close();
        }
    }

    public int removeTsLinks(String name, List<LinkDatasetTimeSeries> tsList) throws IkatsDaoException {
        Session session = getSession();
        int count = 0;

        Transaction tx = null;
        try {

            LOGGER.info("Deleting TS list size=" + tsList.size() + " from dataset with name=" + name);

            tx = session.beginTransaction();

            DataSet mergedDs = (DataSet) session.get(DataSet.class, name);
            if (mergedDs != null) {
                Iterator<LinkDatasetTimeSeries> iterTS = mergedDs.getLinksToTimeSeries().iterator();
                while (iterTS.hasNext()) {
                    LinkDatasetTimeSeries ts = iterTS.next();
                    if (tsList.contains(ts)) {
                        session.delete(ts);
                        iterTS.remove();
                    }
                }
            }
            else {
                throw new IkatsDaoMissingRessource("Remove TS links from dataset failed: Dataset not found with name=" + name);
            }
            session.update(mergedDs);
            tx.commit();
        }
        catch (IkatsDaoMissingRessource me) {
            rollbackAndThrowException(tx, me);
        }
        catch (HibernateException e) {
            IkatsDaoException error = new IkatsDaoException(
                    "HibernateException occured => failed to remove TS links from dataset dataset using mode replace, with name=" + name, e);
            rollbackAndThrowException(tx, error);
        }
        catch (Throwable te) {
            IkatsDaoException error = new IkatsDaoException(te.getClass().getSimpleName()
                                                                    + "unexpectedly occured => Failed to remove TS links from dataset dataset using mode replace, with name=" + name, te);
            rollbackAndThrowException(tx, error);
        }
        finally {
            session.close();
        }
        return count;
    }

}
