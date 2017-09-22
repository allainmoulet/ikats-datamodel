package fr.cs.ikats.process.data.dao;

import java.sql.Blob;
import java.util.List;

import fr.cs.ikats.common.dao.DataBaseDAO;
import fr.cs.ikats.process.data.model.ProcessData;
import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.Hibernate;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.MatchMode;
import org.hibernate.criterion.Restrictions;


/**
 * DAO for ProcessData
 */
public class ProcessDataDAO extends DataBaseDAO {

    private static Logger LOGGER = Logger.getLogger(ProcessDataDAO.class);

    /**
     * public constructor
     */
    public ProcessDataDAO() {

    }


    /**
     * persist the ProcessData
     *
     * @param ds   the process data
     * @param data data to save
     *
     * @return the internal identifier if ProcessData has been correctly persisted,
     */
    public String persist(ProcessData ds, byte[] data) {

        Session session = getSession();
        Transaction tx = null;
        Integer processDataId = null;
        try {
            tx = session.beginTransaction();
            Blob blob;
            blob = Hibernate.createBlob(data);
            ds.setData(blob);
            processDataId = (Integer) session.save(ds);
            session.flush();
            tx.commit();
            LOGGER.debug("ProcessData stored " + ds);
        }
        catch (HibernateException e) {
            if (tx != null) {
                tx.rollback();
            }
            LOGGER.error("", e);
        }
        finally {
            session.close();
        }
        return processDataId.toString();
    }

    /**
     * return a ProcessData instance from database, null if no ProcessData is found.
     *
     * @param id the internal id
     *
     * @return a ProcessData or null if no ProcessData is found.
     */
    public ProcessData getProcessData(Integer id) {
        ProcessData result = null;
        Session session = getSession();
        try {
            result = (ProcessData) session.get(ProcessData.class, id);
        }
        catch (HibernateException e) {
            LOGGER.error("ProcessData " + id + " not found in database", e);
        }
        finally {
            session.close();
        }
        return result;
    }

    /**
     * return a ProcessData instance from database, null if no ProcessData is found.
     *
     * @param processId identifier of the producer
     *
     * @return a ProcessData or an empty list if no ProcessData is found or null if an HibernateException is raised.
     */
    public List<ProcessData> getProcessData(String processId) {
        List<ProcessData> result = null;
        Session session = getSession();
        try {
            Criteria criteria = session.createCriteria(ProcessData.class);
            criteria.add(Restrictions.eq("processId", processId));
            result = criteria.list();
        }
        catch (HibernateException e) {
            // In next version: we ought to manage exceptions instead of returning null:
            // =>  impact analysis + global refactoring: we need to correct each impacted service
            //
            // throw new IkatsDaoException("Error reading process Data for processId " + processId + " in database", e);
            LOGGER.error("Error reading process Data for processId=" + processId + " in database", e);
        }
        finally {
            session.close();
        }

        if ((result != null) && result.isEmpty()) {
            LOGGER.info("No process Data for processId=" + result + " found in database");
        }
        return result;
    }

    /**
     * return all ProcessData from database,
     */
    public List<ProcessData> listTables() {
        List<ProcessData> result = null;
        Session session = getSession();
        try {
            Criteria criteria = session.createCriteria(ProcessData.class);
            criteria.add(Restrictions.sqlRestriction("processid ~ '[a-zA-Z]'"));
            // Table are handled in ProcessData so as CorrelationDataset results.
            // This restriction prevents from having too much non-table data.
            // This temporary patch will be fixed once we switch to JHipster to generate "table" part
            criteria.add(
                    Restrictions.not(
                            Restrictions.ilike("processId", "CorrelationDataset", MatchMode.START)
                                    )
                        );
            result = criteria.list();
        }
        catch (HibernateException e) {
            // In next version: we ought to manage exceptions instead of returning null:
            // =>  impact analysis + global refactoring: we need to correct each impacted service
            //
            // throw new IkatsDaoException("Error reading process Data for processId " + processId + " in database", e);
            LOGGER.error("Error reading process Data in database", e);
        }
        finally {
            session.close();
        }

        if ((result != null) && result.isEmpty()) {
            LOGGER.info("No process Data found in database");
        }
        return result;
    }


    /**
     * remove the ProcessData from database.
     *
     * @param processId identifier of the producer
     */
    public void removeAllProcessData(String processId) {
        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            List<ProcessData> list = getProcessData(processId);
            for (ProcessData pd : list) {
                session.delete(pd);
            }
            tx.commit();
        }
        catch (HibernateException e) {
            if (tx != null) {
                tx.rollback();
            }
            LOGGER.error("Error deleting ProcessData for " + processId, e);
        }
        finally {
            session.close();
        }
    }
}
