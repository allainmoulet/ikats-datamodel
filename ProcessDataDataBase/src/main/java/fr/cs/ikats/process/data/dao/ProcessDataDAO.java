package fr.cs.ikats.process.data.dao;

import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.MatchMode;
import org.hibernate.criterion.Restrictions;

import fr.cs.ikats.common.dao.DataBaseDAO;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.process.data.model.ProcessData;

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
        Integer processDataId = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            ds.setData(data);
            processDataId = (Integer) session.save(ds);
            LOGGER.trace("ProcessData stored " + ds);

            tx.commit();
        }
        catch (RuntimeException e) {
            // try to rollback
            if (tx != null) tx.rollback();
            // Re-raise the original exception
            throw e;
        }
        finally {
            // end the session
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
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            LOGGER.debug("Getting processId:" + id);

            result = (ProcessData) session.get(ProcessData.class, id);
            
            tx.commit();
        }
        catch (RuntimeException e) {
        	 if (tx != null) tx.rollback();
        	 throw e; // or display error message
        }
        finally {
            // end the session
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
      
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            LOGGER.debug("Getting processId:" + processId);

            Criteria criteria = session.createCriteria(ProcessData.class);
            criteria.add(Restrictions.eq("processId", processId));
            result = criteria.list();
            
            tx.commit();
        }
        catch (RuntimeException e) {
        	 if (tx != null) tx.rollback();
        	 throw e; // or display error message
        }
        finally {
            // end the session
            session.close();
        }

        if ((result != null) && result.isEmpty()) {
            LOGGER.debug("No process Data for processId=" + result + " found in database");
        }

        return result;
    }

    /**
     * return all Tables from database,
     * 
     * @return the list of all tables
     */
    public List<ProcessData> listTables() {
        List<ProcessData> result = null;
        Session session = getSession();
        
        Transaction tx = null;
        try {
        	tx = session.beginTransaction();
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
            
            // Read-only query. Transcation commit has implication but save transaction resource from IDLE state.
            tx.commit();
        }
        catch (RuntimeException e) {
            // In next version: we ought to manage exceptions instead of returning null:
            // =>  impact analysis + global refactoring: we need to correct each impacted service
            //
            // throw new IkatsDaoException("Error reading process Data for processId " + processId + " in database", e);
            LOGGER.error("Error reading process Data in database", e);
            
            if (tx != null) tx.rollback();
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
     *
     * @throws IkatsDaoException if error occurs in database
     */
    public void removeAllProcessData(String processId) throws IkatsDaoException {

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
        catch (RuntimeException e) {
          
        	LOGGER.error("Error deleting ProcessData for " + processId, e);
            
        	// try to rollback
        	if (tx != null) tx.rollback();
            // Re-raise the original exception
            throw new IkatsDaoException("Can't delete "+ processId, e);
        }
        finally {
            // end the session
            session.close();
        }
    }
    
}
