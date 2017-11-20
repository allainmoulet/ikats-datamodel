package fr.cs.ikats.table;

import java.util.List;

import fr.cs.ikats.common.dao.DataBaseDAO;
import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.StaleStateException;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;

public class TableDAO extends DataBaseDAO {

//    /**
//     * Logger for TableDAO
//     */
//    private static final Logger LOGGER = Logger.getLogger(TableDAO.class);

    /**
     * List all Tables
     *
     * @return the list of all tables
     *
     * @throws fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource if there is no Table
     * @throws fr.cs.ikats.common.dao.exception.IkatsDaoException        if any other exception occurs
     */
    List<Table> listAll() throws IkatsDaoMissingRessource, IkatsDaoException {
        List<Table> result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Criteria criteria = session.createCriteria(Table.class);
            result = criteria.list();

            tx.commit();
        }
        catch (RuntimeException e) {
            if (tx != null) tx.rollback();
            // Re-raise the original exception
            throw e;
        }
        finally {

            // end the session
            session.close();
        }

        return result;
    }

    /**
     * Logger for TableDAO
     */
    private static final Logger LOGGER = Logger.getLogger(TableDAO.class);

    /**
     * List all Tables matching the pattern
     *
     * @param pattern The pattern to match
     * @param strict set to True to have a strict match.
     *               False indicate the pattern shall be contained in the name
     * @return the list of all tables
     *
     * @throws fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource if there is no Table
     * @throws fr.cs.ikats.common.dao.exception.IkatsDaoException        if any other exception occurs
     */
    List<Table> findByName(String pattern, boolean strict) throws IkatsDaoMissingRessource, IkatsDaoException {
        List<Table> result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Criteria criteria = session.createCriteria(Table.class);
            if (strict) {
                // Strict match
                criteria.add(Restrictions.eq("label", pattern));
            }
            else {
                // The query shall be contained in the Name
                String query = '%' + pattern.replace('*', '%') + '%';
                criteria.add(Restrictions.like("label", query));
            }
            result = criteria.list();

            tx.commit();
        }
        catch (RuntimeException e) {
            if (tx != null) tx.rollback();
            // Re-raise the original exception
            throw e;
        }
        finally {

            // end the session
            session.close();
        }

        return result;
    }

    /**
     * Get a Table by providing its id (which is unique)
     *
     * @param id Id of the Table to get
     *
     * @return The Table matching this id
     *
     * @throws IkatsDaoMissingRessource if there is no Table matching the id
     * @throws IkatsDaoException        if any other exception occurs
     */
    Table getById(Integer id) throws IkatsDaoMissingRessource, IkatsDaoException {
        Table result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Criteria criteria = session.createCriteria(Table.class);
            criteria.add(Restrictions.eq("id", id));
            List<Table> resultList = criteria.list();

            if (resultList == null || (resultList.size() == 0)) {
                String msg = "Searching workflow from id=" + id + ": no resource found, but should exist.";
                LOGGER.error(msg);
                rollbackAndThrowException(tx, new IkatsDaoMissingRessource(msg));
            }
            else {
                result = resultList.get(0);
            }

            tx.commit();
        }
        catch (RuntimeException e) {
            if (tx != null) tx.rollback();
            // Re-raise the original exception
            throw e;
        }
        finally {
            session.close();
        }

        return result;
    }

    /**
     * Save a Table
     *
     * @param table the Table information to save
     *
     * @return the id of the created Table
     *
     * @throws fr.cs.ikats.common.dao.exception.IkatsDaoConflictException if the Table to append already exists
     * @throws IkatsDaoException                                          if any other exception occurs
     */
    public Integer persist(Table table) throws IkatsDaoConflictException, IkatsDaoException {
        Integer tableId = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            String wfInfo = table.toString();
            LOGGER.debug("Creating " + wfInfo + " with id=" + table.getId());

            tableId = (Integer) session.save(table);
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

        return tableId;
    }

    /**
     * Update the workflow/Macro Operator with the defined information
     *
     * @param table the detailed information about the update
     *
     * @return true if the workflow/Macro Operator update is successful
     *
     * @throws IkatsDaoConflictException if the workflow/Macro Operator to update does not exist
     * @throws IkatsDaoException         if any other exception occurs
     */
    public boolean update(Table table) throws IkatsDaoConflictException, IkatsDaoException {
        boolean updated = false;

        Session session = getSession();
        Transaction tx = null;
        try {
            LOGGER.debug("Updating:" + table.getLabel());
            tx = session.beginTransaction();

            session.update(table);
            tx.commit();
            updated = true;
        }
        catch (StaleStateException e) {

            String msg = "No match for Table with id:" + table.getId();
            LOGGER.error(msg, e);
            rollbackAndThrowException(tx, new IkatsDaoMissingRessource(msg, e));
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

        return updated;
    }

    /**
     * Delete a workflow/Macro Operator identified by its id
     *
     * @param id identifier of the workflow/Macro Operator
     *
     * @throws IkatsDaoException if the workflow/Macro Operator couldn't be removed
     */
    public void removeById(Integer id) throws IkatsDaoException {

        Session session = getSession();
        Transaction tx = null;
        try {
            LOGGER.debug("Deleting Table rows matching id=" + id);
            tx = session.beginTransaction();

            Table table = new Table();
            table.setId(id);
            session.delete(table);

            tx.commit();
        }
        catch (HibernateException e) {
            // try to rollback
            if (tx != null) tx.rollback();
            // Re-raise the original exception
            throw e;
        }
        finally {
            // end the session
            session.close();
        }

    }
}
