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

    /**
     * Logger for TableDAO
     */
    private static final Logger LOGGER = Logger.getLogger(TableDAO.class);

    /**
     * List all Tables
     *
     * @return the list of all tables
     *
     * @throws fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource if there is no TableEntity
     * @throws fr.cs.ikats.common.dao.exception.IkatsDaoException        if any other exception occurs
     */
    public List<TableEntity> listAll() throws IkatsDaoMissingRessource, IkatsDaoException {
        List<TableEntity> result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Criteria criteria = session.createCriteria(TableEntity.class);
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
     * List all Tables matching the pattern
     *
     * @param pattern The pattern to match
     * @param strict  set to True to have a strict match.
     *                False indicate the pattern shall be contained in the name
     *
     * @return the list of all tables
     *
     * @throws fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource if there is no TableEntity
     * @throws fr.cs.ikats.common.dao.exception.IkatsDaoException        if any other exception occurs
     */
    public List<TableEntity> findByName(String pattern, boolean strict) throws IkatsDaoMissingRessource, IkatsDaoException {
        List<TableEntity> result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Criteria criteria = session.createCriteria(TableEntity.class);
            if (strict) {
                // Strict match
                criteria.add(Restrictions.eq("name", pattern));
            }
            else {
                // The query shall be contained in the Name
                String query = '%' + pattern.replace('*', '%') + '%';
                criteria.add(Restrictions.like("name", query));
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
     * Get a TableEntity by providing its id (which is unique)
     *
     * @param id Id of the TableEntity to get
     *
     * @return The TableEntity matching this id
     *
     * @throws IkatsDaoMissingRessource if there is no TableEntity matching the id
     * @throws IkatsDaoException        if any other exception occurs
     */
    public TableEntity getById(Integer id) throws IkatsDaoMissingRessource, IkatsDaoException {
        TableEntity result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Criteria criteria = session.createCriteria(TableEntity.class);
            criteria.add(Restrictions.eq("id", id));
            List<TableEntity> resultList = criteria.list();

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
     * Save a TableEntity
     *
     * @param tableEntity the TableEntity information to save
     *
     * @return the id of the created TableEntity
     *
     * @throws fr.cs.ikats.common.dao.exception.IkatsDaoConflictException if the TableEntity to append already exists
     * @throws IkatsDaoException                                          if any other exception occurs
     */
    public Integer persist(TableEntity tableEntity) throws IkatsDaoConflictException, IkatsDaoException {
        Integer tableId = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            String wfInfo = tableEntity.toString();
            LOGGER.debug("Creating " + wfInfo + " with id=" + tableEntity.getId());

            tableId = (Integer) session.save(tableEntity);
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
     * @param tableEntity the detailed information about the update
     *
     * @return true if the workflow/Macro Operator update is successful
     *
     * @throws IkatsDaoConflictException if the workflow/Macro Operator to update does not exist
     * @throws IkatsDaoException         if any other exception occurs
     */
    public boolean update(TableEntity tableEntity) throws IkatsDaoConflictException, IkatsDaoException {
        boolean updated = false;

        Session session = getSession();
        Transaction tx = null;
        try {
            LOGGER.debug("Updating:" + tableEntity.getName());
            tx = session.beginTransaction();

            session.update(tableEntity);
            tx.commit();
            updated = true;
        }
        catch (StaleStateException e) {

            String msg = "No match for TableEntity with id:" + tableEntity.getId();
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
            LOGGER.debug("Deleting TableEntity rows matching id=" + id);
            tx = session.beginTransaction();

            TableEntity tableEntity = new TableEntity();
            tableEntity.setId(id);
            session.delete(tableEntity);

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
