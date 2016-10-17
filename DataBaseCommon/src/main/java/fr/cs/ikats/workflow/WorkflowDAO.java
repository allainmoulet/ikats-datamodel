package fr.cs.ikats.workflow;

import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.StaleStateException;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;
import org.hibernate.exception.ConstraintViolationException;

import fr.cs.ikats.common.dao.DataBaseDAO;
import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;

/**
 * DAO class for MetaData model class
 */
public class WorkflowDAO extends DataBaseDAO {

    /**
     * HQL requests
     */
    private static final String DELETE_BY_ID = "delete from Workflow wf where wf.id = :id";
    private static final String DELETE_ALL = "delete from Workflow WHERE isMacroOp is :macroOp";

    /**
     * Logger for WorkflowDAO
     */
    private static final Logger LOGGER = Logger.getLogger(WorkflowDAO.class);

    /**
     * List all workflows/Macro Operators
     *
     * @return the list of all workflow
     * @throws IkatsDaoMissingRessource if there is no workflow/Macro Operator
     * @throws IkatsDaoException        if any other exception occurs
     */
    List<Workflow> listAll(Boolean isMacroOp) throws IkatsDaoMissingRessource, IkatsDaoException {
        List<Workflow> result = null;
        Session session = getSession();

        try {
            Criteria criteria = session.createCriteria(Workflow.class);
            criteria.add(Restrictions.eq("isMacroOp", isMacroOp));
            result = criteria.list();

        } catch (HibernateException hibException) {
            String msg = "Exception occurred while getting all workflow";
            LOGGER.error(msg);
            throw new IkatsDaoException(msg);
        } catch (Exception error) {
            if (error instanceof IkatsDaoMissingRessource) {
                throw error;
            } else {
                throw new IkatsDaoException();
            }
        } finally {
            session.close();
        }

        return result;
    }

    /**
     * Delete all workflows/Macro Operators
     *
     * @param macroOp set to true to remove all MacroOperators, to false to remove workflows
     *
     * @return the number of deleted workflows/Macro Operators
     * @throws IkatsDaoException if workflows/Macro Operators couldn't be removed
     */
    public int removeAll(Boolean macroOp) throws IkatsDaoException {
        Session session = getSession();
        Transaction tx = null;
        int result = 0;
        try {
            tx = session.beginTransaction();

            Query query = session.createQuery(DELETE_ALL);
            query.setBoolean("macroOp", macroOp);
            result = query.executeUpdate();
            tx.commit();
        } catch (HibernateException e) {
            IkatsDaoException error = new IkatsDaoException("Deleting All workflows/Macro Operators", e);
            LOGGER.error(error);
            rollbackAndThrowException(tx, error);
        } finally {
            session.close();
        }
        return result;
    }

    /**
     * Get a workflow/Macro Operator by providing its id (which is unique)
     *
     * @param id Id of the workflow/Macro Operator to get
     * @return The workflow/Macro Operator matching this id
     * @throws IkatsDaoMissingRessource if there is no workflow/Macro Operator matching the id
     * @throws IkatsDaoException        if any other exception occurs
     */
    Workflow getById(Integer id) throws IkatsDaoMissingRessource, IkatsDaoException {
        List<Workflow> result = null;
        Session session = getSession();

        try {
            Criteria criteria = session.createCriteria(Workflow.class);
            criteria.add(Restrictions.eq("id", id));
            result = criteria.list();

            if (result == null || (result.size() == 0)) {
                String msg = "Searching workflow from id=" + id + ": no resource found, but should exist.";
                LOGGER.error(msg);

                throw new IkatsDaoMissingRessource(msg);
            }

        } catch (HibernateException hibException) {
            String msg = "Exception occurred while getting workflow id:" + id;
            LOGGER.error(msg);
            throw new IkatsDaoException(msg);
        } catch (Exception error) {
            if (error instanceof IkatsDaoMissingRessource) {
                throw error;
            } else {
                String msg = "Searching workflow from id=" + id + ": unexpected Exception.";
                LOGGER.error(msg);
                throw new IkatsDaoException(msg);
            }
        } finally {
            session.close();
        }

        return result.get(0);
    }

    /**
     * Save a workflow/Macro Operator
     *
     * @param wf the workflow/Macro Operator information to save
     * @return the id of the created workflow/Macro Operator
     * @throws IkatsDaoConflictException if the workflow/Macro Operator to append already exists
     * @throws IkatsDaoException         if any other exception occurs
     */
    public Integer persist(Workflow wf) throws IkatsDaoConflictException, IkatsDaoException {
        Session session = getSession();
        Transaction tx = null;
        Integer wfId = null;
        String wfInfo = "null";
        try {
            wfInfo = wf.toString();
            tx = session.beginTransaction();
            wfId = (Integer) session.save(wf);
            tx.commit();
            LOGGER.debug("Created " + wfInfo + " with id=" + wf.getId());
        } catch (ConstraintViolationException e) {

            String msg = "Creating: " + wfInfo + ": already exists in base for same id";
            LOGGER.warn(msg);

            rollbackAndThrowException(tx, new IkatsDaoConflictException(msg, e));
        } catch (HibernateException e) {
            String msg = "Creating: " + wfInfo + ": unexpected HibernateException";
            LOGGER.error(msg, e);

            rollbackAndThrowException(tx, new IkatsDaoException(msg, e));
        } catch (Exception anotherError) {
            // Deals with null pointer exceptions ...
            String msg = "Creating Workflow: " + wfInfo + ": unexpected Exception";
            LOGGER.error(msg, anotherError);

            rollbackAndThrowException(tx, new IkatsDaoException(msg, anotherError));
        } finally {
            session.close();
        }
        return wfId;
    }

    /**
     * Update the workflow/Macro Operator with the defined information
     *
     * @param wf the detailed information about the update
     * @return true if the workflow/Macro Operator update is successful
     * @throws IkatsDaoConflictException if the workflow/Macro Operator to update does not exist
     * @throws IkatsDaoException         if any other exception occurs
     */
    public boolean update(Workflow wf) throws IkatsDaoConflictException, IkatsDaoException {
        Session session = getSession();
        Transaction tx = null;
        String wfInfo = "null";
        boolean updated = false;
        try {
            wfInfo = wf.toString();
            tx = session.beginTransaction();
            session.update(wf);
            tx.commit();
            updated = true;
            LOGGER.debug("Updated:" + wfInfo + " with value=" + wf.getRaw());
        } catch (ConstraintViolationException e) {

            String msg = "Updating: " + wfInfo + ": already exists in base for same id";
            LOGGER.warn(msg);

            rollbackAndThrowException(tx, new IkatsDaoConflictException(msg, e));
        } catch (StaleStateException e) {

            String msg = "No match for Workflow with id:" + wf.getId();
            LOGGER.error(msg, e);
            rollbackAndThrowException(tx, new IkatsDaoMissingRessource(msg, e));

        } catch (HibernateException e) {
            String msg = "Updating: " + wfInfo + ": unexpected HibernateException";
            LOGGER.error(msg, e);

            rollbackAndThrowException(tx, new IkatsDaoException(msg, e));
        } catch (Exception anotherError) {
            // Deals with null pointer exceptions ...
            String msg = "Updating MetaData: " + wfInfo + ": unexpected Exception";
            LOGGER.error(msg, anotherError);

            rollbackAndThrowException(tx, new IkatsDaoException(msg, anotherError));
        } finally {
            session.close();
        }
        return updated;
    }

    /**
     * Delete a workflow/Macro Operator identified by its id
     *
     * @param id identifier of the workflow/Macro Operator
     * @throws IkatsDaoException if the workflow/Macro Operator couldn't be removed
     */
    public void removeById(Integer id) throws IkatsDaoException {
        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            Query query = session.createQuery(DELETE_BY_ID);
            query.setInteger("id", id);
            Integer deletedCount = query.executeUpdate();
            if (deletedCount == 0) {
                String msg = "No workflow exists with Id:" + id.toString();
                LOGGER.warn(msg);
                throw new IkatsDaoMissingRessource(msg);
            }

            tx.commit();
        } catch (HibernateException e) {
            IkatsDaoException error = new IkatsDaoException("Deleting Workflow rows matching id=" + id, e);
            LOGGER.error(error);
            rollbackAndThrowException(tx, error);
        } finally {
            session.close();
        }
    }

}
