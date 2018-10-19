/**
 * Copyright 2018 CS Systèmes d'Information
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.cs.ikats.workflow;

import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.StaleStateException;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;
import org.hibernate.exception.ConstraintViolationException;

import fr.cs.ikats.common.dao.DataBaseDAO;
import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingResource;

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
     * @throws IkatsDaoMissingResource if there is no workflow/Macro Operator
     * @throws IkatsDaoException        if any other exception occurs
     */
    List<WorkflowEntitySummary> listAll(Boolean isMacroOp) throws IkatsDaoException {
        List<WorkflowEntitySummary> result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Criteria criteria = session.createCriteria(WorkflowEntitySummary.class);
            criteria.add(Restrictions.eq("isMacroOp", isMacroOp));
            result = criteria.list();

            tx.commit();
        } catch (RuntimeException e) {
            if (tx != null) tx.rollback();
            // Re-raise the original exception
            throw e;
        } finally {

            // end the session
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
        int result = 0;

        Session session = getSession();
        Transaction tx = null;
        try {
            LOGGER.debug("Deleting all workflow with macroOp flag: " + macroOp);
            tx = session.beginTransaction();

            Query query = session.createQuery(DELETE_ALL);
            query.setBoolean("macroOp", macroOp);
            result = query.executeUpdate();

            tx.commit();
        } catch (RuntimeException e) {
            // try to rollback
            if (tx != null) tx.rollback();
            // Re-raise the original exception
            throw e;
        } finally {
            // end the session
            session.close();
        }

        return result;
    }

    /**
     * Get a workflow/Macro Operator by providing its id (which is unique)
     *
     * @param id Id of the workflow/Macro Operator to get
     * @return The workflow/Macro Operator matching this id
     * @throws IkatsDaoMissingResource if there is no workflow/Macro Operator matching the id
     * @throws IkatsDaoException        if any other exception occurs
     */
    Workflow getById(Integer id) throws IkatsDaoMissingResource, IkatsDaoException {
        Workflow result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Criteria criteria = session.createCriteria(Workflow.class);
            criteria.add(Restrictions.eq("id", id));
            List<Workflow> resultList = criteria.list();

            if (resultList == null || (resultList.size() == 0)) {
                String msg = "Searching workflow from id=" + id + ": no resource found, but should exist.";
                LOGGER.error(msg);
                rollbackAndThrowException(tx, new IkatsDaoMissingResource(msg));
            } else {
                result = resultList.get(0);
            }

            tx.commit();
        } catch (RuntimeException e) {
            if (tx != null) tx.rollback();
            // Re-raise the original exception
            throw e;
        } finally {
            session.close();
        }

        return result;
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
        Integer wfId = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            String wfInfo = wf.toString();
            LOGGER.debug("Creating " + wfInfo + " with id=" + wf.getId());

            wfId = (Integer) session.save(wf);
            tx.commit();
        } catch (RuntimeException e) {
            // try to rollback
            if (tx != null) tx.rollback();
            // Re-raise the original exception
            throw e;
        } finally {
            // end the session
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
    public boolean update(Workflow wf) throws IkatsDaoConflictException, IkatsDaoException, IkatsDaoMissingResource {
        boolean updated = false;

        Session session = getSession();
        Transaction tx = null;
        try {
            LOGGER.debug("Updating:" + wf.getName() + " with value=" + wf.getRaw());
            tx = session.beginTransaction();

            session.update(wf);
            tx.commit();
            updated = true;
        } catch (ConstraintViolationException e) {

            String msg = "Constraint violation : workflow name already exist";
            LOGGER.warn(msg);

            rollbackAndThrowException(tx, new IkatsDaoConflictException(msg, e));
        } catch (StaleStateException e) {

            String msg = "No match for Workflow with id:" + wf.getId();
            LOGGER.error(msg, e);
            rollbackAndThrowException(tx, new IkatsDaoMissingResource(msg, e));
        } catch (RuntimeException e) {
            // try to rollback
            if (tx != null) tx.rollback();
            // Re-raise the original exception
            throw e;
        } finally {
            // end the session
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
            LOGGER.debug("Deleting Workflow rows matching id=" + id);
            tx = session.beginTransaction();

            Query query = session.createQuery(DELETE_BY_ID);
            query.setInteger("id", id);
            Integer deletedCount = query.executeUpdate();
            if (deletedCount == 0) {
                String msg = "No workflow exists with Id:" + id.toString();
                LOGGER.warn(msg);
                session.getTransaction().rollback();
                throw new IkatsDaoMissingResource(msg);
            }

            tx.commit();
        } catch (RuntimeException e) {
            // try to rollback
            if (tx != null) tx.rollback();
            // Re-raise the original exception
            throw e;
        } finally {
            // end the session
            session.close();
        }

    }

}
