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

package fr.cs.ikats.table;

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
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingResource;

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
     * @throws HibernateException if there is no TableEntity
     */
    public List<TableEntitySummary> listAll() {
        List<TableEntitySummary> result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Criteria criteria = session.createCriteria(TableEntitySummary.class);
            result = criteria.list();

            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) {
                tx.rollback();
            }
            // Re-raise the original exception
            throw e;
        } finally {

            // end the session
            session.close();
        }

        return result;
    }

    /**
     * List all Tables matching the pattern
     *
     * @param pattern The pattern to match
     * @param strict  set to true to have a strict match.
     *                false indicate the pattern shall be contained in the name
     *
     * @return the list of all tables
     *
     * @throws HibernateException if there is no TableEntity matching pattern
     */
    public List<TableEntitySummary> findByName(String pattern, boolean strict) {
        List<TableEntitySummary> result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Criteria criteria = session.createCriteria(TableEntitySummary.class);
            if (strict) {
                // Strict match
                criteria.add(Restrictions.eq("name", pattern));
            } else {
                // The query shall be contained in the Name
                String query = '%' + pattern.replace('*', '%') + '%';
                criteria.add(Restrictions.like("name", query));
            }
            result = criteria.list();

            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) {
                tx.rollback();
            }
            // Re-raise the original exception
            throw e;
        } finally {

            // end the session
            session.close();
        }

        return result;
    }


    /**
     * Get a TableEntity by providing its id (which is unique)
     *
     * @param id unique id of the TableEntity to get
     *
     * @return The TableEntity matching this id
     *
     * @throws IkatsDaoMissingResource if there is no TableEntity matching the id
     */
    public TableEntity getById(Integer id) throws IkatsDaoMissingResource {
        TableEntity result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Query query = session.createQuery("from TableEntity fetch all properties where id = ?");
            query.setParameter(0, id);
            result = (TableEntity) query.uniqueResult();

            if (result == null) {
                String msg = "Table " + id + " not found";
                LOGGER.error(msg);
                tx.rollback();
                throw new IkatsDaoMissingResource(msg);
            } else {
                tx.commit();
            }

        } catch (RuntimeException e) {
            if (tx != null) {
                tx.rollback();
            }
            // Re-raise the original exception
            throw e;
        } finally {
            session.close();
        }

        return result;
    }


    /**
     * Get a TableEntity by providing its name (which is unique)
     *
     * @param name unique name of the TableEntity to get
     *
     * @return The TableEntity matching this name
     *
     * @throws IkatsDaoMissingResource if there is no TableEntity matching the name
     */
    public TableEntity getByName(String name) throws IkatsDaoMissingResource {
        TableEntity result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Query query = session.createQuery("from TableEntity fetch all properties where name = ?");
            query.setString(0, name);
            result = (TableEntity) query.uniqueResult();

            if (result == null) {
                String msg = "Table " + name + " not found";
                LOGGER.error(msg);
                tx.rollback();
                throw new IkatsDaoMissingResource(msg);
            } else {
                tx.commit();
            }

        } catch (RuntimeException e) {
            if (tx != null) {
                tx.rollback();
            }
            // Re-raise the original exception
            throw e;
        } finally {
            session.close();
        }

        return result;
    }


    /**
     * Save a table
     *
     * @param tableEntity the table information to save
     *
     * @return the id of the created table
     *
     * @throws IkatsDaoConflictException if the table to append already exists
     */
    public Integer persist(TableEntity tableEntity) throws IkatsDaoConflictException {
        Integer tableId = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            LOGGER.debug("Creating " + tableEntity.getName() + " with id=" + tableEntity.getId());

            tableId = (Integer) session.save(tableEntity);
            tx.commit();
        } catch (ConstraintViolationException e) {
            // try to rollback
            if (tx != null) {
                tx.rollback();
            }
            // Raise the exception into a specific IKATS one to allow its handling with IKATS specific handler for HTTP response  
            throw new IkatsDaoConflictException(e);
        } catch (HibernateException e) {
            // try to rollback
            if (tx != null) {
                tx.rollback();
            }
            // Re-raise the original exception
            throw e;
        } finally {
            // end the session
            session.close();
        }

        return tableId;
    }

    /**
     * Update the table with the defined information
     *
     * @param tableEntity the detailed information about the update
     *
     * @return true if the table update is successful
     *
     * @throws IkatsDaoConflictException if the table to update does not exist
     * @throws IkatsDaoException         if any other exception occurs
     */
    public boolean update(TableEntity tableEntity) throws IkatsDaoException {
        boolean updated = false;

        Session session = getSession();
        Transaction tx = null;
        try {
            LOGGER.debug("Updating:" + tableEntity.getName());
            tx = session.beginTransaction();

            session.update(tableEntity);
            tx.commit();
            updated = true;
        } catch (StaleStateException e) {

            String msg = "No match for TableEntity with id:" + tableEntity.getId();
            LOGGER.error(msg, e);
            rollbackAndThrowException(tx, new IkatsDaoMissingResource(msg, e));
        } catch (RuntimeException e) {
            // try to rollback
            if (tx != null) {
                tx.rollback();
            }
            // Re-raise the original exception
            throw e;
        } finally {
            // end the session
            session.close();
        }

        return updated;
    }

    /**
     * Delete a Table identified by its id
     *
     * @param id identifier of the table
     *
     * @throws HibernateException if the table couldn't be removed
     */
    public void removeById(Integer id) {

        Session session = getSession();
        Transaction tx = null;
        try {
            LOGGER.debug("Deleting TableEntity rows matching id=" + id);
            tx = session.beginTransaction();

            TableEntity tableEntity = new TableEntity();
            tableEntity.setId(id);
            session.delete(tableEntity);

            tx.commit();
        } catch (HibernateException e) {
            // try to rollback
            if (tx != null) {
                tx.rollback();
            }
            // Re-raise the original exception
            throw e;
        } finally {
            // end the session
            session.close();
        }

    }
}
