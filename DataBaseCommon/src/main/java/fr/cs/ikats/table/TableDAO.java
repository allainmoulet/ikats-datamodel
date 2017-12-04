/**
 * LICENSE:
 * --------
 * Copyright 2017 CS SYSTEMES D'INFORMATION
 *
 * Licensed to CS SYSTEMES D'INFORMATION under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. CS licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * @author Fabien TORTORA <fabien.tortora@c-s.fr>
 * @author Fabien TORAL <fabien.toral@c-s.fr>
 *
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

import fr.cs.ikats.common.dao.DataBaseDAO;
import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;

public class TableDAO extends DataBaseDAO {

    /**
     * Logger for TableDAO
     */
    private static final Logger LOGGER = Logger.getLogger(TableDAO.class);

    private DatabaseManager instance;

    public TableDAO() {
        instance = DatabaseManager.getInstance();
    }

    /* (non-Javadoc)
     * @see fr.cs.ikats.common.dao.DataBaseDAO#getSession()
     */
    @Override
    public Session getSession() {
        return instance.getSession();
    }

    /**
     * List all Tables
     *
     * @return the list of all tables
     *
     * @throws HibernateException if there is no TableEntity
     */
    public List<TableEntity> listAll() throws HibernateException {
        List<TableEntity> result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Criteria criteria = session.createCriteria(TableEntity.class);
            result = criteria.list();

            tx.commit();
        }
        catch (HibernateException e) {
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
     * @param strict  set to true to have a strict match.
     *                false indicate the pattern shall be contained in the name
     *
     * @return the list of all tables
     *
     * @throws HibernateException if there is no TableEntity matching pattern
     */
    public List<TableEntity> findByName(String pattern, boolean strict) throws HibernateException {
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
        catch (HibernateException e) {
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
     * @param id unique id of the TableEntity to get
     *
     * @return The TableEntity matching this id
     *
     * @throws IkatsDaoMissingRessource if there is no TableEntity matching the id
     */
    public TableEntity getById(Integer id) throws IkatsDaoMissingRessource {
        TableEntity result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Query query = session.createQuery("from TableEntity fetch all properties where id = :id");
            query.setParameter(id, id);
            result = (TableEntity) query.uniqueResult();

            if (result == null) {
                String msg = "Table " + id + " not found";
                LOGGER.error(msg);
                tx.rollback();
                throw new IkatsDaoMissingRessource(msg);
            }
            else {
                tx.commit();
            }

        }
        catch (RuntimeException e) {
            if (tx != null) {
                tx.rollback();
            }
            // Re-raise the original exception
            throw e;
        }
        finally {
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
     * @throws IkatsDaoMissingRessource if there is no TableEntity matching the name
     */
    public TableEntity getByName(String name) throws IkatsDaoMissingRessource {
        TableEntity result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Query query = session.createQuery("from TableEntity fetch all properties where name = :name");
            query.setParameter("name", name);
            result = (TableEntity) query.uniqueResult();

            if (result == null) {
                String msg = "Table " + name + " not found";
                LOGGER.error(msg);
                tx.rollback();
                throw new IkatsDaoMissingRessource(msg);
            }
            else {
                tx.commit();
            }

        }
        catch (RuntimeException e) {
            if (tx != null) {
                tx.rollback();
            }
            // Re-raise the original exception
            throw e;
        }
        finally {
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
     * @throws HibernateException if the table to append already exists
     */
    public Integer persist(TableEntity tableEntity) throws HibernateException {
        Integer tableId = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            LOGGER.debug("Creating " + tableEntity.getName() + " with id=" + tableEntity.getId());

            tableId = (Integer) session.save(tableEntity);
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
     * Delete a Table identified by its id
     *
     * @param id identifier of the table
     *
     * @throws IkatsDaoException if the table couldn't be removed
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
