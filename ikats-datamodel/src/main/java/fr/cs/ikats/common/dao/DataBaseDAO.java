/**
 * Copyright 2018-2019 CS Systèmes d'Information
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

package fr.cs.ikats.common.dao;


import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoRollbackException;

/**
 * Abstract class for DAO. init the hibernate configuration by annotations. and
 * get the session Factory when configuration is complete.
 */
public abstract class DataBaseDAO {

    private static Logger logger = Logger.getLogger(DataBaseDAO.class);

    private DatabaseManager instance;
    
    protected DataBaseDAO() {
        instance = DatabaseManager.getInstance();
    }

    /**
     * get a session from the session factory throw a runtime exception if
     * configuration is not complete.
     *
     * @return a session.
     */
    public Session getSession() {
        return instance.getSession();
    }

    /**
     * close the sessionFactory
     */
    public void stop() {
        logger.trace("Stopping DataBase Dao : " + getClass());
        instance.getSessionFactory().close();
    }

    /**
     * This method sums up a DAO action made on a transaction:
     * <ul>
     * <li>Try to roll-back the transaction.</li>
     * <li>And then throws the defined exception.</li>
     * </ul>
     * Specific case: when the roll-back fails: the method raises
     * a new IkatsDaoRollbackException( causeException ).
     *
     * @param rolledBackTransaction transaction on which roll-back is applied
     * @param causeException        thrown exception after roll-back
     * @throws IkatsDaoException defined causeException or else a IkatsDaoRollbackException
     */
    public final void rollbackAndThrowException(Transaction rolledBackTransaction, IkatsDaoException causeException)
            throws IkatsDaoException {
        try {
            if (rolledBackTransaction != null) {
                rolledBackTransaction.rollback();
            }
        } catch (HibernateException e) {
            logger.error(e);
            throw new IkatsDaoRollbackException(causeException);
        }

        throw causeException;
    }

    /**
     * Wrapps the HibernateException into the GOOD subclass of IkatsDaoException, which will be handled by Web application error handlers.
     *
     * @param ikatsMessage
     * @param hibernateException
     * @return
     */
    public IkatsDaoException buildDaoException(String ikatsMessage, HibernateException hibernateException) {
        IkatsDaoException error;
        if (hibernateException.getMessage().indexOf("Could not execute JDBC batch update") >= 0) {
            error = new IkatsDaoConflictException(ikatsMessage + " already existing in database.", hibernateException);
        } else {
            error = new IkatsDaoException(ikatsMessage, hibernateException);
        }
        return error;
    }
    
}
