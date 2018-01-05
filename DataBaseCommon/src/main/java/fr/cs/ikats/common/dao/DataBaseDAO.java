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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * @author Fabien TORAL <fabien.toral@c-s.fr>
 * @author Fabien TORTORA <fabien.tortora@c-s.fr>
 */

package fr.cs.ikats.common.dao;


import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.AnnotationConfiguration;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoRollbackException;

/**
 * Abstract class for DAO. init the hibernate configuration by annotations. and
 * get the session Factory when configuration is complete.
 */
public abstract class DataBaseDAO {

    private static Logger LOGGER = Logger.getLogger(DataBaseDAO.class);
    private AnnotationConfiguration configuration;
    private SessionFactory sessionFactory;

    /**
     * init an AnnotationConfiguration with a minimal Hibernate.cfg.xml read from the classpath
     *
     * @param hibernateConfigurationFile the hibernate configuration file
     */
    public void init(String hibernateConfigurationFile) {
        configuration = new AnnotationConfiguration().configure(hibernateConfigurationFile);
    }

    /**
     * create the session factory from configuration. After call to this method,
     * nothing can be changed in configuration
     */
    public void completeConfiguration() {
        if (sessionFactory != null) {
            LOGGER.error("Configuration is already complete, nothing done");
        } else {
            sessionFactory = configuration.buildSessionFactory();
        }
    }

    /**
     * register an annotated class to the hibernate configuration
     *
     * @param annotatedClass the class with annotations
     */
    public void addAnnotatedClass(Class<?> annotatedClass) {
        if (sessionFactory != null) {
            LOGGER.error("Configuration is already complete, nothing done");
        } else {
            configuration.addAnnotatedClass(annotatedClass);
        }

    }

    /**
     * register an annotated package to the hibernate configuration
     *
     * @param packageName complete name of the package
     */
    public void addAnotatedPackage(String packageName) {
        configuration.addPackage(packageName);
    }

    /**
     * get a session from the session factory throw a runtime exception if
     * configuration is not complete.
     *
     * @return a session.
     */
    public Session getSession() throws IkatsDaoException {
        Session result = null;
        if (isReady()) {
            result = sessionFactory.openSession();
        } else {
            String message = "Manager not initialize, call complete configuration first";
            LOGGER.error(message);
            throw new IkatsDaoException(message);
        }
        return result;
    }

    /**
     * check if configuration has been completed by testing sessionFactory
     * instanciation
     *
     * @return true if session factory is not null
     */
    protected boolean isReady() {
        return (sessionFactory != null);
    }

    /**
     * close the sessionFactory
     */
    public void stop() {
        LOGGER.trace("Stopping DataBase Dao : " + getClass());
        sessionFactory.close();
    }

    /**
     * Return configured property
     *
     * @param propertyName
     * @return configuration.getProperty(propertyName)
     */
    public String getAnnotationConfigurationProperty(String propertyName) {
        if (configuration != null) {
            return configuration.getProperty(propertyName);
        } else {
            return null;
        }
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
            LOGGER.error(e);
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

