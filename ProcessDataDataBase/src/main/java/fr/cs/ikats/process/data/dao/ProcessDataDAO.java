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
 * @author Fabien TORAL <fabien.toral@c-s.fr>
 * @author Fabien TORTORA <fabien.tortora@c-s.fr>
 * @author Mathieu BERAUD <mathieu.beraud@c-s.fr>
 * @author Maxime PERELMUTER <maxime.perelmuter@c-s.fr>
 * 
 */

package fr.cs.ikats.process.data.dao;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.MatchMode;
import org.hibernate.criterion.Restrictions;
import org.hibernate.impl.SessionFactoryImpl;

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
        	
        	// -- Trick to change the query depending on the database
        	// getSessionFactory doesn't expose th dialect property in our Hibernate 3.3.
        	// Use Java reflexion instead
        	Criterion procDataTableMatchCrit;
        	Field f = SessionFactoryImpl.class.getDeclaredField("properties");
        	f.setAccessible(true);
        	Properties p = (Properties)f.get(session.getSessionFactory());
        	
        	String dialect = p.getProperty("hibernate.dialect");
        	if (dialect.toString().equals("org.hibernate.dialect.PostgreSQLDialect")) {
        	    // For PostgreSQL
        	    procDataTableMatchCrit = Restrictions.sqlRestriction("{alias}.processid ~ '^[a-zA-Z]+$'");
        	} else {
        	    // for HSQLDB when in Unit Tests
        	    procDataTableMatchCrit = Restrictions.sqlRestriction("regexp_matches({alias}.processid, '^[a-zA-Z]+$')");
        	}
        	
            Criteria criteria = session.createCriteria(ProcessData.class);
            criteria.add(procDataTableMatchCrit);
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
        catch (RuntimeException | NoSuchFieldException | IllegalAccessException e) {
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

