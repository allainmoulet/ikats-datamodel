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

package fr.cs.ikats.common.dao;

import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.AnnotationConfiguration;

import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.process.data.model.ProcessData;
import fr.cs.ikats.table.TableEntity;
import fr.cs.ikats.table.TableEntitySummary;
import fr.cs.ikats.ts.dataset.model.DataSet;
import fr.cs.ikats.ts.dataset.model.LinkDatasetTimeSeries;
import fr.cs.ikats.workflow.Workflow;
import fr.cs.ikats.workflow.WorkflowEntitySummary;

/**
 * Manages the initialization of the database for entities through a singleton
 */
public class DatabaseManager {

    private static final String HIBERNATE_CONFIG_FILE = "hibernate.cfg.xml";
    private static Logger logger = Logger.getLogger(DataBaseDAO.class);
    private AnnotationConfiguration configuration;
    private SessionFactory sessionFactory;

    private static DatabaseManager instance = null;

    static {
        if (instance == null) {
            instance = new DatabaseManager();
        }
    }

    /**
     * Create a management instance for the Hibernate session factory 
     */
    public DatabaseManager() {
        try {
            configuration = new AnnotationConfiguration().configure(HIBERNATE_CONFIG_FILE);
        } catch (HibernateException ex) {
            logger.error("Failed to create Initialize IKATS Database", ex);
            throw new ExceptionInInitializerError(ex);
        }

	    configuration.addPackage("fr.cs.ikats.ts.dataset.model");
        configuration.addAnnotatedClass(DataSet.class);
        configuration.addAnnotatedClass(LinkDatasetTimeSeries.class);
        
        configuration.addPackage("fr.cs.ikats.metadata.model");
        configuration.addAnnotatedClass(MetaData.class);
        configuration.addAnnotatedClass(FunctionalIdentifier.class);
        
        configuration.addPackage("fr.cs.ikats.process.data.model");
        configuration.addAnnotatedClass(ProcessData.class);
        
        configuration.addPackage("fr.cs.ikats.workflow");
        configuration.addAnnotatedClass(Workflow.class);
        configuration.addAnnotatedClass(WorkflowEntitySummary.class);        
        
        configuration.addPackage("fr.cs.ikats.table");
        configuration.addAnnotatedClass(TableEntity.class);
        configuration.addAnnotatedClass(TableEntitySummary.class);

        sessionFactory = configuration.buildSessionFactory();
    }

    /**
     * Return the unique instance of the manger
     * @return
     */
    public static DatabaseManager getInstance() {
        return instance;
    }

    /**
     * Get a session from the session factory
     *
     * @return a session
     */
    public Session getSession() {
        return sessionFactory.openSession();
    }

    /**
     * @return the session factory
     */
	public SessionFactory getSessionFactory() {
		return sessionFactory;
	}
}
