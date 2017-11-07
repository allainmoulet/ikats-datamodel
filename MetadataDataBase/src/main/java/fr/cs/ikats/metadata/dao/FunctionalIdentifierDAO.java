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
 * @author Pierre BONHOURE <pierre.bonhoure@c-s.fr>
 * 
 */

package fr.cs.ikats.metadata.dao;

import java.util.ArrayList;
import java.util.List;

import fr.cs.ikats.common.dao.DataBaseDAO;
import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.Hibernate;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.transform.Transformers;

/**
 * FunctionalIdentifierDAO is providing CRUD services on the resource FunctionalIdentifier. <br/>
 * Note: replaced org.hibernate.HibernateException by Throwable in catch blocs of services.
 */
public class FunctionalIdentifierDAO extends DataBaseDAO {

	private static final Logger LOGGER = Logger.getLogger(FunctionalIdentifierDAO.class);

	/**
	 * Maximum limit of the SQL 'IN' clause, that cause JDBC driver to hang. TODO think to move this elsewhere...
	 */
	private static final int MAX_SQL_IN_CLAUSE_LIMIT = 20000;

	/**
	 * persist FunctionalIdentifier into database
	 *
	 * @param fi
	 *            FunctionalIdentifier
	 *
	 * @return the internal id
	 */
	public int persist(FunctionalIdentifier fi) throws IkatsDaoConflictException, IkatsDaoException {

		int result = 0;

		String tsuid= (fi != null) ? fi.getTsuid() : null;
		String funcId = ( fi != null ) ? fi.getFuncId() : null;
		
		Session session = getSession();
		Transaction tx = null;
		try {
			tx = session.beginTransaction();
			session.save(fi);
			tx.commit();
			LOGGER.debug("FunctionalIdentifier stored " + tsuid + ";" + funcId);
			result++;
		} catch (ConstraintViolationException e) {

			String msg = "Writting FunctionalIdentifier for tsuid=" + tsuid + "failed : constraint violation";
			LOGGER.warn(msg);

			rollbackAndThrowException(tx, new IkatsDaoConflictException(msg, e));
		} catch (HibernateException e) {
			String msg = "Writting FunctionalIdentifier for tsuid=" + tsuid + "failed : does not exist";
			LOGGER.error(msg, e);

			rollbackAndThrowException(tx, new IkatsDaoException(msg, e));
		} catch (RuntimeException e) {
			if (tx != null)
				tx.rollback();
			LOGGER.error("Not stored: " + ((fi != null) ? fi.toString() : "FunctionalIdentifier is null"));
			LOGGER.error(e.getClass().getSimpleName() + "Throwable in FunctionalIdentifierDAO::persist", e);
			// Re-raise the original exception
			throw e;
		} finally {
			session.close();
		}

		return result;
	}

	/**
	 * remove FunctionalIdentifier from database.
	 *
	 * @param tsuidList
	 *            the list of identifiers
	 *
	 * @return number of removals
	 */
	public int remove(List<String> tsuidList) throws IkatsDaoConflictException, IkatsDaoException {
		Session session = getSession();
		Transaction tx = null;
		int result = 0;
		String currentTsuid = null;
		try {
			tx = session.beginTransaction();
			for (String tsuid : tsuidList) {
				currentTsuid = tsuid;
				String hql = "delete from FunctionalIdentifier where tsuid= :uid";
				Query query = session.createQuery(hql);
				query.setString("uid", tsuid);
				result = query.executeUpdate();
			}
			tx.commit();
		} catch (ConstraintViolationException e) {
			String msg = "Removing FunctionalIdentifier for tsuid=" + currentTsuid + "failed : constraint violation";
			LOGGER.warn(msg);

			// Re-raise the original exception
			rollbackAndThrowException(tx, new IkatsDaoConflictException(msg, e));
		} catch (HibernateException e) {
			String msg = "Removing FunctionalIdentifier for tsuid=" + currentTsuid + "failed : does not exist";
			LOGGER.error(msg, e);

			// Re-raise the original exception
			rollbackAndThrowException(tx, new IkatsDaoException(msg, e));
		} finally {
			session.close();
		}

		return result;
	}

	/**
	 * Get the list of each FunctionalIdentifier matching the tsuids list. See FunctionalIdentifier: stands for a pair
	 * (tsuid, functional ID).
	 *
	 * @param tsuids
	 *            the criterion list.
	 *
	 * @return null if nothing is found, or error occured.
	 */
	@SuppressWarnings("unchecked")
	public List<FunctionalIdentifier> list(List<String> tsuids) {

		List<FunctionalIdentifier> result = new ArrayList<>();
		List<List<String>> tsuidSublists = new ArrayList<List<String>>();

		if (tsuids == null || tsuids.isEmpty()) {
			return result;
		}

		if (tsuids.size() > MAX_SQL_IN_CLAUSE_LIMIT) {
			// Limit raised : cut list in sublists
			int from = 0;
			// note : upperbound is exclusive for List.subList(from, to)
			int to = MAX_SQL_IN_CLAUSE_LIMIT;

			while (from < tsuids.size()) {
				tsuidSublists.add(tsuids.subList(from, to));

				// set next lower and upper bounds, limited to tsuid list size.
				from = to;
				to += MAX_SQL_IN_CLAUSE_LIMIT;
				if (to > tsuids.size()) {
					to = tsuids.size();
				}
			}
		} else {
			// Limit not raised : add the entire list
			tsuidSublists.add(tsuids);
		}

		Session session = getSession();
		Transaction tx = null;
		try {
			tx = session.beginTransaction();

			for (List<String> tsuidList : tsuidSublists) {
				// loop over each sublists (if more than one) and concatenate
				// the result
				Criteria criteria = session.createCriteria(FunctionalIdentifier.class);
				criteria.add(Restrictions.in("tsuid", tsuidList));
				result.addAll(criteria.list());
			}

			tx.commit();
		} catch (RuntimeException e) {
			if (tx != null)
				tx.rollback();
			throw e; // or display error message
		} finally {
			// end the session
			session.close();
		}

		return result;
	}

	/**
	 * Get the list of each FunctionalIdentifier matching the datasetName. See FunctionalIdentifier: stands for a pair
	 * (tsuid, functional ID).
	 *
	 * @param datasetName
	 *            the name of the dataset to use.
	 *
	 * @return a list of FunctionalIdentifier, or null if nothing is found
	 */
	@SuppressWarnings("unchecked")
	public List<FunctionalIdentifier> listFromDataset(String datasetName) throws IkatsDaoException {
		List<FunctionalIdentifier> result;

		Session session = getSession();
		Transaction tx = null;
		try {
			tx = session.beginTransaction();

			String queryString = "SELECT " + "tsfunctionalidentifier.tsuid as tsuid, "
					+ "tsfunctionalidentifier.funcid as FuncId " + "FROM tsfunctionalidentifier, timeseries_dataset "
					+ "WHERE tsfunctionalidentifier.tsuid = timeseries_dataset.tsuid AND "
					+ "timeseries_dataset.dataset_name = '" + datasetName + "'" + "ORDER BY FuncId";

			Query q = session.createSQLQuery(queryString).addScalar("tsuid", Hibernate.STRING)
					.addScalar("tsuid", Hibernate.STRING).addScalar("FuncId", Hibernate.STRING)
					.setResultTransformer(Transformers.aliasToBean(FunctionalIdentifier.class));

			result = (List<FunctionalIdentifier>) q.list();

			tx.commit();
		} catch (RuntimeException e) {
			if (tx != null)
				tx.rollback();
			throw e; // or display error message
		} finally {
			// end the session
			session.close();
		}

		return result;
	}

	/**
	 * Get the list of each FunctionalIdentifier matching the funcIds list. See FunctionalIdentifier: stands for a pair
	 * (tsuid, functional ID).
	 *
	 * @param funcIds
	 *            the criterion list
	 *
	 * @return null if nothing is found, or error occured.
	 */
	public List<FunctionalIdentifier> listByFuncIds(List<String> funcIds) {

		List<FunctionalIdentifier> result = null;

		Session session = getSession();
		Transaction tx = null;
		try {
			tx = session.beginTransaction();

			Criteria criteria = session.createCriteria(FunctionalIdentifier.class);
			criteria.add(Restrictions.in("funcId", funcIds));
			result = criteria.list();
			
			tx.commit();
		} catch (RuntimeException e) {
			if (tx != null)
				tx.rollback();
			throw e; // or display error message
		} finally {

			// end the session
			session.close();
		}

		return result;
	}

	/**
	 * Get the list of each FunctionalIdentifier. See FunctionalIdentifier: stands for a pair (tsuid, functional ID).
	 *
	 * @return null if nothing is found, or error occurred.
	 */
	public List<FunctionalIdentifier> listAll() {

		List<FunctionalIdentifier> result = null;

		Session session = getSession();
		Transaction tx = null;
		try {
			tx = session.beginTransaction();

			result = (List<FunctionalIdentifier>) session.createCriteria(FunctionalIdentifier.class).list();
			
			tx.commit();
		} catch (RuntimeException e) {
			if (tx != null)
				tx.rollback();
			throw e; // or display error message
		} finally {
			 

			// end the session
			session.close();
		}

		return result;
	}

	/**
	 * Get the FunctionalIdentifier entity from database with the funcId value
	 *
	 * @param funcId
	 *            the functional identifier to search for.
	 *
	 * @return null if nothing found in database, FunctionalIdentifier else.
	 */
	public FunctionalIdentifier getFromFuncId(String funcId)
			throws IkatsDaoMissingRessource, IkatsDaoConflictException, IkatsDaoException {

		String propertyName = "funcId";

		FunctionalIdentifier result = getByProperty(propertyName, funcId);
		return result;
	}

	/**
	 * Get the FunctionalIdentifier entity from database with the tsuid value
	 *
	 * @param tsuid
	 *
	 * @return
	 *
	 * @throws IkatsDaoMissingRessource
	 * @throws IkatsDaoConflictException
	 * @throws IkatsDaoException
	 */
	public FunctionalIdentifier getFromTsuid(String tsuid)
			throws IkatsDaoMissingRessource, IkatsDaoConflictException, IkatsDaoException {

		String propertyName = "tsuid";

		FunctionalIdentifier result = getByProperty(propertyName, tsuid);
		return result;
	}

	private FunctionalIdentifier getByProperty(String propertyName, String propertyValue) throws IkatsDaoException {

		FunctionalIdentifier result = null;

		Session session = getSession();
		Transaction tx = null;
		try {
			tx = session.beginTransaction();

			Criteria criteria = session.createCriteria(FunctionalIdentifier.class);

			criteria.add(Restrictions.eq(propertyName, propertyValue));
			List<?> results = criteria.list();
			if ((results == null) || (results.size() == 0)) {
				throw new IkatsDaoMissingRessource(
						"Missing FunctionalIdentifier matching " + propertyName + "=" + propertyValue);
			} else if ((results != null) && (results.size() > 1)) {
				throw new IkatsDaoConflictException("Unexpected conflict error: several FunctionalIdentifier matching "
						+ propertyName + "=" + propertyValue);
			}
			result = (FunctionalIdentifier) results.get(0);
			
			tx.commit();
			
		} catch (RuntimeException e) {
			if (tx != null)
				tx.rollback();
			throw e; // or display error message
		} finally {
			
			// end the session
			session.close();
		}

		return result;
	}
}

