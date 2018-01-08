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
 * @author Mathieu BERAUD <mathieu.beraud@c-s.fr>
 * @author Pierre BONHOURE <pierre.bonhoure@c-s.fr>
 */

package fr.cs.ikats.metadata.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.NonUniqueResultException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Conjunction;
import org.hibernate.criterion.Junction;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.exception.ConstraintViolationException;

import fr.cs.ikats.common.dao.DataBaseDAO;
import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoInvalidValueException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingResource;
import fr.cs.ikats.common.expr.Atom;
import fr.cs.ikats.common.expr.Expression;
import fr.cs.ikats.common.expr.Expression.ConnectorExpression;
import fr.cs.ikats.common.expr.Group;
import fr.cs.ikats.common.expr.SingleValueComparator;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.metadata.model.MetaData.MetaType;
import fr.cs.ikats.metadata.model.MetadataCriterion;

/**
 * DAO class for MetaData model class
 */
public class MetaDataDAO extends DataBaseDAO {

    private static final Logger logger = Logger.getLogger(MetaDataDAO.class);

    /**
     * dtypes whose value is directly compared with value of MetadataCriterion
     * (no conversion needed)
     */
    private final static List<MetaType> EQ_COMPARABLE_DTYPES;

    /**
     * eligible dtypes for filters LT, LE, GT, GE, NEQUAL (requiring conversion
     * to number)
     */
    private final static List<MetaType> NUMBER_EVALUATED_DTYPES;

    /**
     * eligible dtypes for filters based upon string patterns (no conversion
     * needed)
     */
    private final static List<MetaType> PATTERN_EVALUATED_DTYPES;

    static {
        EQ_COMPARABLE_DTYPES = new ArrayList<MetaType>();
        EQ_COMPARABLE_DTYPES.add(MetaType.string);
        EQ_COMPARABLE_DTYPES.add(MetaType.date);
        // EQ_COMPARABLE_DTYPES does not contains:
        // number dtype is treated with specific conversion ...
        // complex dtype is ignored ...

        NUMBER_EVALUATED_DTYPES = new ArrayList<MetaType>();
        NUMBER_EVALUATED_DTYPES.add(MetaType.number);
        NUMBER_EVALUATED_DTYPES.add(MetaType.date);

        PATTERN_EVALUATED_DTYPES = new ArrayList<MetaType>();
        PATTERN_EVALUATED_DTYPES.add(MetaType.string);
        PATTERN_EVALUATED_DTYPES.add(MetaType.date);
        PATTERN_EVALUATED_DTYPES.add(MetaType.complex);
    }

    /**
     * persist metadata into database
     *
     * @param md the metadata
     * @return the internal Id for this metadata
     * @throws IkatsDaoConflictException conflict error with existing metadata
     * @throws IkatsDaoException         any other error
     */
    public Integer persist(MetaData md) throws IkatsDaoConflictException, IkatsDaoException {
        Integer mdId = null;
        String mdInfo = "null";

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            mdInfo = md.toString();
            tx = session.beginTransaction();
            mdId = (Integer) session.save(md);
            logger.debug("Created " + mdInfo + " with value=" + md.getValue());

            tx.commit();
        } catch (ConstraintViolationException e) {

            String msg = "Creating: " + mdInfo + ": already exists in base for same (TSUID, name)";
            logger.warn(msg);

            rollbackAndThrowException(tx, new IkatsDaoConflictException(msg, e));
        } catch (RuntimeException e) {
            // try to rollback
            if (tx != null && !tx.wasRolledBack()) tx.rollback();
            // Re-raise the original exception
            throw e;
        } finally {
            // end the session
            session.close();
        }

        return mdId;
    }

    /**
     * persist a list of metadata into database metadata are created or
     * created/updated, according to optional boolean 'update' a list of
     * database identifiers is returned
     *
     * @param mdList list of metadata to persist in database
     * @param update if true, already existing metadata is updated otherwise no
     *               metadata is persisted if one of them already exists
     * @return a list of imported metadata identifiers in db
     * @throws IkatsDaoConflictException
     * @throws IkatsDaoException
     */
    public List<Integer> persist(List<MetaData> mdList, Boolean update) throws IkatsDaoConflictException, IkatsDaoException {
        Integer mdId = null;
        String mdInfo = "null";
        MetaData currentRow = null;
        List<Integer> lProcessedIds = new ArrayList<Integer>();
        Long date = (new Date()).getTime();

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            logger.debug("MetaDataDAO::persist(Lis<MetaData>): started transaction [" + date + "] ...");
            for (Iterator<MetaData> iterator = mdList.iterator(); iterator.hasNext(); ) {
                currentRow = iterator.next();
                if (update) {
                    // check if metadata already exists
                    String queryStr = "select id from tsmetadata where name = '" + currentRow.getName() + "' and tsuid ='" + currentRow.getTsuid()
                            + "'";
                    List<?> result = session.createSQLQuery(queryStr).list();
                    if (result.size() == 0) {
                        // creation of metadata
                        mdId = (Integer) session.save(currentRow);
                        logger.trace("- ... created metadata id=" + mdId);
                    } else {
                        // update of metadata
                        mdId = (Integer) result.get(0);
                        MetaData meta = (MetaData) session.get(MetaData.class, mdId);
                        meta.setDType(currentRow.getDType());
                        meta.setValue(currentRow.getValue());
                        session.update(meta);
                        logger.trace("- ... updated metadata id=" + mdId);
                    }
                } else {
                    // creation of metadata
                    mdId = (Integer) session.save(currentRow);
                    logger.trace("- ... created metadata id=" + mdId);
                }
                lProcessedIds.add(mdId);
            }
            logger.debug("MetaDataDAO::persist(List<MetaData>) has successfully imported " + mdList.size() + " metadata rows");
            tx.commit();
            logger.trace("MetaDataDAO::persist(List<MetaData>): committed transaction [" + date + "]");
        } catch (ConstraintViolationException e) {
            mdInfo = (currentRow != null) ? currentRow.toString() : "null";
            String msg = "Importing: " + mdInfo + ": ConstraintViolationException occurred: see the full error stack in the logs for further details";
            logger.error(msg);

            rollbackAndThrowException(tx, new IkatsDaoConflictException(msg, e));
        } catch (RuntimeException e) {
            // try to rollback
            if (tx != null) tx.rollback();
            // Re-raise the original exception
            throw e;
        } finally {
            // end the session
            session.close();
        }

        return lProcessedIds;
    }

    /**
     * update an existing metadata into database
     *
     * @param md the metadata
     * @return the internal Id for this metadata
     * @throws IkatsDaoConflictException in case of conflict with existing ( tsuid, name ) pair
     * @throws IkatsDaoException         if the meta doesn't exists or if database can't be accessed
     * @since [#142998] manage IkatsDaoConflictException, IkatsDaoException and
     * method rollbackAndThrowException
     */
    public boolean update(MetaData md) throws IkatsDaoConflictException, IkatsDaoException {

        String mdInfo = null;
        boolean updated = false;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            mdInfo = md.toString();
            tx = session.beginTransaction();
            session.update(md);
            updated = true;
            logger.debug("Updated:" + mdInfo + " with value=" + md.getValue());

            tx.commit();
        } catch (ConstraintViolationException e) {

            String msg = "Updating: " + mdInfo + ": already exists in base for same (TSUID, name)";
            logger.warn(msg);

            rollbackAndThrowException(tx, new IkatsDaoConflictException(msg, e));
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
     * Remove the set of MetaData matching the defined tsuid from database
     *
     * @param tsuid identifier of tsuids
     * @return number of removals
     * @throws IkatsDaoException
     */
    public int remove(String tsuid) throws IkatsDaoException {
        int result = 0;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            String hql = "delete from MetaData where tsuid= :uid";
            Query query = session.createQuery(hql);
            query.setString("uid", tsuid);
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
     * remove Metadata from database.
     *
     * @param tsuid identifier of tsuids
     * @param name  name ot the metadata
     * @return number of removals
     * @throws IkatsDaoException error deleting the resource
     */
    public int remove(String tsuid, String name) throws IkatsDaoException {
        int result = 0;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            String hql = "delete from MetaData where tsuid= :uid and name= :name";
            Query query = session.createQuery(hql);
            query.setString("uid", tsuid);
            query.setString("name", name);
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
     * get list of metadata for one tsuid.
     *
     * @param tsuid the ts identifier
     * @return the list if any result found.
     * @throws IkatsDaoMissingResource error raised when no matching resource is found, for a tsuid
     *                                 different from '*'
     * @throws IkatsDaoException       other errors
     */
    @SuppressWarnings("unchecked")
    public List<MetaData> listForTS(String tsuid) throws IkatsDaoMissingResource, IkatsDaoException {
        List<MetaData> result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            if (tsuid.equals("*")) {
                Criteria criteria = session.createCriteria(MetaData.class);
                result = criteria.list();
                tx.commit();
            } else {
                Query q = session.createQuery(MetaData.LIST_ALL_FOR_TSUID);
                q.setString("tsuid", tsuid);
                result = q.list();
                tx.commit();
                if (result == null || (result.isEmpty())) {
                    String msg = "Searching MetaData from tsuid=" + tsuid + ": no resource found, but should exist.";

                    logger.error(msg);
                    throw new IkatsDaoMissingResource(msg);
                }
            }
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
     * get list of metadata/type couples.
     *
     * @return the list if any result found.
     * @throws IkatsDaoMissingResource error raised when no matching resource is found, for a tsuid
     *                                 different from '*'
     * @throws IkatsDaoException       other errors
     */
    public Map<String, String> listTypes() throws IkatsDaoMissingResource, IkatsDaoException {
        Map<String, String> result = new HashMap<>();

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Query q = session.createSQLQuery("select distinct LOWER(name) as name,dtype from tsmetadata order by name;");
            List<?> q_result = q.list();
            for (Object result_elem : q_result) {
                Object[] obj_result = (Object[]) result_elem;
                result.put((String) obj_result[0], (String) obj_result[1]);
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
     * get the metadata entry for a TSUID and a Metadata name.
     *
     * @param tsuid the ts identifier
     * @param name  the name of the metadata
     * @return the list if any result found, null otherwise.
     * @throws IkatsDaoConflictException error raised when multiple metadata are found
     */
    public MetaData getMD(String tsuid, String name) throws IkatsDaoConflictException, IkatsDaoException {

        MetaData result = null;
        String pairCriteria = "null";

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            pairCriteria = "(tsuid=" + tsuid + ", name=" + name + " )";
            Query q = session.createQuery(MetaData.GET_MD);
            q.setString("tsuid", tsuid);
            q.setString("name", name);
            result = (MetaData) q.uniqueResult();

            tx.commit();
        } catch (NonUniqueResultException multipleResults) {
            // Re-raise the original exception
            if (tx != null)
                tx.rollback();
            throw new IkatsDaoConflictException("Multiple MetaData are matching the criteria: " + pairCriteria, multipleResults);
        } catch (RuntimeException e) {
            if (tx != null)
                tx.rollback();
            throw e;
        } finally {
            // end the session
            session.close();
        }

        return result;
    }

    /**
     * Filter: criterion connected by AND operator: solving the front-end
     * request: {MetadataCriterion 1} and {MetadataCriterion 2} and ... and
     * {MetadataCriterion M} The <MetadataCriterion> are grouped by name of
     * metadata before being processed
     *
     * @param scope:   subset where is applied the filter
     * @param formula: the logical expression with metadata criteria defining the
     *                 filter
     * @return the result is the subset accepted by the filter
     * @throws IkatsDaoException
     */
    public List<FunctionalIdentifier> searchFuncId(List<FunctionalIdentifier> scope, Group<MetadataCriterion> formula) throws IkatsDaoException {

        // only AND connector allowed
        if (ConnectorExpression.AND != formula.getConnector()) {
            throw new IkatsDaoInvalidValueException(
                    "searchFuncId expects ConnectorExpression.AND: not yet implemented connector " + formula.getConnector());
        }

        // step 1:
        // Group <MetadataCriterion> by metadata having same name
        Map<String, List<MetadataCriterion>> groupByMetadataName = initMapGroupingSameMetadataName(formula);

        // step2:
        // prepare initial scope: tsuid list
        List<String> tsuidsInScope = new ArrayList<String>();
        Map<String, FunctionalIdentifier> mapFunctionalIdentifier = new HashMap<String, FunctionalIdentifier>();
        for (FunctionalIdentifier tsId : scope) {
            String tsuid = tsId.getTsuid();
            tsuidsInScope.add(tsuid);
            mapFunctionalIdentifier.put(tsuid, tsId);
        }

        // step3:
        // compute the intersection of TSUIDS of all requests <i> on each
        // different metadata <name i>
        for (Map.Entry<String, List<MetadataCriterion>> mapEntry : groupByMetadataName.entrySet()) {

            // new tsuidsInScope is a subset of previous tsuidsInScope
            if (!tsuidsInScope.isEmpty()) {
                List<MetadataCriterion> criteria = mapEntry.getValue();
                tsuidsInScope = searchFuncIdForMetadataNamed(tsuidsInScope, criteria);
            }
        }

        // step4: encode the result
        //
        List<FunctionalIdentifier> included = new ArrayList<FunctionalIdentifier>();
        for (String includedTsuid : tsuidsInScope) {
            included.add(mapFunctionalIdentifier.get(includedTsuid));
        }
        return included;
    }

    /**
     * Reduce a tsuid list (scope) according to criteria : expecting list of
     * criterion for one only metadata name
     *
     * @param tsuidsInScope
     * @param criteria
     * @return
     */
    private List<String> searchFuncIdForMetadataNamed(List<String> tsuidsInScope,
                                                      List<MetadataCriterion> criteria)
            throws IkatsDaoException {

        if ((tsuidsInScope == null) || (tsuidsInScope.isEmpty())) {
            return tsuidsInScope;
        }

        List<String> result = new ArrayList<String>();
        ConnectorExpression lOperator = ConnectorExpression.AND;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Criteria sessionCriteria = session.createCriteria(MetaData.class);
            Junction lGroupJunction;

            if (lOperator == ConnectorExpression.AND) {
                lGroupJunction = Restrictions.conjunction();
                lGroupJunction.add(Restrictions.in("tsuid", tsuidsInScope));
                sessionCriteria.add(lGroupJunction);

                // Create the subquery from the filters
                Conjunction filtersCritQuery = prepareFiltersCritQuery(criteria);


                sessionCriteria.add(filtersCritQuery);
            } else {
                // try to rollback
                if (tx != null) tx.rollback();
                throw new IkatsDaoException("Not yet implemented: operator OR grouping  expressions of metadata criterion");
            }

            sessionCriteria.setProjection(Projections.projectionList().add(Projections.groupProperty("tsuid")));
            sessionCriteria.addOrder(Order.asc("tsuid"));

            result = (List<String>) sessionCriteria.list();

            tx.commit();
        } catch (IkatsDaoInvalidValueException invalidCriterionException) {
            // raised by prepareFiltersCritQuery ...
            if (tx != null) tx.rollback();
            String msg = "Resource Not found, searching functional identifiers matched by metadata criteria";
            logger.error("NOT FOUND: " + msg);
            throw new IkatsDaoMissingResource(msg, invalidCriterionException);
        } catch (RuntimeException e) {
            if (tx != null) tx.rollback();
            throw e;
        } finally {

            // end the session
            session.close();
        }

        if (result == null) {
            result = Collections.emptyList();
        }
        return result;
    }

    /**
     * Prepare the criteria depending on the list of metadata filters
     *
     * @param criteria list of criterion to use for request build
     * @return the SQL part (as criteria) containing the metadata filters
     * @throws IkatsDaoInvalidValueException if a filter operand is not a number
     * @throws IkatsDaoInvalidValueException if a comparator is unknown
     */
    private Conjunction prepareFiltersCritQuery(List<MetadataCriterion> criteria)
            throws IkatsDaoInvalidValueException {

        Conjunction restrictionToAdd = Restrictions.conjunction();

        // expecting criteria grouped by metadata name => one type of
        // criterion per request ...
        for (MetadataCriterion criterion : criteria) {

            String metadataName = criterion.getMetadataName();
            String criterionValue = criterion.getValue();
            SingleValueComparator criterionOperator = criterion.getTypedComparator();
            String sqlOperator = criterionOperator.getText();
            SingleValueComparator comparator = SingleValueComparator.parseComparator(sqlOperator);

            switch (comparator) {
                case EQUAL:
                case NEQUAL:
                case GT:
                case LT:
                case GE:
                case LE:
                    // Date or number
                    try {
                        // this trick allow to verify the criterionValue can be casted to float number
                        Float.parseFloat(criterionValue);

                        restrictionToAdd
                                .add(Restrictions.eq("name", metadataName))
                                .add(Restrictions.sqlRestriction(
                                        "cast( {alias}.value as float ) " + sqlOperator + " " + criterionValue));
                    } catch (NumberFormatException e) {
                        throw new IkatsDaoInvalidValueException("Operand is not a number; " + e.getMessage(), e);
                    }
                    break;
                case IN: {
                    // List of values separated by ";"
                    List<String> splitValues = Arrays.asList(criterionValue.split("\\s*;\\s*"));
                    restrictionToAdd
                            .add(Restrictions.eq("name", metadataName))
                            .add(Restrictions.in("value", splitValues));
                }
                break;
                case NIN: {
                    List<String> splitValues = Arrays.asList(criterionValue.split("\\s*;\\s*"));
                    restrictionToAdd
                            .add(Restrictions.eq("name", metadataName))
                            .add(Restrictions.not(Restrictions.in("value", splitValues)));
                }
                break;
                case LIKE:
                case NLIKE:
                    restrictionToAdd
                            .add(Restrictions.eq("name", metadataName))
                            .add(Restrictions.sqlRestriction("value " + sqlOperator + " '" + criterionValue + "'"));
                    break;
                default:
                    // Unreachable
                    throw new IkatsDaoInvalidValueException("Unknown comparator : " + comparator);
            }
        }

        return restrictionToAdd;
    }

    /**
     * Filter: criterion connected by AND operator: solving the front-end
     * request: {MetadataCriterion 1} and {MetadataCriterion 2} and ... and
     * {MetadataCriterion M} The <MetadataCriterion> are grouped by name of
     * metadata before being processed
     *
     * @param datasetName dataset scope where is applied the filter
     * @param criteria    list of criterion
     * @return the result list of tuple tsuid/funcid filtered
     * @throws IkatsDaoException
     */
    public List<FunctionalIdentifier> searchFuncId(String datasetName, List<MetadataCriterion> criteria)
            throws IkatsDaoException {

        List<FunctionalIdentifier> result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            Criteria critQuery = session.createCriteria(FunctionalIdentifier.class);

            critQuery
                    .createAlias("LinkDatasetTimeSeries", "tsds", Criteria.INNER_JOIN)
                    .add(Restrictions.eqProperty("tsds.tsuid", "FunctionalIdentifier.tsuid"))
                    .createAlias("MetaData", "md", Criteria.INNER_JOIN)
                    .add(Restrictions.eq("md.tsuid", "tsds.tsuid"))
                    .add(Restrictions.eq("tsds.dataset", datasetName));

            // Loop over the criteria to eval metadata name with value
            Conjunction filtersCritQuery = prepareFiltersCritQuery(criteria);
            critQuery.add(filtersCritQuery);

            result = (List<FunctionalIdentifier>) critQuery.list();

            tx.commit();
        } catch (RuntimeException e) {
            if (tx != null) tx.rollback();
            throw e;
        } finally {

            // end the session
            session.close();
        }

        return result;
    }


    /**
     * Create the map grouping the criteria with same metadata name
     *
     * @param formula the formula is a simple group of atomic criteria connected
     *                with AND operator
     * @return
     */
    private Map<String, List<MetadataCriterion>> initMapGroupingSameMetadataName(Group<MetadataCriterion> formula)
            throws IkatsDaoException {

        Map<String, List<MetadataCriterion>> map = new HashMap<String, List<MetadataCriterion>>();

        for (Expression<MetadataCriterion> expression : formula.getTerms()) {
            if (expression instanceof Atom<?>) {
                Atom<MetadataCriterion> atomMetaCriterion = (Atom<MetadataCriterion>) expression;
                MetadataCriterion metaCriterion = atomMetaCriterion.getAtomicTerm();
                String criterionPropertyName = metaCriterion.getMetadataName();
                List<MetadataCriterion> mapValue = map.get(criterionPropertyName);
                if (mapValue == null) {
                    mapValue = new ArrayList<MetadataCriterion>();
                    map.put(criterionPropertyName, mapValue);
                }

                mapValue.add(metaCriterion);
            } else {
                throw new IkatsDaoException("Not yet implemented: recursive expressions of metadata criterion");
            }
        }
        return map;
    }

    /**
     * Retrieve the functional ids list whose metadata metric has a given value
     *
     * @param metric value of the metadata metric
     * @return
     * @throws IkatsDaoException
     */
    @SuppressWarnings("unchecked")
    public List<String> getListFuncIdFromMetric(String metric) throws IkatsDaoException {
        List<String> result = null;

        Session session = getSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            String hql = "select Func.funcId from MetaData Meta, FunctionalIdentifier Func  "
                    + "where Meta.name='metric' and Meta.value= :mid and Meta.tsuid=Func.tsuid";
            Query query = session.createQuery(hql);
            query.setString("mid", metric);
            result = query.list();

            tx.commit();
        } catch (RuntimeException e) {
            if (tx != null) tx.rollback();
            throw e;
        } finally {
            // end the session
            session.close();
        }

        return result;
    }

}

