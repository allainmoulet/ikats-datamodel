package fr.cs.ikats.metadata.dao;

import fr.cs.ikats.common.dao.DataBaseDAO;
import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoInvalidValueException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import fr.cs.ikats.common.expr.Atom;
import fr.cs.ikats.common.expr.Expression;
import fr.cs.ikats.common.expr.Expression.ConnectorExpression;
import fr.cs.ikats.common.expr.Group;
import fr.cs.ikats.common.expr.SingleValueComparator;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;
import fr.cs.ikats.metadata.model.MetaData;
import fr.cs.ikats.metadata.model.MetaData.MetaType;
import fr.cs.ikats.metadata.model.MetadataCriterion;
import org.apache.log4j.Logger;
import org.hibernate.*;
import org.hibernate.criterion.*;
import org.hibernate.exception.ConstraintViolationException;

import java.util.*;

/**
 * DAO class for MetaData model class
 */
public class MetaDataDAO extends DataBaseDAO {

    private static final Logger LOGGER = Logger.getLogger(MetaDataDAO.class);

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
        Session session = getSession();
        Transaction tx = null;
        Integer mdId = null;
        String mdInfo = "null";
        try {
            mdInfo = md.toString();
            tx = session.beginTransaction();
            mdId = (Integer) session.save(md);
            tx.commit();
            LOGGER.debug("Created " + mdInfo + " with value=" + md.getValue());
        } catch (ConstraintViolationException e) {

            String msg = "Creating: " + mdInfo + ": already exists in base for same (TSUID, name)";
            LOGGER.warn(msg);

            rollbackAndThrowException(tx, new IkatsDaoConflictException(msg, e));
        } catch (HibernateException e) {
            String msg = "Creating: " + mdInfo + ": unexpected HibernateException";
            LOGGER.error(msg, e);

            rollbackAndThrowException(tx, new IkatsDaoException(msg, e));
        } catch (Exception anotherError) {
            // deals with null pointer exceptions ...
            String msg = "Creating MetaData: " + mdInfo + ": unexpected Exception";
            LOGGER.error(msg, anotherError);

            rollbackAndThrowException(tx, new IkatsDaoException(msg, anotherError));
        } finally {
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
        Session session = getSession();
        Transaction tx = null;
        Integer mdId = null;
        String mdInfo = "null";
        MetaData currentRow = null;
        List<Integer> lProcessedIds = new ArrayList<Integer>();
        Long date = (new Date()).getTime();
        try {
            tx = session.beginTransaction();
            LOGGER.info("MetaDataDAO::persist(Lis<MetaData>): started transaction [" + date + "] ...");
            for (Iterator<MetaData> iterator = mdList.iterator(); iterator.hasNext(); ) {
                currentRow = iterator.next();
                if (update) {
                    // check if metadata already exists
                    String queryStr = "select id from tsmetadata where name = '" + currentRow.getName() + "' and tsuid ='" + currentRow.getTsuid()
                            + "'";
                    List<Object> result = session.createSQLQuery(queryStr).list();
                    if (result.size() == 0) {
                        // creation of metadata
                        mdId = (Integer) session.save(currentRow);
                        LOGGER.debug("- ... created metadata id=" + mdId);
                    } else {
                        // update of metadata
                        mdId = (Integer) result.get(0);
                        MetaData meta = (MetaData) session.get(MetaData.class, mdId);
                        meta.setDType(currentRow.getDType());
                        meta.setValue(currentRow.getValue());
                        session.update(meta);
                        LOGGER.debug("- ... updated metadata id=" + mdId);
                    }
                } else {
                    // creation of metadata
                    mdId = (Integer) session.save(currentRow);
                    LOGGER.debug("- ... created metadata id=" + mdId);
                }
                lProcessedIds.add(mdId);
            }
            LOGGER.info("MetaDataDAO::persist(List<MetaData>) has successfully imported " + mdList.size() + " metadata rows");
            tx.commit();
            LOGGER.info("MetaDataDAO::persist(List<MetaData>): committed transaction [" + date + "]");
        } catch (ConstraintViolationException e) {
            mdInfo = (currentRow != null) ? currentRow.toString() : "null";
            String msg = "Importing: " + mdInfo + ": ConstraintViolationException occurred: see the full error stack in the logs for further details";
            LOGGER.error(msg);

            rollbackAndThrowException(tx, new IkatsDaoConflictException(msg, e));
        } catch (HibernateException e) {
            mdInfo = (currentRow != null) ? currentRow.toString() : "null";
            String msg = "Importing: " + mdInfo + ": unexpected HibernateException";
            LOGGER.error(msg, e);

            rollbackAndThrowException(tx, new IkatsDaoException(msg, e));
        } catch (Exception anotherError) {
            mdInfo = (currentRow != null) ? currentRow.toString() : "null";
            // deals with null pointer exceptions ...
            String msg = "Importing MetaData: " + mdInfo + ": unexpected Exception";
            LOGGER.error(msg, anotherError);

            rollbackAndThrowException(tx, new IkatsDaoException(msg, anotherError));
        } finally {
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
        Session session = getSession();
        Transaction tx = null;
        String mdInfo = "null";
        boolean updated = false;
        try {
            mdInfo = md.toString();
            tx = session.beginTransaction();
            session.update(md);
            tx.commit();
            updated = true;
            LOGGER.debug("Updated:" + mdInfo + " with value=" + md.getValue());
        } catch (ConstraintViolationException e) {

            String msg = "Updating: " + mdInfo + ": already exists in base for same (TSUID, name)";
            LOGGER.warn(msg);

            rollbackAndThrowException(tx, new IkatsDaoConflictException(msg, e));
        } catch (HibernateException e) {
            String msg = "Updating: " + mdInfo + ": unexpected HibernateException";
            LOGGER.error(msg, e);

            rollbackAndThrowException(tx, new IkatsDaoException(msg, e));
        } catch (Exception anotherError) {
            // deals with null pointer exceptions ...
            String msg = "Updating MetaData: " + mdInfo + ": unexpected Exception";
            LOGGER.error(msg, anotherError);

            rollbackAndThrowException(tx, new IkatsDaoException(msg, anotherError));
        } finally {
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
        Session session = getSession();
        Transaction tx = null;
        int result = 0;
        try {
            tx = session.beginTransaction();
            String hql = "delete from MetaData where tsuid= :uid";
            Query query = session.createQuery(hql);
            query.setString("uid", tsuid);
            result = query.executeUpdate();
            tx.commit();
        } catch (HibernateException e) {
            IkatsDaoException ierror = new IkatsDaoException("Deleting MetaData rows matching tsuid=" + tsuid, e);
            LOGGER.error(ierror);
            rollbackAndThrowException(tx, ierror);
        } finally {
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
        Session session = getSession();
        Transaction tx = null;
        int result = 0;
        try {
            tx = session.beginTransaction();
            String hql = "delete from MetaData where tsuid= :uid and name= :name";
            Query query = session.createQuery(hql);
            query.setString("uid", tsuid);
            query.setString("name", name);
            result = query.executeUpdate();
            tx.commit();
        } catch (HibernateException e) {

            IkatsDaoException ierror = new IkatsDaoException("Deleting MetaData rows matching tsuid=" + tsuid + " name=" + name, e);
            LOGGER.error(ierror);
            rollbackAndThrowException(tx, ierror);
        } finally {
            session.close();
        }
        return result;
    }

    /**
     * get list of metadata for one tsuid.
     *
     * @param tsuid the ts identifier
     * @return the list if any result found.
     * @throws IkatsDaoMissingRessource error raised when no matching resource is found, for a tsuid
     *                                  different from '*'
     * @throws IkatsDaoException        other errors
     */
    @SuppressWarnings("unchecked")
    public List<MetaData> listForTS(String tsuid) throws IkatsDaoMissingRessource, IkatsDaoException {
        List<MetaData> result = null;
        Session session = getSession();
        try {

            if (tsuid.equals("*")) {
                Criteria criteria = session.createCriteria(MetaData.class);
                result = criteria.list();
            } else {
                // Query q =
                // session.createQuery("from MetaData where tsuid = :tsuid");
                Query q = session.createQuery(MetaData.LIST_ALL_FOR_TSUID);
                q.setString("tsuid", tsuid);
                result = q.list();
                if (result == null || (result.size() == 0)) {
                    String msg = "Searching MetaData from tsuid=" + tsuid + ": no resource found, but should exist.";
                    LOGGER.error(msg);

                    throw new IkatsDaoMissingRessource(msg);
                }
            }
        } catch (HibernateException hibException) {
            String msg = "Searching MetaData from tsuid=" + tsuid + ": unexpected HibernateException.";
            LOGGER.error(msg);
            throw new IkatsDaoException(msg);
        } catch (Exception error) {
            if (error instanceof IkatsDaoMissingRessource) {
                throw error;
            } else {
                String msg = "Searching MetaData from tsuid=" + tsuid + ": unexpected Exception.";
                LOGGER.error(msg);
                throw new IkatsDaoException(msg);
            }
        } finally {
            session.close();
        }

        return result;
    }

    /**
     * get list of metadata/type couples.
     *
     * @return the list if any result found.
     * @throws IkatsDaoMissingRessource error raised when no matching resource is found, for a tsuid
     *                                  different from '*'
     * @throws IkatsDaoException        other errors
     */
    @SuppressWarnings("unchecked")
    public Map<String, String> listTypes() throws IkatsDaoMissingRessource, IkatsDaoException {
        Map<String, String> result = new HashMap<>();
        Session session = getSession();
        try {
            Query q = session.createSQLQuery("select distinct LOWER(name) as name,dtype from tsmetadata order by name;");
            List q_result = q.list();
            for (Object result_elem : q_result) {
                Object[] obj_result = (Object[]) result_elem;
                result.put((String) obj_result[0], (String) obj_result[1]);
            }
        } catch (HibernateException hibException) {
            String msg = "Searching MetaData types : unexpected HibernateException.";
            LOGGER.error(msg);
            throw new IkatsDaoException(msg);
        } catch (Exception error) {
            if (error instanceof IkatsDaoMissingRessource) {
                throw error;
            } else {
                String msg = "Searching MetaData types: unexpected Exception.";
                LOGGER.error(msg);
                throw new IkatsDaoException(msg);
            }
        } finally {
            session.close();
        }

        return result;
    }

    /**
     * get the metadata entry for a TSUID and a Metadata name.
     *
     * @param tsuid the ts identifier
     * @param name  the name of the metadata
     * @return the list if any result found.
     * @throws IkatsDaoMissingRessource  error raised when no metadata is matching the tsuid+name
     *                                   criteria
     * @throws IkatsDaoConflictException error raised when multiple metadata are found
     * @throws IkatsDaoException         any other exceptions
     */
    public MetaData getMD(String tsuid, String name) throws IkatsDaoMissingRessource, IkatsDaoConflictException, IkatsDaoException {

        MetaData result = null;
        Session session = getSession();
        String pairCriteria = "null";
        try {
            pairCriteria = "(tsuid=" + tsuid + ", name=" + name + " )";
            Query q = session.createQuery(MetaData.GET_MD);
            q.setString("tsuid", tsuid);
            q.setString("name", name);
            result = (MetaData) q.uniqueResult();
            if (result == null) {
                throw new IkatsDaoMissingRessource("Reading MetaData: missing ressource for " + pairCriteria);
            }
        } catch (NonUniqueResultException multipleResults) {
            throw new IkatsDaoConflictException("Multiple MetaData are matching the criteria: " + pairCriteria);
        } catch (HibernateException hibError) {
            throw new IkatsDaoException("Reading MetaData: unexpected HibernateError with: " + pairCriteria, hibError);
        } catch (IkatsDaoException daoError) {
            throw daoError; // already handled
        } catch (Exception other) {
            throw new IkatsDaoException("Reading MetaData: unexpected exception with: " + pairCriteria, other);
        } finally {
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
        if (ConnectorExpression.AND != formula.connector) {
            throw new IkatsDaoInvalidValueException(
                    "searchFuncId expects ConnectorExpression.AND: not yet implemented connector " + formula.connector);
        }

        // step 1:
        // Group <MetadataCriterion> by metadata having same name
        Map<String, List<MetadataCriterion>> groupByMetadataName = initMapGroupingSameMetadataname(formula);

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
                String metaName = mapEntry.getKey();
                List<MetadataCriterion> criterions = mapEntry.getValue();

                tsuidsInScope = searchFuncIdForMetadataNamed(tsuidsInScope, metaName, criterions);
            }
        }

        // step4: encode the result
        //
        List<FunctionalIdentifier> included = new ArrayList<FunctionalIdentifier>();
        for (String includedTsuid : tsuidsInScope) {
            included.add(mapFunctionalIdentifier.get(includedTsuid));
        }
        // TBC: excluded collection may be integrated in returned JSON ?
        // List<FunctionalIdentifier> excluded = new
        // ArrayList<FunctionalIdentifier>( scope );
        // excluded.removeAll( included );
        return included;
    }

    /**
     * Reduce a tsuid list (scope) according to criteria : expecting list of
     * criterion for one only metadata name
     *
     * @param tsuidsInScope
     * @param metaName
     * @param criterions
     * @return
     */
    private List<String> searchFuncIdForMetadataNamed(List<String> tsuidsInScope, String metaName, List<MetadataCriterion> criterions)
            throws IkatsDaoException {

        if ((tsuidsInScope == null) || (tsuidsInScope.isEmpty())) {
            return tsuidsInScope;
        }

        List<String> result = new ArrayList<String>();
        ConnectorExpression lOperator = ConnectorExpression.AND;

        Session session = null;

        try {
            session = getSession();

            Criteria criteria = session.createCriteria(MetaData.class);
            Junction lGroupJunction;

            if (lOperator == ConnectorExpression.AND) {
                lGroupJunction = Restrictions.conjunction();
                lGroupJunction.add(Restrictions.in("tsuid", tsuidsInScope));
                lGroupJunction.add(Restrictions.eq("name", metaName));
                criteria.add(lGroupJunction);

                // expecting criteria grouped by metadata name => one type of
                // criterion per request ...
                for (MetadataCriterion metaCriterion : criterions) {

                    String criterionPropertyValue = metaCriterion.getValue();
                    SingleValueComparator criterionOperator = metaCriterion.getTypedComparator();
                    Conjunction restrictionToAdd = Restrictions.conjunction();

                    // date or number
                    String sqlOperator = criterionOperator.getText();
                    boolean isCurrentCriterionNumber = ((Objects.equals(sqlOperator, "=")) ||
                            (Objects.equals(sqlOperator, "!=")) ||
                            (Objects.equals(sqlOperator, ">")) ||
                            (Objects.equals(sqlOperator, "<")) ||
                            (Objects.equals(sqlOperator, ">=")) ||
                            (Objects.equals(sqlOperator, "<=")));

                    if (isCurrentCriterionNumber) {
                        Float numberValue = null;
                        try {
                            numberValue = Float.parseFloat(criterionPropertyValue);
                        } catch (NumberFormatException e) {
                            throw new IkatsDaoException();
                        }
                        restrictionToAdd.add(Restrictions.sqlRestriction("cast( {alias}.value as float ) " + sqlOperator + " " + criterionPropertyValue));
                    } else {

                        if (Objects.equals(sqlOperator, "in")) {
                            List<String> splitValues = Arrays.asList(criterionPropertyValue.split("\\s*;\\s*"));
                            restrictionToAdd.add(Restrictions.in("value", splitValues));
                        } else if (Objects.equals(sqlOperator, "not in")) {
                            List<String> splitValues = Arrays.asList(criterionPropertyValue.split("\\s*;\\s*"));
                            restrictionToAdd.add(Restrictions.not(Restrictions.in("value", splitValues)));
                        } else {
                            // Handle "like" and "not like" operators
                            restrictionToAdd.add(Restrictions.sqlRestriction("value " + sqlOperator + " '" + criterionPropertyValue + "'"));
                        }

                    }
                    lGroupJunction.add(restrictionToAdd);
                }
                // Get all the meta data
            } else {
                throw new IkatsDaoException("Not yet implemented: operator OR grouping  expressions of metadata criterion");
            }

            criteria.setProjection(Projections.projectionList().add(Projections.groupProperty("tsuid")));
            criteria.addOrder(Order.asc("tsuid"));
            List l = criteria.list();
            result = (List<String>) l;
        } catch (HibernateException hibException) {
            String msg = "Searching MetaData from criteria map: unexpected HibernateException.";
            LOGGER.error(msg);
            throw new IkatsDaoException(msg, hibException);
        } catch (IkatsDaoInvalidValueException invalidCriterionException) {
            String msg = "Not found ressource, searching functional identifiers matched by metadata criteria: near metadata named " + metaName;
            LOGGER.error("NOT FOUND: " + msg);

            throw new IkatsDaoMissingRessource(msg, invalidCriterionException);
        } catch (IkatsDaoException invalidNumberException) {
            String msg = "searchFuncIdForMetadataNamed :: Comparison operand is not a number ";
            LOGGER.error("NOT FOUND: " + msg);

            throw new IkatsDaoMissingRessource(msg, invalidNumberException);
        } catch (Throwable exception) {
            String msg = "Searching MetaData from criteria map: unexpected Throwable exception. ";
            LOGGER.error(msg);
            throw new IkatsDaoException(msg, exception);
        } finally {
            if (session != null) {
                session.close();
                LOGGER.info("searchFuncIdForMetadataNamed :: Session closed");
            }
        }

        if (result == null) {
            result = Collections.emptyList();
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
    private Map<String, List<MetadataCriterion>> initMapGroupingSameMetadataname(Group<MetadataCriterion> formula) throws IkatsDaoException {

        Map<String, List<MetadataCriterion>> map = new HashMap<String, List<MetadataCriterion>>();

        for (Expression<MetadataCriterion> expression : formula.terms) {
            if (expression instanceof Atom<?>) {
                Atom<MetadataCriterion> atomMetaCriterion = (Atom<MetadataCriterion>) expression;
                MetadataCriterion metaCriterion = atomMetaCriterion.atomicTerm;
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
        Session session = getSession();
        List<String> result = null;
        try {
            String hql = "select Func.funcId from MetaData Meta, FunctionalIdentifier Func  "
                    + "where Meta.name='metric' and Meta.value= :mid and Meta.tsuid=Func.tsuid";
            Query query = session.createQuery(hql);
            query.setString("mid", metric);
            result = query.list();
        } catch (HibernateException e) {
            IkatsDaoException ierror = new IkatsDaoException("Error while retrieving Functional identifiers of timeseries matching metric =" + metric,
                    e);
            LOGGER.error(ierror);
            throw ierror;
        } finally {
            session.close();
        }
        return result;
    }

}
