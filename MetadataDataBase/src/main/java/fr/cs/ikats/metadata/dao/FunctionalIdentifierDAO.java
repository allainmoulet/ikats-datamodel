
package fr.cs.ikats.metadata.dao;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;
import org.hibernate.exception.ConstraintViolationException;

import fr.cs.ikats.common.dao.DataBaseDAO;
import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import fr.cs.ikats.metadata.model.FunctionalIdentifier;

/**
 * FunctionalIdentifierDAO is providing CRUD services on the resource
 * FunctionalIdentifier. <br/>
 * Note: replaced org.hibernate.HibernateException by Throwable in catch blocs
 * of services.
 */
public class FunctionalIdentifierDAO extends DataBaseDAO {

    private static final Logger LOGGER = Logger.getLogger(FunctionalIdentifierDAO.class);

    /**
     * Maximum limit of the SQL 'IN' clause, that cause JDBC driver to hang.
     * TODO think to move this elsewhere...
     */
    private static final int MAX_SQL_IN_CLAUSE_LIMIT = 20000;

    /**
     * persist FunctionalIdentifier into database
     * 
     * @param fi
     *            FunctionalIdentifier
     * @return the internal id
     */
    public int persist(FunctionalIdentifier fi) {
        Session session = getSession();
        Transaction tx = null;
        int result = 0;
        try {
            tx = session.beginTransaction();
            session.save(fi);
            tx.commit();
            LOGGER.debug("FunctionalIdentifier stored " + fi.getTsuid() + ";" + fi.getFuncId());
            result++;
        }
        catch (Throwable eOther) {
            if (tx != null) {
                tx.rollback();
            }
            LOGGER.error("Not stored: " + ((fi != null) ? fi.toString() : "FunctionalIdentifier is null"));
            LOGGER.error(eOther.getClass().getSimpleName() + "Throwable in FunctionalIdentifierDAO::persist", eOther);
        }
        finally {
            session.close();
        }
        return result;
    }

    /**
     * remove FunctionalIdentifier from database.
     * 
     * @param tsuidList
     *            the list of identifiers
     * @return number of removals
     */
    public int remove(List<String> tsuidList) throws IkatsDaoConflictException, IkatsDaoException{
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
        }
        catch (ConstraintViolationException e) {

            String msg = "Removing FunctionalIdentifier for tsuid=" + currentTsuid + "failed : constraint violation";
            LOGGER.warn(msg);

            rollbackAndThrowException(tx, new IkatsDaoConflictException(msg, e));
        }
        catch (HibernateException e) {
            String msg = "Removing FunctionalIdentifier for tsuid=" + currentTsuid + "failed : does not exist";
            LOGGER.error(msg, e);

            rollbackAndThrowException(tx, new IkatsDaoException(msg, e));
        }
        catch (Exception anotherError) {
            String msg = "Removing FunctionalIdentifier for tsuid=" + currentTsuid + "failed : unexpected Exception";
            LOGGER.error(msg, anotherError);

            rollbackAndThrowException(tx, new IkatsDaoException(msg, anotherError));
        }
        finally {
            session.close();
        }
        return result;
    }

    /**
     * Get the list of each FunctionalIdentifier matching the tsuids list. See
     * FunctionalIdentifier: stands for a pair (tsuid, functional ID).
     * 
     * @param tsuids
     *            the criterion list.
     * @return null if nothing is found, or error occured.
     */
    @SuppressWarnings("unchecked")
    public List<FunctionalIdentifier> list(List<String> tsuids) {
        Session session = getSession();
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
        }
        else {
            // Limit not raised : add the entire list
            tsuidSublists.add(tsuids);
        }

        try {
            for (List<String> tsuidList : tsuidSublists) {
                // loop over each sublists (if more than one) and concatenate
                // the result
                Criteria criteria = session.createCriteria(FunctionalIdentifier.class);
                criteria.add(Restrictions.in("tsuid", tsuidList));
                result.addAll(criteria.list());
            }
        }
        catch (Throwable eOther) {
            LOGGER.error(eOther.getClass().getSimpleName() + " in FunctionalIdentifierDAO::list", eOther);
        }
        finally {
            session.close();
        }
        return result;

    }

    /**
     * Get the list of each FunctionalIdentifier matching the funcIds list. See
     * FunctionalIdentifier: stands for a pair (tsuid, functional ID).
     * 
     * @param funcIds
     *            the criterion list
     * @return null if nothing is found, or error occured.
     */
    public List<FunctionalIdentifier> listByFuncIds(List<String> funcIds) {
        Session session = getSession();
        Transaction tx = null;
        List<FunctionalIdentifier> result = null;
        try {
            tx = session.beginTransaction();
            Criteria criteria = session.createCriteria(FunctionalIdentifier.class);
            criteria.add(Restrictions.in("funcId", funcIds));
            result = criteria.list();
            tx.commit();
        }
        // catch (HibernateException e) {
        // if (tx != null) {
        // tx.rollback();
        // }
        // LOGGER.error("FunctionalIdentifierDAO::listByFuncIds", e);
        // }
        catch (Throwable eOther) {
            try {
                if (tx != null) {
                    tx.rollback();
                }
            }
            catch (Throwable e) {

                LOGGER.error(this.getClass().getSimpleName() + "::listByFuncIds : failed roll-back", e);
            }

            LOGGER.error(eOther.getClass().getSimpleName() + " in FunctionalIdentifierDAO::listByFuncIds", eOther);
        }
        finally {
            session.close();
        }
        return result;

    }

    /**
     * Get the list of each FunctionalIdentifier. See FunctionalIdentifier:
     * stands for a pair (tsuid, functional ID).
     * 
     * @return null if nothing is found, or error occurred.
     */
    public List<FunctionalIdentifier> listAll() {
        Session session = getSession();
        Transaction tx = null;
        List<FunctionalIdentifier> result = null;
        try {
            tx = session.beginTransaction();
            // Query q = session.createQuery("FROM FunctionalIdentifier");
            // result = (List<FunctionalIdentifier>)q.list();

            result = (List<FunctionalIdentifier>) session.createCriteria(FunctionalIdentifier.class).list();

            tx.commit();
        }
        // catch (HibernateException e) {
        // if (tx != null) {
        // tx.rollback();
        // }
        // LOGGER.error("HibernateException in
        // FunctionalIdentifierDAO::listAllWorkflows", e);
        //
        // }
        catch (Throwable eOther) {

            try {
                if (tx != null) {
                    tx.rollback();
                }
            }
            catch (Throwable e) {

                LOGGER.error(this.getClass().getSimpleName() + "::listall : failed roll-back", e);
            }
            LOGGER.error(eOther.getClass().getSimpleName() + " in FunctionalIdentifierDAO::listAllWorkflows", eOther);
        }
        finally {
            session.close();
        }
        return result;

    }

    /**
     * Get the FunctionalIdentifier entity from database with the funcId value
     * 
     * @param funcId
     *            the functional identifier to search for.
     * @return null if nothing found in database, FunctionalIdentifier else.
     */
    public FunctionalIdentifier getFromFuncId(String funcId) throws IkatsDaoMissingRessource, IkatsDaoConflictException, IkatsDaoException {

        String propertyName = "funcId";

        FunctionalIdentifier result = getByProperty(propertyName, funcId);
        return result;
    }

    /**
     * Get the FunctionalIdentifier entity from database with the tsuid value
     * @param funcId
     * @return
     * @throws IkatsDaoMissingRessource
     * @throws IkatsDaoConflictException
     * @throws IkatsDaoException
     */
    public FunctionalIdentifier getFromTsuid(String tsuid) throws IkatsDaoMissingRessource, IkatsDaoConflictException, IkatsDaoException {

        String propertyName = "tsuid";

        FunctionalIdentifier result = getByProperty(propertyName, tsuid);
        return result;
    }
    
    private FunctionalIdentifier getByProperty(String propertyName, String propertyValue ) throws IkatsDaoException {
        Session session = null;
        FunctionalIdentifier result = null;
        try {
            session = getSession();
            Criteria criteria = session.createCriteria(FunctionalIdentifier.class);

            criteria.add(Restrictions.eq(propertyName, propertyValue));
            List<FunctionalIdentifier> results = criteria.list();
            if ((results == null) || (results.size() == 0)) {
                throw new IkatsDaoMissingRessource("Missing FunctionalIdentifier matching " + propertyName + "=" + propertyValue);
            }
            else if ((results != null) && (results.size() > 1)) {
                throw new IkatsDaoConflictException(
                        "Unexpected conflict error: several FunctionalIdentifier matching " + propertyName + "=" + propertyValue);
            }
            result = (FunctionalIdentifier) results.get(0);
        }
        catch (IkatsDaoException de) {
            throw de;
        }
        catch (Throwable e) {
            IkatsDaoException le = new IkatsDaoException( e.getClass().getSimpleName() + " in FunctionalIdentifierDAO::getByProperty " + propertyName + "=" + propertyValue);
            throw le;
        }
        finally {
            if (session != null) {
                session.close();
            }
        }
        return result;
    }
}
