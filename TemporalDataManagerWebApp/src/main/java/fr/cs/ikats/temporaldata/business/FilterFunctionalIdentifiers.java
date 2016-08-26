/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 8 janv. 2016 : Creation 
 *
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.temporaldata.business;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import fr.cs.ikats.lang.StringUtils;

/**
 * @author ikats Instance defined by the JSON param defining filters criteria on
 *         FunctionalIdentifier
 */
public class FilterFunctionalIdentifiers {
    
    /**
     * list mapping json list named tsuids
     */
    private List<String> tsuids = null;

    /**
     * list mapping json list named funcIds
     */
    private List<String> funcIds = null;

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        String tsuidsInfo = "";
        if ((tsuids != null) && (tsuids.size() > 0)) {
            tsuidsInfo = StringUtils.join(",", tsuids);
        }
        String funcIdsInfo = "";
        if ((funcIds != null) && (funcIds.size() > 0)) {
            funcIdsInfo = StringUtils.join(",", funcIds);
        }
        return "FilterFunctionalIdentifiers with criteria: tsuids[" + tsuidsInfo + "] funcIDs[" + funcIdsInfo + "]";
    }

    /**
     * Build FilterFunctionalIdentifiers with form parameters
     * 
     * @param tsuids
     *            list of tsuids defined with ',' separator. null is accepted
     *            this criterion is disabled.
     * @param funcIds
     *            optional list of funcIds defined with ',' separator. null is
     *            accepted this criterion is disabled.
     * @return the filter.
     */
    static public FilterFunctionalIdentifiers buildWithFormParams(String tsuids, String funcIds) {
        FilterFunctionalIdentifiers filter = new FilterFunctionalIdentifiers();
        if (tsuids != null) {
            List<String> tsuidList = Arrays.asList(tsuids.split(","));
            filter.tsuids = tsuidList;
        }
        if (funcIds != null) {
            List<String> funcIdList = Arrays.asList(funcIds.split(","));
            filter.funcIds = funcIdList;
        }
        return filter;
    }

    /**
     * @param tsuids list of tsuids
     */
    public void setTsuids(List<String> tsuids) { 
        this.tsuids = tsuids;
    }

    /**
     * @param funcIds list of functional identidiers values
     */
    public void setFuncIds(List<String> funcIds) {
        this.funcIds = funcIds;
        
    }

    /**
     * Getter for tsuids
     * @return tsuids
     */
    public List<String> getTsuids() {
        return this.tsuids;
    }

    /**
     * Getter for funcIds
     * @return funcIds
     */
    public List<String> getFuncIds() {
        return this.funcIds;
    }
}
