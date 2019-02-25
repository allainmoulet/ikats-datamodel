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

package fr.cs.ikats.temporaldata.business;

import java.util.Arrays;
import java.util.List;

import fr.cs.ikats.lang.StringUtils;

/**
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
