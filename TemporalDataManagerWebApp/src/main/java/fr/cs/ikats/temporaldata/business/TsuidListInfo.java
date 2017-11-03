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
 * 
 */

package fr.cs.ikats.temporaldata.business;

import java.util.List;

/**
 * Class JSONifiable: wrapper of simple list of TSUIDs.
 * 
 * Jersey Framework is automatically converting JSON from/to TsuidListInfo.
 */
public class TsuidListInfo {

    private List<String> tsuids;

    /**
     * 
     */
    public TsuidListInfo() {
        super();
    }

    /**
     * @param tsuids
     */
    public TsuidListInfo(List<String> tsuids) {
        super();
        this.tsuids = tsuids;
    }

    /**
     * Getter
     * 
     * @return the tsuids
     */
    public List<String> getTsuids() {
        return tsuids;
    }

    /**
     * Setter
     * 
     * @param tsuids
     *            the tsuids to set
     */
    public void setTsuids(List<String> tsuids) {
        this.tsuids = tsuids;
    }

    /**
     * Beware to very big collections: all content is written !
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        Integer lSize = 0;
        StringBuffer lBuff = new StringBuffer("TsuidListInfo: ");
        if ( tsuids != null )
        {
            lSize = tsuids.size();
            lBuff.append( " with ");
            lBuff.append( lSize );
            lBuff.append( " tsuids=[");
            boolean isFirst = true;
            for (String val : tsuids) {
                if ( ! isFirst )
                {
                    lBuff.append( ", ");     
                }
                else {
                    isFirst = false;
                }
                lBuff.append( val );
            }
            lBuff.append("].");
        }
        else
        {
            lBuff.append( " tsuids=[].");
        }
       return lBuff.toString();
       
    }

}

