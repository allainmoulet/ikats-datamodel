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

package fr.cs.ikats.datamanager.client.opentsdb.model;

import java.util.List;

/**
 *
 */
public class TSUIDFilter extends AbstractFilter {

    String aggregator;
    List<String> tsuids;

    /**
     * @param aggregator
     * @param tsuids
     */
    public TSUIDFilter(String aggregator, List<String> tsuids) {
        super();
        this.aggregator = aggregator;
        this.tsuids = tsuids;
    }

    public TSUIDFilter() {
        super();
    }

    /**
     * Getter
     * @return the aggregator
     */
    public String getAggregator() {
        return aggregator;
    }

    /**
     * Setter
     * @param aggregator the aggregator to set
     */
    public void setAggregator(String aggregator) {
        this.aggregator = aggregator;
    }

    /**
     * Getter
     * @return the tsuids
     */
    public List<String> getTsuids() {
        return tsuids;
    }

    /**
     * Setter
     * @param tsuids the tsuids to set
     */
    public void setTsuids(List<String> tsuids) {
        this.tsuids = tsuids;
    }

}
