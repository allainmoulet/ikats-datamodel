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

package fr.cs.ikats.datamanager.client.opentsdb.model;

import java.util.Map;

/**
 *
 */
public class METRICFilter extends AbstractFilter {

    String aggregator;
    String metric;
    boolean rate;
    Map<String, String> tags;

    /**
     * @param aggregator
     * @param metric
     * @param rate
     * @param tags
     */
    public METRICFilter(String aggregator, String metric, boolean rate, Map<String, String> tags) {
        super();
        this.aggregator = aggregator;
        this.metric = metric;
        this.rate = rate;
        this.tags = tags;
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
     * @return the metric
     */
    public String getMetric() {
        return metric;
    }

    /**
     * Setter
     * @param metric the metric to set
     */
    public void setMetric(String metric) {
        this.metric = metric;
    }

    /**
     * Getter
     * @return the rate
     */
    public boolean isRate() {
        return rate;
    }

    /**
     * Setter
     * @param rate the rate to set
     */
    public void setRate(boolean rate) {
        this.rate = rate;
    }

    /**
     * Getter
     * @return the tags
     */
    public Map<String, String> getTags() {
        return tags;
    }

    /**
     * Setter
     * @param tags the tags to set
     */
    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }


}
