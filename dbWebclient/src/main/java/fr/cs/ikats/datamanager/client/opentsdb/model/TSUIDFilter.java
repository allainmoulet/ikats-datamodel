/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 28 janv. 2016 : Creation 
 *
 * FIN-HISTORIQUE
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
