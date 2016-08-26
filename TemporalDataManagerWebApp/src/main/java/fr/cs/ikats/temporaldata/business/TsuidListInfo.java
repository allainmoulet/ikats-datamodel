/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 30 mai 2016 : Creation 
 *
 * FIN-HISTORIQUE
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
