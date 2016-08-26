/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 13 janv. 2016 : Creation 
 *
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.lang;

import java.util.Collection;

/**
 *
 */
public class CollectionUtils {

    /**
     * Tests if collection is null or sized zero
     * @param coll any collection
     * @return true if the collection is null or empty
     */
    public static boolean isNullOrEmpy(Collection<?> coll) {
        return (coll == null) || (coll.size() == 0);
    }
}
