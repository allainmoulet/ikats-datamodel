/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 12 janv. 2016 : Creation 
 *
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.lang;

/**
 * Class grouping useful methods working with Strings
 */
public class StringUtils {

    /**
     * Concatenates the CharSequences from stringList, with a defined delimiter
     * @param delimiter string inserted between two joined values
     * @param stringList list of values to be joined
     * @return join result.
     */
    public static String join(CharSequence delimiter, CharSequence ... stringList)
    {
        StringBuilder lBuff = new StringBuilder();
        boolean addDelim = false;
        for (CharSequence currentString : stringList) {
            if ( addDelim )
            {
                lBuff.append( delimiter );  
            }
            addDelim = true;
            lBuff.append( currentString );
        }
        return lBuff.toString();
    }
    
    /**
     * Concatenates the CharSequences from stringIter, with a defined delimiter
     * @param delimiter string inserted between two joined values
     * @param stringIter iterable of CharSequence values to be joined
     * @return join result.
     */
    public static String join(CharSequence delimiter,  Iterable<? extends CharSequence> stringIter)
    {
        StringBuilder lBuff = new StringBuilder();
        boolean addDelim = false;
        for (CharSequence currentString : stringIter) {
            if ( addDelim )
            {
                lBuff.append( delimiter );  
            }
            addDelim = true;
            lBuff.append( currentString );
        }
        return lBuff.toString();
    }
}
