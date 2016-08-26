/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 18 mai 2016 : Creation 
 *
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.common.expr;

/**
 *  * TODO a finaliser ... la methode eval reste hypothetique*
 * le premier but des Expression: donnee definie de maniere reccursive
 * ... a voir: garder eval(), comment adapter 
 */
public abstract class Expression<T> {
    
    public enum ConnectorExpression {
        AND, OR
    }
    
    public abstract boolean eval();
}
