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

import java.util.List;

/**
 * TODO a finaliser ... la methode eval reste hypothetique
 * le premier but des Expression: donnee definie de maniere reccursive
 * ... a voir: garder eval(), comment adapter
 */
public class Group<T> extends Expression<T>{
    public ConnectorExpression connector;
    public List<Expression<T>> terms;
    
    /**
     * STUB
     * {@inheritDoc}
     */
    @Override
    public boolean eval() {
        // TODO Auto-generated method stub
        // while expression is true : evaluate :
        //       expression ::= expression <connector> <next term>
        // return expression
        return false;
    }
}
