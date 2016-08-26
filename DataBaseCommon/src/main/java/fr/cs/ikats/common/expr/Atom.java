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
 * TODO a finaliser ... la methode eval reste hypothetique*
 * le premier but des Expression: donnee definie de maniere reccursive
 * ... a voir: garder eval(), comment adapter
 */
public class Atom<T> extends Expression<T> {
    public T atomicTerm;
    
    /**
     * STUB
     * {@inheritDoc}
     */
    @Override
    public boolean eval() {
        // TODO Auto-generated method stub
        // evaluate atomicTerm
        return false;
    }
}
