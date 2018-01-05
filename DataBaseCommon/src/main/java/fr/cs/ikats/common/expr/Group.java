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
 * http://www.apache.org/licenses/LICENSE-2.0
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
 */

package fr.cs.ikats.common.expr;

import java.util.List;

/**
 * TODO a finaliser ... la methode eval reste hypothetique
 * le premier but des Expression: donnee definie de maniere reccursive
 * ... a voir: garder eval(), comment adapter
 */
public class Group<T> extends Expression<T> {
    private ConnectorExpression connector;

    public List<Expression<T>> getTerms() {
        return terms;
    }

    public void setTerms(List<Expression<T>> terms) {
        this.terms = terms;
    }

    private List<Expression<T>> terms;

    public ConnectorExpression getConnector() {
        return connector;
    }

    public void setConnector(ConnectorExpression connector) {
        this.connector = connector;
    }

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

