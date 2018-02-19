/**
 * LICENSE:
 * --------
 * Copyright 2017-2018 CS SYSTEMES D'INFORMATION
 *
 * Licensed to CS SYSTEMES D'INFORMATION under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. CS licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
 * @author Maxime PERELMUTER <maxime.perelmuter@c-s.fr>
 *
 */

package fr.cs.ikats.common.expr;

import java.util.List;

/**
 * le premier but des Expression: donnee definie de maniere reccursive
 */
public class Group extends Expression {
    private ConnectorExpression connector;

    public List<Expression> getTerms() {
        return terms;
    }

    public void setTerms(List<Expression> terms) {
        this.terms = terms;
    }

    private List<Expression> terms;

    public ConnectorExpression getConnector() {
        return connector;
    }

    public void setConnector(ConnectorExpression connector) {
        this.connector = connector;
    }

}
