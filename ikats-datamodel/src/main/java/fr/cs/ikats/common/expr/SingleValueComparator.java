/**
 * Copyright 2018-2019 CS Systèmes d'Information
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

package fr.cs.ikats.common.expr;

import fr.cs.ikats.common.dao.exception.IkatsDaoInvalidValueException;

/**
 * Enum defining comparators dealing with the same type of value on both operand sides, left and right.
 */
public enum SingleValueComparator {
    EQUAL("="),
    NEQUAL("!="),
    LT("<"),
    GT(">"),
    LE("<="),
    GE(">="),
    LIKE("like"),
    NLIKE("not like"),
    IN("in"),
    NIN("not in"),
    IN_TABLE("in table");

    final private String text;

    SingleValueComparator(String aText) {
        text = aText;
    }

    public String getText() {
        return text;
    }

    /**
     * Usefully parses a string into matching SingleValueComparator value
     *
     * @param aText the text which is defining the  SingleValueComparator value
     * @return the parsed SingleValueComparator value
     * @throws IkatsDaoInvalidValueException: failed to retrieve any matching SingleValueComparator
     */
    public static SingleValueComparator parseComparator(String aText) throws IkatsDaoInvalidValueException {
        SingleValueComparator parsed = null;

        for (SingleValueComparator currentOper : SingleValueComparator.values()) {
            if (currentOper.getText().equals(aText)) {
                parsed = currentOper;
            }
        }
        if (parsed != null) {
            return parsed;
        } else {
            throw new IkatsDaoInvalidValueException("Unexpected text for Comparator: " + aText);
        }
    }
}
