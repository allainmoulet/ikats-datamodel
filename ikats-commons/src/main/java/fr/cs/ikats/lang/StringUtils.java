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
    public static String join(CharSequence delimiter, CharSequence... stringList) {
        StringBuilder lBuff = new StringBuilder();
        boolean addDelim = false;
        for (CharSequence currentString : stringList) {
            if (addDelim) {
                lBuff.append(delimiter);
            }
            addDelim = true;
            lBuff.append(currentString);
        }
        return lBuff.toString();
    }

    /**
     * Concatenates the CharSequences from stringIter, with a defined delimiter
     * @param delimiter string inserted between two joined values
     * @param stringIter iterable of CharSequence values to be joined
     * @return join result.
     */
    public static String join(CharSequence delimiter, Iterable<? extends CharSequence> stringIter) {
        StringBuilder lBuff = new StringBuilder();
        boolean addDelim = false;
        for (CharSequence currentString : stringIter) {
            if (addDelim) {
                lBuff.append(delimiter);
            }
            addDelim = true;
            lBuff.append(currentString);
        }
        return lBuff.toString();
    }
}
