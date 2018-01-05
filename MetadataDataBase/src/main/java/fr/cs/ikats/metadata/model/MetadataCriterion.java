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

package fr.cs.ikats.metadata.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import fr.cs.ikats.common.dao.exception.IkatsDaoInvalidValueException;
import fr.cs.ikats.common.expr.SingleValueComparator;

/**
 * The Metadata criterion defines one condition based upon one value in the
 * metadata filter. This condition is based on single value: <br/>
 * &lt;left operand&gt; &lt;comparator&gt; &lt;right operand&gt; <br/>
 * Where:
 * <ul>
 * <li>&lt;left operand&gt; is the searched value</li>
 * <li>&lt;comparator&gt; is defined by this.comparator</li>
 * <li>&lt;right operand&gt; is this.value</li>
 * </ul>
 *
 *
 */
public class MetadataCriterion {

    /**
     * Name of the tested metadata item
     */
    private String metadataName;

    /**
     * Type of comparator, used in the evaluated condition
     */
    private String comparator;

    private String value;

    public MetadataCriterion() {

    }

    public MetadataCriterion(String critName, String critOperator, String rightOperandValue) {
        metadataName = critName;
        comparator = critOperator;
        value = rightOperandValue;
    }

    /**
     * Getter
     *
     * @return the metadataName
     */
    public String getMetadataName() {
        return metadataName;
    }

    /**
     * Setter
     *
     * @param metadataName
     *            the metadataName to set
     */
    public void setMetadataName(String metadataName) {
        this.metadataName = metadataName;
    }

    /**
     * Getter
     *
     * @return the comparator
     */
    public String getComparator() {
        return comparator;
    }

    /**
     * Setter
     *
     * @param comparator
     *            the comparator to set
     */
    public void setComparator(String comparator) {
        this.comparator = comparator;
    }

    /**
     * Getter on the raw value: string. See algo getTypedValue()
     *
     * @return the value
     */
    public String getValue() {
        return value;
    }

    /**
     * Setter
     *
     * @param value
     *            the value to set
     */
    public void setValue(String value) {
        this.value = value;
    }

    @JsonIgnore
    public SingleValueComparator getTypedComparator() throws IkatsDaoInvalidValueException {
        return SingleValueComparator.parseComparator(this.comparator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        StringBuffer lBuff = new StringBuffer("MetadataCriterion: ");
        lBuff.append("metadataName=").append((metadataName != null) ? metadataName : "null");
        lBuff.append("comparator=").append((comparator != null) ? comparator : "null");
        lBuff.append("value=").append((value != null) ? value : "null");
        return lBuff.toString();
    }

    /**
     * Replace '*' by '%' for operator like
     *
     */
    public void computeServerValue() throws IkatsDaoInvalidValueException {
        if ((getTypedComparator() == SingleValueComparator.LIKE) || (getTypedComparator() == SingleValueComparator.NLIKE)) {
            String lValue = getValue();
            lValue = lValue.replace('*', '%');
            setValue(lValue);
        }

    }
}

