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
 * @author Mathieu BERAUD <mathieu.beraud@c-s.fr>
 * @author Maxime PERELMUTER <maxime.perelmuter@c-s.fr>
 */

package fr.cs.ikats.metadata.model;

import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

/**
 * model class for MetaData.
 *
 */
@Entity
@Table(name = "TSMetadata", uniqueConstraints = @UniqueConstraint(columnNames = {"tsuid", "name"}))
public class MetaData {

    /**
     * Enumerate of possible types for a metadata (even if database encoding is fixed)
     */
    public enum MetaType {
        /**
         * Type of a Metadata coding for a string
         */
        string,
        /**
         * Type of a Metadata value coding for a date
         */
        date,
        /**
         * Type for Metadata value coding for a number
         */
        number,
        /**
         * Type for Metadata value coding for a complex structure (json ? ...)
         */
        complex;
    }

    /**
     * HQL request for all tsuids
     */
    public final static String LIST_ALL_FOR_TSUID = "select md from MetaData md where md.tsuid = :tsuid";

    /**
     * HQL request for a meta data entry
     */
    public final static String GET_MD = "select md from MetaData md where md.tsuid = :tsuid and  md.name = :name";

    /**
     * default constructor
     */
    public MetaData() {

    }

    @Id
    @SequenceGenerator(name = "tsmetadata_id_seq", sequenceName = "tsmetadata_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "tsmetadata_id_seq")
    @Column(name = "id", updatable = false)
    private Integer id;

    @Column(name = "tsuid")
    private String tsuid;

    @Column(name = "name")
    private String name;

    @Column(name = "value")
    private String value;

    @Column(name = "dtype")
    @Enumerated(EnumType.STRING)
    private MetaType dtype;

    /**
     * @return the id
     */
    public Integer getId() {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(Integer id) {
        this.id = id;
    }

    /**
     * @return the tsuid
     */
    public String getTsuid() {
        return tsuid;
    }

    /**
     * @param tsuid
     *            the tsuid to set
     */
    public void setTsuid(String tsuid) {
        this.tsuid = tsuid;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * :tsuid
     *
     * @param name
     *            the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the value
     */
    public String getValue() {
        return value;
    }

    /**
     * @param value
     *            the value to set
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     *
     * {@inheritDoc}
     */
    public String toString() {
        StringBuilder buff = new StringBuilder("MetaData");
        String name = getName();
        String tsuid = getTsuid();
        MetaType dtype = getDType();
        String value = getValue();
        Integer id = getId();
        if (name != null) {
            buff.append("with name=[");
            buff.append(name);
            buff.append("] ");
        }
        if (tsuid != null) {
            buff.append("for tsuid=[");
            buff.append(tsuid);
            buff.append("] ");
        }
        if (id != null) {
            buff.append("with id=[");
            buff.append(id);
            buff.append("] ");
        }
        if (value != null) {
            buff.append("with value=[");
            buff.append(value);
            buff.append("] ");
        }
        if (dtype != null) {
            buff.append("with dtype=[");
            buff.append(dtype);
            buff.append("] ");
        }
        return buff.toString();
    }

    /**
     * @return the datatype
     */
    public MetaType getDType() {
        return dtype;
    }

    /**
     * @param value
     *            the value to set
     */
    public void setDType(MetaType value) {
        this.dtype = value;
    }


    /**
     * Tests the entity equality between this and obj: database identity
     *
     * Using Hibernate: advised to implement equals: see §13.1.3
     * http://docs.jboss.org/hibernate/orm/3.6/reference/en-US/html_single/#transactions-demarcation
     *
     * @param obj object to compare with
     * @return true if they match, false otherwise
     */
    @Override
    public boolean equals(Object obj) {

        if (this == obj) return true;

        if (!(obj instanceof MetaData)) return false;

        MetaData otherMeta = (MetaData) obj;
        String objTsuid = otherMeta.getTsuid();
        String objName = otherMeta.getName();
        MetaType objType = otherMeta.getDType();
        String objValue = otherMeta.getValue();

        // Avoid null pointer exceptions ...
        boolean res = Objects.equals(tsuid, objTsuid);
        res = res && Objects.equals(name, objName);
        res = res && Objects.equals(dtype, objType);
        return res && Objects.equals(value, objValue);
    }

    /**
     * Using Hibernate: advised to implement hashcode: see §13.1.3
     * http://docs.jboss.org/hibernate/orm/3.6/reference/en-US/html_single/#transactions-demarcation
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {

        return ("" + dtype + name + tsuid + value + "Meta").hashCode();
    }

}

