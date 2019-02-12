/**
 * Copyright 2018 CS Systèmes d'Information
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

package fr.cs.ikats.table;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.Table;


/**
 * The full table entity with raw properties 
 */
@Entity
@Table(name = "TableEntity")
public class TableEntity extends AbstractTableEntity {


    /**
     * Opaque data containing the 2D-array containing the values of the "cells"
     * including headers as a serialized object
     */
    @Lob
    @Column(name = "rawValues")
    private byte[] rawValues;

    /**
     * Opaque data containing the 2D-array containing the links of the "cells"
     * including headers as a serialized object
     */
    @Lob
    @Column(name = "rawDataLinks")
    private byte[] rawDataLinks;

    /**
     * Getter for the rawValues
     *
     * @return the rawValues to get
     */
    public byte[] getRawValues() {
        return rawValues;
    }

    /**
     * Setter for the rawValues
     *
     * @param rawValues the rawValues to set
     */
    public void setRawValues(byte[] rawValues) {
        this.rawValues = rawValues;
    }

    /**
     * Getter for the rawDataLinks
     *
     * @return the rawDataLinks to get
     */
    public byte[] getRawDataLinks() {
        return rawDataLinks;
    }

    /**
     * Setter for the rawDataLinks
     *
     * @param rawDataLinks the rawDataLinks to set
     */
    public void setRawDataLinks(byte[] rawDataLinks) {
        this.rawDataLinks = rawDataLinks;
    }

}
