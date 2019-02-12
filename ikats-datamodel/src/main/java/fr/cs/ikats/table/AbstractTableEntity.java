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

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.SequenceGenerator;

/**
 * Superclass allowing to get a light {@link TableEntitySummary} and a full entity {@link TableEntity}
 */
@MappedSuperclass
public abstract class AbstractTableEntity {

    /**
     * Unique identifier allowing to query any table
     */
    @Id
    @SequenceGenerator(name = "table_id_seq", sequenceName = "table_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "table_id_seq")
    @Column(name = "id", updatable = false)
    private int id;

    /**
     * Unique name used to identify the TableEntity by user
     */
    @Column(name = "name", unique = true)
    private String name;

    /**
     * Give an optional title of the
     */
    @Column(name = "title")
    private String title;


    /**
     * Additional user description for the Table
     */
    @Column(name = "description")
    private String description;

    /**
     * Boolean indicating if the TableEntity contains columns headers
     */
    @Column(name = "colHeader")
    private boolean colHeader;

    /**
     * Boolean indicating if the TableEntity contains row headers
     */
    @Column(name = "rowHeader")
    private boolean rowHeader;

    /**
     * Creation date of the table
     */
    @Column(name = "created")
    private Date created;


    /**
     * Getter for the id
     *
     * @return the id to get
     */
    public int getId() {
        return id;
    }

    /**
     * Setter for the id
     *
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Getter for the name
     *
     * @return the name to get
     */
    public String getName() {
        return name;
    }

    /**
     * Setter for the name
     *
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }


    /**
     * Getter for the table title
     *
     * @return the title
     */
    public String getTitle() {
        return title;
    }


    /**
     * Setter for the table title
     *
     * @param title the title to set
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * Getter for the description
     *
     * @return the description to get
     */
    public String getDescription() {
        return description;
    }

    /**
     * Setter for the description
     *
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Getter for the colHeader
     *
     * @return the colHeader to get
     */
    public boolean hasColHeader() {
        return colHeader;
    }

    /**
     * Setter for the colHeader
     *
     * @param colHeader the colHeader to set
     */
    public void setColHeader(boolean colHeader) {
        this.colHeader = colHeader;
    }

    /**
     * Getter for the rowHeader
     *
     * @return the rowHeader to get
     */
    public boolean hasRowHeader() {
        return rowHeader;
    }

    /**
     * Setter for the rowHeader
     *
     * @param rowHeader the rowHeader to set
     */
    public void setRowHeader(boolean rowHeader) {
        this.rowHeader = rowHeader;
    }

    /**
     * Getter for the created date
     *
     * @return the created date to get
     */
    public Date getCreated() {
        return created;
    }

    /**
     * Setter for the creation date
     *
     * @param created the created date to set
     */
    public void setCreated(Date created) {
        this.created = created;
    }

}
