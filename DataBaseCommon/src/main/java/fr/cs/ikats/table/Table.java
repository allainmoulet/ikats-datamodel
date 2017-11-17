package fr.cs.ikats.table;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;

/**
 * The Table entity
 */
@Entity
@javax.persistence.Table(name = "Table")
public class Table {

    /**
     * Unique identifier allowing to query any table
     */
    @Id
    @SequenceGenerator(name = "table_id_seq", sequenceName = "table_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "table_id_seq")
    @Column(name = "id", updatable = false)
    private Integer id;

    /**
     * User label allowing to identify a table in a working context
     */
    @Column(name = "label")
    private String label;

    /**
     * Additional user description
     */
    @Column(name = "description")
    private String description;

    //TODO fetch lazy
    /**
     * Opaque data containing the 2D-array containing the values of the "cells"
     * including headers
     */
    @Column(name = "rawValues")
    private byte[] rawValues;

    //TODO fetch lazy
    /**
     * Opaque data containing the 2D-array containing the links of the "cells"
     * including headers
     */
    @Column(name = "rawDataLinks")
    private byte[] rawDataLinks;

    /**
     * Boolean indicating if the Table contains columns headers
     */
    @Column(name = "colHeader")
    private boolean colHeader;

    /**
     * Boolean indicating if the Table contains row headers
     */
    @Column(name = "rowHeader")
    private boolean rowHeader;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }


    public byte[] getRawValues() {
        return rawValues;
    }

    public void setRawValues(byte[] rawValues) {
        this.rawValues = rawValues;
    }

    public byte[] getRawDataLinks() {
        return rawDataLinks;
    }

    public void setRawDataLinks(byte[] rawDataLinks) {
        this.rawDataLinks = rawDataLinks;
    }

    public boolean hasColHeader() {
        return colHeader;
    }

    public void setColHeader(boolean colHeader) {
        this.colHeader = colHeader;
    }

    public boolean hasRowHeader() {
        return rowHeader;
    }

    public void setRowHeader(boolean rowHeader) {
        this.rowHeader = rowHeader;
    }
}
