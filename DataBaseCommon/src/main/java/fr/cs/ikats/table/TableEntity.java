package fr.cs.ikats.table;

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Transient;

import org.hibernate.bytecode.javassist.FieldHandled;
import org.hibernate.bytecode.javassist.FieldHandler;


/**
 * The Table entity
 */
@Entity
@javax.persistence.Table(name = "TableEntity")
public class TableEntity implements FieldHandled {

    @Transient
    private FieldHandler fieldHandler;

    /**
     * Unique identifier allowing to query any table
     */
    @Id
    @SequenceGenerator(name = "table_id_seq", sequenceName = "table_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "table_id_seq")
    @Column(name = "id", updatable = false)
    private Integer id;

    /**
     * Unique name used to identify the TableEntity by user
     */
    @Column(name = "name", unique = true)
    private String name;

    /**
     * Additional user description
     */
    @Column(name = "description")
    private String description;

    /**
     * Opaque data containing the 2D-array containing the values of the "cells"
     * including headers
     */
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "rawValues")
    private byte[] rawValues;

    /**
     * Opaque data containing the 2D-array containing the links of the "cells"
     * including headers
     */
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "rawDataLinks")
    private byte[] rawDataLinks;

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


    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }


    public byte[] getRawValues() {
        if (fieldHandler != null) {
            return (byte[]) fieldHandler.readObject(this, "rawValues", rawValues);
        }
        return rawValues;
    }

    public void setRawValues(byte[] rawValues) {
        if (fieldHandler != null) {
            fieldHandler.writeObject(this, "rawValues", this.rawValues, rawValues);
            return;
        }
        this.rawValues = rawValues;
    }

    public byte[] getRawDataLinks() {
        if (fieldHandler != null) {
            return (byte[]) fieldHandler.readObject(this, "rawDataLinks", rawDataLinks);
        }
        return rawDataLinks;
    }

    public void setRawDataLinks(byte[] rawDataLinks) {
        if (fieldHandler != null) {
            fieldHandler.writeObject(this, "rawDataLinks", this.rawDataLinks, rawDataLinks);
            return;
        }
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

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    @Override
    public void setFieldHandler(FieldHandler handler) {
        this.fieldHandler = handler;
    }

    @Override
    public FieldHandler getFieldHandler() {
        return this.fieldHandler;
    }
}
