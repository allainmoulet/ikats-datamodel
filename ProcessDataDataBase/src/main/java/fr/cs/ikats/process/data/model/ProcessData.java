package fr.cs.ikats.process.data.model;

import java.sql.Blob;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * ProcessData model : 
 * 
 */
@Entity
@Table(name = "ProcessData")
public class ProcessData {
   
    /**
     * constructor 
     * @param processId the processId
     * @param dataType the dataType
     * @param name the name of the result
     */
    public ProcessData(String processId ,String dataType,String name) {
        this.processId=processId;
        this.dataType=dataType;
        this.name=name;
    }

    @SuppressWarnings("unused")
    private ProcessData() {
        
    }
    
    @Transient
    private static final long serialVersionUID = 1L;

    /**
     * HQL request
     */
    public static final String LIST_ID_FOR_PROCESSID = "select pd.id from ProcessData pd where pd.processId = :processId";
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID", unique = true, nullable = false)
    private Integer id;

    @Column(unique = false, nullable = false, length = 100)
    private String processId;
    
    @Column(unique = false, nullable = false, length = 50)
    private String dataType;
    
    @Column(unique = false, nullable = true,length = 100)
    private String name;
    
    @Lob
    @Column(name = "DATA", unique = false, nullable = false)
    @JsonIgnore
    private Blob data;
    
    
    /**
     * Getter
     * @return the data
     */
    @JsonIgnore
    public Blob getData() {
        return data;
    }
    
    /**
     * Setter
     * @param data the data to set
     */
    public void setData(Blob data) {
        this.data = data;
    }

    /**
     * Getter
     * @return the id
     */
    public Integer getId() {
        return id;
    }

    /**
     * Getter
     * @return the processId
     */
    public String getProcessId() {
        return processId;
    }

    /**
     * Getter
     * @return the dataType
     */
    public String getDataType() {
        return dataType;
    }

    /**
     * Getter
     * @return the name
     */
    public String getName() {
        return name;
    }
    
    /**
     * Setter
     * @param id the id to set
     */
    public void setId(Integer id) {
        this.id = id;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer("{dataType ");
        sb.append("id : ").append(id).append(",");
        sb.append("processId : ").append(processId).append(",");
        sb.append("dataType : ").append(dataType);
        sb.append("}");
        return sb.toString();
        
    }
}
