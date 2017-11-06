
package fr.cs.ikats.metadata.model;

import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

/**
 * model class for FunctionalIdentifier.
 */
@Entity
@Table(name = "TSFunctionalIdentifier", uniqueConstraints = {
        @UniqueConstraint(columnNames = { "tsuid", "FuncId" })
})
public class FunctionalIdentifier {


    @Id
    @Column(name = "tsuid")
    String tsuid;

    @Column(name = "FuncId", nullable = false)
    String funcId;

    /**
     * default contructor
     */
    public FunctionalIdentifier() {
        // NOTHING TO DO
    }

    /**
     * Explicit constructor
     *
     * @param tsuid
     * @param funcid
     */
    public FunctionalIdentifier(String tsuid, String funcid) {
        this.tsuid = tsuid;
        this.funcId = funcid;
    }

    /**
     * Getter
     *
     * @return the tsuid
     */
    public String getTsuid() {
        return tsuid;
    }

    /**
     * Setter
     *
     * @param tsuid the tsuid to set
     */
    public void setTsuid(String tsuid) {
        this.tsuid = tsuid;
    }

    /**
     * Getter
     *
     * @return the funcId
     */
    public String getFuncId() {
        return funcId;
    }

    /**
     * Setter
     *
     * @param funcId the funcId to set
     */
    public void setFuncId(String funcId) {
        this.funcId = funcId;
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
        StringBuilder sBuff = new StringBuilder();
        sBuff.append("FunctionalIdentifier: ");
        sBuff.append("[tsuid=");
        sBuff.append(tsuid);
        sBuff.append("][funcId=");
        sBuff.append(funcId);
        sBuff.append("]");
        return sBuff.toString();
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
    	
    	if ( this == obj) return true;
    	
    	if ( ! (obj instanceof FunctionalIdentifier)) return false;
    	
        FunctionalIdentifier otherTsIds = (FunctionalIdentifier) obj;
		String objTsuid = otherTsIds.getTsuid();
        String objFuncId = otherTsIds.getFuncId();
        
        // Avoid null pointer exceptions ...
		return  Objects.equals(tsuid, objTsuid) && Objects.equals(funcId, objFuncId);
    }
    
    /**
     * Using Hibernate: advised to implement hashcode: see §13.1.3
     * http://docs.jboss.org/hibernate/orm/3.6/reference/en-US/html_single/#transactions-demarcation
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
     
    	return (""+ tsuid + funcId + "TSIds").hashCode();
    }
}
