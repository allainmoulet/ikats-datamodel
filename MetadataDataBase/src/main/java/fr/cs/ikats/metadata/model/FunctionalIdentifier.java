
package fr.cs.ikats.metadata.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * model class for FunctionalIdentifier.
 */
@Entity
@Table(name = "TSFunctionalIdentifier")
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
     * Tests the parameter object contains the same tsuid and funcid
     *
     * @param obj object to compare with
     * @return true if they match, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        return ((obj instanceof FunctionalIdentifier) &&
                (((FunctionalIdentifier) obj).getTsuid().equals(tsuid)) &&
                (((FunctionalIdentifier) obj).getFuncId().equals(funcId)));
    }
}
