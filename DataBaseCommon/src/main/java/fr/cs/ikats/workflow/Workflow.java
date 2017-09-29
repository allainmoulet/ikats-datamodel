package fr.cs.ikats.workflow;

import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

/**
 * The type Workflow.
 */
@Entity
@Table(name = "Workflow")
public class Workflow {

    @Id
    @SequenceGenerator(name = "workflow_id_seq", sequenceName = "workflow_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "workflow_id_seq")
    @Column(name = "id", updatable = false)
    private Integer id;

    @Column(name = "name")
    private String name;

    @Column(name = "description")
    private String description;

    @Column(name = "isMacroOp")
    private Boolean isMacroOp;

    @Column(name = "raw")
    private String raw;

    /**
     * Gets id.
     *
     * @return the id
     */
    public Integer getId() {
        return id;
    }

    /**
     * Sets id.
     *
     * @param id the id
     */
    public void setId(Integer id) {
        this.id = id;
    }

    /**
     * Gets name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets name.
     *
     * @param name the name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets macroOp flag.
     *
     * @return the flag indicating if this is a macro operator (true) or not (false)
     */
    public Boolean getMacroOp() {
        return isMacroOp;
    }

    /**
     * Sets macroOp flag.
     *
     * @param macroOp the boolean indicating if the item is a macro operator (true) or not (false)
     */
    public void setMacroOp(Boolean macroOp) {
        isMacroOp = macroOp;
    }

    /**
     * Gets description.
     *
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets description.
     *
     * @param description the description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Gets raw.
     *
     * @return the raw
     */
    public String getRaw() {
        return raw;
    }

    /**
     * Sets raw.
     *
     * @param raw the raw
     */
    public void setRaw(String raw) {
        this.raw = raw;
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
    	
    	if ( ! (obj instanceof Workflow)) return false;
    	
        // Avoid null pointer exceptions ...
    	Workflow otherWkf = (Workflow) obj;
		String objName = otherWkf.getName();
        String objDesc = otherWkf.getDescription();
        Boolean objIsMacroOp = otherWkf.getMacroOp();
        String objRaw = otherWkf.getRaw();
        
    	boolean res =  Objects.equals( name, objName) && Objects.equals(description, objDesc);
		return res && Objects.equals(isMacroOp, objIsMacroOp) && Objects.equals(raw, objRaw);
    }
    
    @Override
    public java.lang.String toString() {
        return "Workflow{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", isMacroOp='" + isMacroOp + '\'' +
                ", description='" + description + '\'' +
                ", raw='" + raw + '\'' +
                '}';
    }
    
    /**
     * Using Hibernate: advised to implement hashcode: see §13.1.3
     * http://docs.jboss.org/hibernate/orm/3.6/reference/en-US/html_single/#transactions-demarcation
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
    	// - avoid to involve the database key this.id: refer to mentioned doc in javadoc
    	// - do not involve raw: too big ...
    	return (""+ name + isMacroOp + description +"Wkf").hashCode();
    }
}
