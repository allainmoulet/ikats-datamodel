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

package fr.cs.ikats.workflow;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.SequenceGenerator;

/**
 * The type Workflow.
 */
@MappedSuperclass
public abstract class AbstractWorkflowEntity {

    @Id
    @SequenceGenerator(name = "workflow_id_seq", sequenceName = "workflow_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "workflow_id_seq")
    @Column(name = "id", updatable = false)
    private Integer id;

    /**
     * Unique name used to identify the AbstractWorkflowEntity by user
     */
    @Column(name = "name", unique = true)
    private String name;

    @Column(name = "description")
    private String description;

    @Column(name = "isMacroOp")
    private Boolean isMacroOp;

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
     * Using Hibernate: advised to implement hashcode and therefore equals: see §13.1.3
     * http://docs.jboss.org/hibernate/orm/3.6/reference/en-US/html_single/#transactions-demarcation
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        // - avoid to involve the database key this.id: refer to mentioned doc in javadoc
        // - do not involve raw: too big ...
        return ("" + name + isMacroOp + description + "Wkf").hashCode();
    }
    
    /**
     * Following the {@link #hashCode()} rationale, overrides the {@link Object#equals(Object)}
     */
	@Override
	public boolean equals(Object obj) {
		// Same instance 
		if (this == obj) {
			return true;
		}
		
		// compared to null -> false
		if (obj == null) {
			return false;
		}
		
		// No the same Class -> false
		if (getClass() != obj.getClass()) {
			return false;
		}
		
		// Previous checks lets us confident in casting
		AbstractWorkflowEntity other = (AbstractWorkflowEntity) obj;
		
		// Check all properties equality
		if (description == null) {
			if (other.description != null) {
				return false;
			}
		} else if (!description.equals(other.description)) {
			return false;
		}
		
		if (isMacroOp == null) {
			if (other.isMacroOp != null) {
				return false;
			}
		} else if (!isMacroOp.equals(other.isMacroOp)) {
			return false;
		}
		
		if (name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (!name.equals(other.name)) {
			return false;
		}
		
		// Finally -> We're all good
		return true;
	}
}
