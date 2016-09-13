package fr.cs.ikats.workflow;

import javax.persistence.*;
import java.sql.Blob;

@Entity
@Table(name = "Workflow")
public class Workflow {

	@Id
    @SequenceGenerator(name="workflow_id_seq", sequenceName="workflow_id_seq", allocationSize=1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator="workflow_id_seq")
    @Column(name = "id", updatable = false)
    private Integer id;

	@Column(name = "name")
	private String name;

	@Column(name = "description")
	private String description;

    @Lob
	@Column(name = "raw")
	private Blob raw;
		
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

	public Blob getRaw() {
		return raw;
	}

	public void setRaw(Blob raw) {
		this.raw = raw;
	}

	@java.lang.Override
	public java.lang.String toString() {
		return "Workflow{" +
				"id=" + id +
				", name='" + name + '\'' +
				", description='" + description + '\'' +
				", raw='" + raw.toString() + '\'' +
				'}';
	}
}
