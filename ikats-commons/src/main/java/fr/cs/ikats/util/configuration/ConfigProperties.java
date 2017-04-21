package fr.cs.ikats.util.configuration;

// Review#147170 javadoc résumant le principe de cette interface et lien avec autres classes
public interface ConfigProperties {

	public String getPropertiesFilename();

	public String getPropertyName();

	public String getDefaultValue();

}
