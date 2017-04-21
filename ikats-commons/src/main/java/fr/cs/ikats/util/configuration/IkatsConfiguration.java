package fr.cs.ikats.util.configuration;

/**
 * <p>
 * Default implementation for {@link AbstractIkatsConfiguration}<br>
 * Any new Ikats configuration property set (mainly provided with a file) could be provided with a new instance of that class<br>
 * </p>
 * <p>Example :<br>
 * <code>
 * private IkatsConfiguration<ApplicationConfiguration> config = new IkatsConfiguration<ApplicationConfiguration>(ApplicationConfiguration.class);
 * </code><br><br>
 * With <code>ApplicationConfiguration</code> as an implementation of {@link ConfigProperties}
 * </p>
 * 
 * @author ftoral
 * @param <T> as an {@link Enum} implementing {@link ConfigProperties}
 */
public class IkatsConfiguration<T extends Enum<T> & ConfigProperties> extends AbstractIkatsConfiguration<T> {

	public IkatsConfiguration(Class<T> configPropertiesClazz) {
		super.init(configPropertiesClazz);
	}

}
	