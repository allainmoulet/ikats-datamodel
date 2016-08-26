package fr.cs.ikats.temporaldata.application;

import javax.annotation.PreDestroy;
import javax.ws.rs.ApplicationPath;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

import fr.cs.ikats.temporaldata.utils.ExecutorManager;

/**
 * Application Class for temporal data Webapp.
 * Path is root 
 */
@ApplicationPath("/")
public class TemporalDataApplication extends ResourceConfig {
	
    /**
     * configuration for this application
     */
	private static ApplicationConfiguration CONFIGURATION = new ApplicationConfiguration();
	
	/**
	 * get jersey CONFIGURATION
	 * @return CONFIGURATION
	 */
	public static ApplicationConfiguration getApplicationConfiguration() {
		return CONFIGURATION;
	}

	/**
	 * add resource package as Jersey resources classes
	 * register the import executor pool.
	 */
	public TemporalDataApplication() {
	    
	    // register multipart feature and jackson JSON for all resources.
		packages("fr.cs.ikats.temporaldata.resource").register(MultiPartFeature.class).register(JacksonFeature.class);
		packages("fr.cs.ikats.temporaldata.exception");
		// registering thread pool for import
		ExecutorManager.getInstance().registerExecutorPool(ApplicationConfiguration.IMPORT_THREAD_POOL_NAME, getImportExecutorServiceSize(),getImportExecutorPoolSize());
	    if(CONFIGURATION == null) {
	        CONFIGURATION = new ApplicationConfiguration();
	    }
		CONFIGURATION.getSpringContext();
	}

	/**
	 * get the Import thread numbers.
	 * @return
	 */
	private int getImportExecutorServiceSize() {
		return TemporalDataApplication.getApplicationConfiguration().getIntValue(ApplicationConfiguration.IMPORT_EXECUTOR_SERVICE_SIZE);
	}
	
	/**
	 * get the Import executor Pool Size.
	 * @return
	 */
	private int getImportExecutorPoolSize() {
		return TemporalDataApplication.getApplicationConfiguration().getIntValue(ApplicationConfiguration.IMPORT_EXECUTOR_POOL_SIZE);
	}

    /**
     * {@inheritDoc}
     */
    @Override
    
    protected void finalize() throws Throwable {
        
        super.finalize();
        
        
    }
	
    /**
     * destroy the Application
     */
    @PreDestroy
    public void destroy() {
        System.out.println("DESTROYING TemporalDataApplication");
        ExecutorManager.getInstance().stopExecutors();
        //CONFIGURATION = null;
    }
	
	
}
