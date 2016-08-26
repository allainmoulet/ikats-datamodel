/**
 * 
 */
package fr.cs.ikats.temporaldata.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * ExecutorService pool factory. TODO : improve the object validation in order
 * to be able to reuse the instances correctly.
 * 
 * @author ikats
 *
 */
public class ExecutorPoolObjectFactory implements PooledObjectFactory<ExecutorService> {

    private int nbThreads;

    /**
     * constructor
     * @param nbThreads number of thread for this pool
	 * 
	 */
    public ExecutorPoolObjectFactory(int nbThreads) {
        this.nbThreads = nbThreads;
    }

    @Override
    public PooledObject<ExecutorService> makeObject() throws Exception {
        ExecutorService service = Executors.newCachedThreadPool();
        DefaultPooledObject<ExecutorService> object = new DefaultPooledObject<ExecutorService>(service);
        return object;
    }

    @Override
    public void destroyObject(PooledObject<ExecutorService> p) throws Exception {
        p.getObject().shutdownNow();
        p.getObject().awaitTermination(100, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean validateObject(PooledObject<ExecutorService> p) {
    	if (p.getObject().isTerminated()) {
    		return true;
    	} else {
    		return false;
    	}
    }

    @Override
    public void activateObject(PooledObject<ExecutorService> p) throws Exception {
    }

    @Override
    public void passivateObject(PooledObject<ExecutorService> p) throws Exception {
    }

}
