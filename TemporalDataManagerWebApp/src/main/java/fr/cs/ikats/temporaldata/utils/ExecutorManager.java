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

package fr.cs.ikats.temporaldata.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.log4j.Logger;

import fr.cs.ikats.datamanager.DataManagerException;

/**
 * ExecutorService Manager. To use this class First register a new
 * ExecutorService. Each executor will be added into a pool. Then get the
 * executor service which will be borrowed from the pool. Then use it to execute
 * Runnable or Callable tasks. user awaitTermination to drive the completion of
 * the tasks. This method will also return the ExeuctorServices into the pool to
 * be available for other requests.
 *
 */
public class ExecutorManager {

    /**
     * private LOGGER instance
     */
    private Logger logger = Logger.getLogger(ExecutorManager.class);

    /**
     * map of executor pools
     */
    Map<String, GenericObjectPool<ExecutorService>> executors;

    /**
     * singleton instance
     */
    private static ExecutorManager instance = new ExecutorManager();

    /**
     * private constructor
     */
    private ExecutorManager() {
        executors = new HashMap<>();
    }

    /**
     * ask for an executor service, will block until an executor is available
     * from the pool for the executorName.
     *
     * @param executorName name of the executor to ask.
     * @return the executorService from the pool
     * @throws DataManagerException
     */
    public ExecutorService getExecutorService(String executorName) throws DataManagerException {
        try {
            logger.debug("ask executor " + executorName + " from pool");
            ExecutorService executor = executors.get(executorName).borrowObject();
            logger.debug("executor " + executorName + " borrowed from pool");
            return executor;
        } catch (Exception e) {
            throw new DataManagerException("Executor " + executorName + "can't borrow object: " + e.getMessage(), e);
        }
    }

    /**
     * Manage the return of {@link ExecutorService} to the pool named poolName
     *
     * @param poolName
     * @param executorService
     * @throws DataManagerException if one of argument is null or if the pool is not found.
     */
    public void returnExecutorService(String poolName, ExecutorService executorService) throws DataManagerException {

        if (poolName == null || executorService == null) {
            throw new DataManagerException("Can't return to ExecutoService pool: name=" + poolName + ", executor=" + executorService);
        }

        GenericObjectPool<ExecutorService> pool = executors.get(poolName);
        if (pool == null) {
            throw new DataManagerException("Pool name (" + poolName + ") not found");
        } else {
            if (logger.isDebugEnabled()) logger.debug("Return Executor service " + executorService + " to pool");
            pool.returnObject(executorService);
        }
    }

    /**
     * @param executor     the executor previously borrowed
     * @param runnable     the runnalbe task to run
     * @param executorName the name of the executor
     * @deprecated FTO voir à supprimer ou revoir la façon dont travaille l'executor manager... 1 pool par fichier
     * d'import pourrait être plus efficace.
     * execute a Runnable into the executor.
     */
    public void execute(ExecutorService executor, Runnable runnable, String executorName) {
        if (executor == null) {
            logger.error("executor " + executorName + " not registered");
        } else {
            logger.debug("execute runnable in executor " + executorName);
            executor.execute(runnable);
            logger.debug("runnable added to executor " + executorName);
        }
    }

    /**
     * @param executor     the executor previously borrowed
     * @param callable     the callable task to run
     * @param executorName the name of the executor
     * @param <T>          the result type of method call
     * @return a list of execution result
     * @deprecated FTO voir à supprimer ou revoir la façon dont travaille l'executor manager... 1 pool par fichier
     * d'import pourrait être plus efficace.
     * execute a Callable into the executor.
     */
    public <T> Future<T> execute(ExecutorService executor, Callable<T> callable, String executorName) {
        if (executor == null) {
            logger.error("executor " + executorName + " not registered");
            return null;
        } else {
            logger.debug("submit callable in executor " + executorName);
            return executor.submit(callable);
        }
    }

    /**
     * add an ExecutorService pool into the Manager .
     * For the moment, pool is 1 size fixed.
     *
     * @param name     name of the executor
     * @param poolSize size of the executor pool
     */
    public void registerExecutorPool(String name, int poolSize) {
        logger.debug("registering executor " + name + " into manager");
        GenericObjectPool<ExecutorService> pool = new GenericObjectPool<>(new ExecutorPoolObjectFactory());
        pool.setMaxTotal(poolSize);
        pool.setTestOnReturn(true);
        if (!executors.containsKey(name)) {
            executors.put(name, pool);
        }
    }

    /**
     * call this method to bock until all running or submited task finish.
     *
     * @param service  the executor
     * @param name     the name of the executor
     * @param delay    the delay to wait
     * @param timeunit timeunit of the delay
     * @param remove   if true, remove definitly the named Executor from the Manager.
     */
    public void waitForExecutorTermination(ExecutorService service, String name, long delay, TimeUnit timeunit,
                                           boolean remove) {
        logger.info("Stopping Executor " + name);
        if (executors.get(name) != null) {
            service.shutdown();
            try {
                boolean terminationNominal = service.awaitTermination(delay, timeunit);
                if (terminationNominal == false) {
                    logger.warn("Timeout occured for executor " + name);
                }
            } catch (InterruptedException ie) {
                logger.warn("Executor " + name + " interrupted", ie);
                Thread.currentThread().interrupt();
            } finally {
                executors.get(name).returnObject(service);
                logger.debug("executor " + name + " returned into pool");
            }
        }
        if (remove) {
            logger.debug("remove pool " + name + " from manager");
            executors.get(name).close();
            executors.remove(name);
        }
    }

    /**
     * stop all executors
     */
    public void stopExecutors() {
        logger.debug("remove all pools from manager");
        for (GenericObjectPool<ExecutorService> pool : executors.values()) {
            logger.debug("stopping pool");
            pool.close();
        }
        for (String name : executors.keySet()) {
            executors.remove(name);
        }
    }

    /**
     * singleton instance accessor.
     *
     * @return the instance
     */
    public static ExecutorManager getInstance() {
        return instance;
    }

}
