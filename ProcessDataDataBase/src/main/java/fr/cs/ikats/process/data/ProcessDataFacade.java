package fr.cs.ikats.process.data;

import fr.cs.ikats.process.data.dao.ProcessDataDAO;
import fr.cs.ikats.process.data.model.ProcessData;
import org.apache.log4j.Logger;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.InputStream;
import java.util.List;

/**
 * Facade to the storage facility for datasets
 */
@Component("ProcessDataFacade")
@Scope("singleton")
public class ProcessDataFacade {


    /**
     * the logger instance for this class
     */
    private static final Logger LOGGER = Logger.getLogger(ProcessDataFacade.class);

    /**
     * DataSetFacade
     * the DAO for acces to MetaData storage
     */
    private ProcessDataDAO dao;

    public ProcessDataFacade() {
        init();
    }

    /**
     * init the dao and its mapping : use the hibernate.cfg.xml file + add
     * package and classes where annotations are set.
     */
    public void init() {
        LOGGER.info("init of hibernate configuration with classpath resource file /processDataHibernate.cfg.xml");
        dao = new ProcessDataDAO();
        dao.init("/processDataHibernate.cfg.xml");

        dao.addAnotatedPackage("fr.cs.ikats.process.data.model");
        dao.addAnnotatedClass(ProcessData.class);
        dao.completeConfiguration();
    }

    /**
     * @param data   processData
     * @param is     input stream
     * @param length size of data
     * @return the internal identifier
     */
    public String importProcessData(ProcessData data, InputStream is, int length) {
        return dao.persist(data, is, length);
    }

    /**
     * Import a data to database
     *
     * @param processData processData to use
     * @param data        data to save
     * @return the internal identifier
     */
    public String importProcessData(ProcessData processData, String data) {
        return dao.persist(processData, data);
    }

    /**
     * get all processData for processId
     *
     * @param processId the producer
     * @return null if nothing is found.
     */
    public List<ProcessData> getProcessData(String processId) {
        return dao.getProcessData(processId);
    }

    /**
     * get the processData for internal id id
     *
     * @param id the internal identifier.
     * @return null if not found.
     */
    public ProcessData getProcessPieceOfData(int id) {
        return dao.getProcessData(id);
    }

    /**
     * remove all processResults for processId
     *
     * @param processId the producer
     */
    public void removeProcessData(String processId) {
        dao.removeAllProcessData(processId);
    }

    /**
     * destroy the facade
     */
    @PreDestroy
    public void destroy() {
        System.out.println("Destroying ProcessDataFacade");
        dao.stop();
    }
}
