package fr.cs.ikats.process.data;

import fr.cs.ikats.process.data.dao.ProcessDataDAO;
import fr.cs.ikats.process.data.model.ProcessData;
import org.apache.log4j.Logger;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
     *               Review#156651 ajout d'un commentaire sur la possibilité d'utiliser -1 comme longueur utilisé {@link ProcessDataTest}
     * @param length size of data, could -1 to read until the end of the stream.
     * @return the internal identifier
     * @throws IOException
     */
    public String importProcessData(ProcessData data, InputStream is, int length) throws IOException {

        // Prepare a buffer to get the table of bytes from the InputStream
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        // Put a boolean flag for the case where length = -1
        boolean canRead = true;

        for (int i = 0; i < length || canRead; i++) {
            int read = is.read();
            if (read != -1) {
                buffer.write(read);
            } else {
                canRead = false;
            }
        }
        buffer.flush();

        return dao.persist(data, buffer.toByteArray());
    }

    /**
     * Import a data to database
     *
     * @param processData processData to use
     * @param data        data to save
     * @return the internal identifier
     */
    public String importProcessData(ProcessData processData, String data) {
        return dao.persist(processData, data.getBytes());
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
