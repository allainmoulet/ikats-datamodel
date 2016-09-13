package fr.cs.ikats.workflow;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import org.apache.log4j.Logger;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;


@Component("WorkflowFacade")
@Scope("singleton")
public class WorkflowFacade {

    /**
     * Logger declaration
     */
    private static final Logger LOGGER = Logger.getLogger(WorkflowFacade.class);

    /**
     * DAO object to access the Workflow storage
     */
    private WorkflowDAO dao;

    /**
     * Constructor
     */
    public WorkflowFacade() {

        init();
    }

    /**
     * Initialize the DAO
     */
    public void init() {
        dao = new WorkflowDAO();
        dao.init("/workflowHibernate.cfg.xml");

        dao.addAnnotatedClass(Workflow.class);
        dao.completeConfiguration();
    }

    /**
     * Create Workflow in database
     *
     * @param wf        workflow object
     *
     * @return the ID of the inserted data
     * @throws IkatsDaoConflictException create error raised on conflict with another resource
     * @throws IkatsDaoException         another error from DAO
     */
    public Integer persist(Workflow wf) throws IkatsDaoConflictException, IkatsDaoException {

        return dao.persist(wf);
    }
    /**
     * Create Workflow in database for a given name, description and raw content
     *
     * @param name        name of the workflow
     * @param description description of the workflow
     * @param raw         content of the workflow as a json
     *
     * @return the ID of the inserted data
     * @throws IkatsDaoConflictException create error raised on conflict with another resource
     * @throws IkatsDaoException         another error from DAO
     */
    public Integer persist(String name, String description, String raw) throws IkatsDaoConflictException, IkatsDaoException {

        Workflow wf = new Workflow();
        wf.setName(name);
        wf.setDescription(description);
        wf.setRaw(raw);

        return dao.persist(wf);
    }

    /**
     * List all workflows
     *
     * @return The list of all workflow
     * @throws IkatsDaoException if there is no workflow
     */
    public List<Workflow> listAll() throws IkatsDaoException {
        return dao.listAll();
    }


    /**
     * Get a workflow content by providing its id
     *
     * @param id id of the workflow
     *
     * @return the workflow matching this id
     * @throws IkatsDaoException
     */
    public Workflow getById(Integer id) throws IkatsDaoException {
        return dao.getById(id);
    }

    /**
     * Update a workflow with new values
     *
     * @param id identifier of the workflow (before it changes)
     * @param name new name to apply to workflow
     * @param description new description of the workflow
     * @param raw new content of the workflow
     *
     * @return true if the workflow update is successful
     * @throws IkatsDaoConflictException if the new name is already used (not unique)
     * @throws IkatsDaoException if any other exception occurs
     */
    public boolean update(Integer id, String name, String description, String raw) throws IkatsDaoConflictException, IkatsDaoException {
        Workflow wf = dao.getById(id);
        wf.setName(name);
        wf.setDescription(description);
        wf.setRaw(raw);
        return dao.update(wf);
    }

    /**
     * Update a workflow with new values based on its id
     *
     * @param wf Updated Workflow object
     *
     * @return true if the workflow update is successful
     * @throws IkatsDaoConflictException if the new name is already used (not unique)
     * @throws IkatsDaoException if any other exception occurs
     */
    public boolean update(Workflow wf) throws IkatsDaoConflictException, IkatsDaoException {
        return dao.update(wf);
    }

    /**
     * Delete a workflow identified by its id
     *
     * @param id identifier of the workflow
     *
     * @return the id of the removed workflow
     * @throws IkatsDaoException if the workflow couldn't be removed
     */
    public int removeById(Integer id) throws IkatsDaoException {
        return dao.removeById(id);
    }


    /**
     * Delete a workflow identified by its name
     *
     * @param name name identifying the workflow to remove
     *
     * @return the id of the removed workflow     *
     * @throws IkatsDaoException if the workflow couldn't be removed
     */
    public int removeByName(String name) throws IkatsDaoException {
        return dao.removeByName(name);
    }
}