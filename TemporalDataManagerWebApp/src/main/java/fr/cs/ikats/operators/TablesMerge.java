package fr.cs.ikats.operators;

import fr.cs.ikats.temporaldata.business.Table;
import fr.cs.ikats.temporaldata.business.TableInfo;
import fr.cs.ikats.temporaldata.business.TableManager;

/**
 * IKATS Operator Tables Merge<br>
 * 
 * Provides
 * 
 */
public class TablesMerge {

    /**
     * The JSON Request
     * 
     * @author ftoral
     *
     */
    public static class Request {

        public TableInfo[] tables;
        public String      joinOn;
        public String      outputTableName;

        public Request() {
            ; // default constructor
        }
    }

    private Request      request;
    private TableManager tableManager;

    /**
     * Table Merge operator initialization
     * 
     * @param request the input data provided to the operator
     * @throws IkatsOperatorException
     */
    public TablesMerge(Request request) throws IkatsOperatorException {

        // check the inputs
        if (request.tables.length < 2) {
            throw new IkatsOperatorException("There should be 2 tables for a merge");
        }

        this.request = request;
        this.tableManager = new TableManager();
    }

    /**
     * Operator processing for the merge
     * 
     * @return the merged table
     * @throws IkatsOperatorException
     */
    public Table doMerge() throws IkatsOperatorException {
        throw new IkatsOperatorException("Operator not implemented");
    }
}
