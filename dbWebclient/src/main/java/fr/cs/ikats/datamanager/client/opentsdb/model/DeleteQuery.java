/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 28 janv. 2016 : Creation 
 *
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.datamanager.client.opentsdb.model;

/**
 *
 */
public class DeleteQuery extends TSQuery {

    boolean delete = true;
    
    /**
     * 
     * @param start
     * @param end
     */
    public DeleteQuery(String start, String end) {
        super(start, end);
    }

    /**
     * @param start
     * @param end
     * @param tsuid
     */
    public DeleteQuery(String start, String end, String tsuid) {
        super(start, end, tsuid);
    }

    /**
     * Getter
     * @return the delete
     */
    public boolean isDelete() {
        return delete;
    }

    /**
     * Setter
     * @param delete the delete to set
     */
    public void setDelete(boolean delete) {
        this.delete = delete;
    }

}
