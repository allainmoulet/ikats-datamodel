/**
 * $Id$
 *
 * HISTORIQUE
 *
 * VERSION : 1.0 : <US> : <NumUS> : 26 mai 2016 : Creation 
 *
 * FIN-HISTORIQUE
 */
package fr.cs.ikats.temporaldata.business;

import fr.cs.ikats.metadata.model.MetaData;

/**
 * Jsonifiable class for Rest interface: this instance represents one instance of MetaData model.
 */
public class MetadataInfo {
    
    private String name;
    private String value;
    private String tsuid;
    private String type;
    
    public MetadataInfo( MetaData model)
    {
       name = model.getName();
       value= model.getValue();
       tsuid =model.getTsuid();
       type = model.getDType().name();
    }
    
    public MetadataInfo()
    {
       name = null;
       value= null;
       tsuid =null;
       type = null;
    }

    /**
     * Getter
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Setter
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Getter
     * @return the value
     */
    public String getValue() {
        return value;
    }

    /**
     * Setter
     * @param value the value to set
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Getter
     * @return the tsuid
     */
    public String getTsuid() {
        return tsuid;
    }

    /**
     * Setter
     * @param tsuid the tsuid to set
     */
    public void setTsuid(String tsuid) {
        this.tsuid = tsuid;
    }

    /**
     * Getter
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * Setter
     * @param type the type to set
     */
    public void setType(String type) {
        this.type = type;
    }
    
}
