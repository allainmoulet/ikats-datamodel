package fr.cs.ikats.temporaldata.business;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import fr.cs.ikats.temporaldata.exception.IkatsJsonException;

/**
 * The manager is grouping services on the Table objects
 * <ul><li>
 * persistence services (java/JSON)
 * </li><li>
 * data selection services
 * </li><li>
 * data modification services
 * </li>
 * </ul>
 */
public class TableManager {
    
    
    private ObjectMapper jsonObjectMapper;
    
    /**
     * Default constructor for default configuration of jsonObjectMapper.
     */
    public TableManager()
    {
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        jsonObjectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, false);
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonObjectMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        jsonObjectMapper.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS,  false);
        
        // In case of null value for an object attribute: do not serialize associated json property.
        jsonObjectMapper.setSerializationInclusion(Include.NON_NULL);
    }
    
    /**
     * Loads a Table object from the json plain text.
     * @param jsonContent: json plain text value.
     * @return the loaded Table
     * @throws IkatsJsonException in case of parsing error.
     */
    public Table loadFromJson(String jsonContent) throws IkatsJsonException
    {
        try {
            return jsonObjectMapper.readValue( jsonContent, Table.class);
        }
        catch (Exception e) {
            throw new IkatsJsonException( "Failed to load Table business resource from the json content", e );
        }
    }
    
    /**
     * 
     * @param businessResource
     * @return
     * @throws IkatsJsonException
     */
    public String serializeToJson(Table businessResource) throws IkatsJsonException
    {
        try {
            return jsonObjectMapper.writeValueAsString(businessResource);
        }
        catch (Exception e) {
            throw new IkatsJsonException( "Failed to serialize Table business resource to the json content", e );
        }
    }
}
