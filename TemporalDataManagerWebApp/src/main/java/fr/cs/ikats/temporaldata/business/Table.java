package fr.cs.ikats.temporaldata.business;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * The Table is a business resource for IKATS: it is composed of one TableDesc section, 
 * one TableHeaders section and finally one TableContent section. Each section is detailed below.
 * 
 * TODO 158227/157215 complete table: add a map for metadata in the TableDesc section.
 */
public class Table {
    
    /**
     * The TableContent is the central part of the table, aside TableHeaders, TableDesc.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static public class TableContent {
        /**
         * The data directly contained by this table: cells structured as a list of rows.
         * A row is a list of cell. A cell is an Object, whose effective type is not fixed.
         *
         * Optional: may be missing if links are defined.
         */
        public List<List<Object>> cells;
        /** 
         * The default_links just provides mutual properties, shared by all this.links, in order to sum
         * the link information, optimizing the json serialized from Table class.
         * <br/>
         * Example: if all this.links points to timeseries, you may define 
         * <ul><li>
         * default_links.type = 'ts_list' 
         * </li><li>
         * and default_links.context = 'ts'
         * </li>
         * 
         * Optional: if there is no links, or no mutual information shared by DataLink objects.
         */
        public DataLink default_links;
        
        /**
         * The data linked by this table: links structured as a list of rows.
         * A row is a list of links. A link is an DataLink.
         *
         * Optional: may be missing, when there is no deeper content to be explored by link.
         */
        public List<List<DataLink>> links;
        
        /**
         * The public contructor required by jackson ObjectMapper
         */
        public TableContent() {
            super();
        }
    }

    /**
     * DataLink describes how to get linked data, at a deeper level.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static public class DataLink {
        
        /**
         * Functional type of the linked data. 
         */
        public String type;
        /**
         * The parameter value defining the link to the data.
         * For example: the ID value of linked data (processdata, TS, ...)
         */
        public String val;
        /**
         * The context defines how to retrieve the linked data. 
         * Non exhaustive exemples:
         * <ul>
         * <li>'processdata': when the linked content data is in the processdata database
         * </li>
         * <li>'ts' when the linked data is retrieved from timeseries database
         * </li>
         * <li>'metadata' when the linked data is retrieved from the metadata database
         * </li>
         * </ul>
         */
        public String context;
        
        /**
         * The public contructor required by jackson ObjectMapper
         */
        public DataLink() {
            super();
        }
    }
    
    /**
     *
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static public class Header {
        
        public List<Object> data;
        public DataLink default_links;
        public List<DataLink> links;
        
        /**
         * The public contructor required by jackson ObjectMapper
         */
        public Header() {
            super();
        }
    }
    /**
     * The header section of the table.
     * The headers are separated from the content.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static public class TableHeaders {
      
        public Header col;
        public Header row;
        
        /**
         * The public contructor required by jackson ObjectMapper
         */
        public TableHeaders() {
            super();
        }
    }
    
    /**
     * The description section of the table.
     * Contains different meta informations.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static public class TableDesc {
        /**
         * The title of the table
         */
        public String title;
        
        /**
         * The text describing the table
         */
        public String desc;
        
        /**
         * The public contructor required by jackson ObjectMapper.
         */
        public TableDesc() {
            super();
        }
    }
    
    /**
     * See {@link TableDesc}
     */
    public TableDesc table_desc;
    
    /**
     * See {@link TableHeaders}
     */
    public TableHeaders headers;
    
    /**
     * See {@link TableContent}
     */
    public TableContent content;
    
    /**
     * The public contructor required by jackson ObjectMapper
     */
    public Table() {
        super();
    }
     
}
