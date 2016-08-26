package fr.cs.ikats.datamanager.client.opentsdb;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * result of meta query
 */
public class QueryMetaResult implements Serializable {

    private static final long serialVersionUID = 8623274622284411448L;
    /**
     * key : tsuid / vals : tags values
     */
    private Map<String, String[]> series;

    /**
     * constructor
     */
    public QueryMetaResult() {
        series = new LinkedHashMap<String, String[]>();
    }

    /**
     * add serie 
     * @param tsuid the tsuid
     * @param tag1 a tag
     * @param tag2 a tag
     */
    public void addSerie(String tsuid, String tag1, String tag2) {
        series.put(tsuid, new String[] { tag1, tag2 });
    }

    /**
     * @return the series
     */
    public Map<String, String[]> getSeries() {
        return series;
    }

    /**
     * Getter 
     * @return the tsuids of the series
     */
    public Set<String> getTsuids() {
        return series.keySet();
    }

}
