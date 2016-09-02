package fr.cs.ikats.datamanager.client.opentsdb.generator;

import java.text.ParseException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * JSon generator from input csv file generate a JSON from a CSV line using a
 * SplittedLineReader with his own configuration. The configuration is used to
 * read the single splitted values of timestamps
 * 
 * @author ikats
 *
 */
public class AdvancedJsonGenerator {

    private static final Logger LOGGER = Logger.getLogger(AdvancedJsonGenerator.class);
    static final String DATA_SET_TAG_NAME = "dataset";
    static final int DIGITS_SECONDE = 10;
    static final int DIGITS_MILLISECONDE = 13;

    /**
     * list of tags
     */
    Map<String, String> tags;
    
    /**
     * metric
     */
    String metric;
    
    /**
     * input line with points
     */
    private String pointList;
    
    /**
     * splitted input line.
     */
    private String[] splittedLine;

    /**
     * start date of the timeseries
     */
    private Long lowestTimeStampValue;

    /**
     * end date of the timeseries
     */
    private Long highestTimeStampValue;

    /**
     * line reader
     */
    private SplittedLineReader lineReader;

    /**
     * constructor 
     * @param reader the line reader
     * @param metric the metric
     * @param tags the tags
     */
    public AdvancedJsonGenerator(SplittedLineReader reader, String metric, Map<String, String> tags) {
        this.metric = metric;
        this.tags = tags;
        lineReader = reader;
        this.highestTimeStampValue = 0L;
        this.lowestTimeStampValue = 0L;
    }

    /**
     * check value format
     * 
     * @param valToCheck
     * @return always true for the moment.
     */
    private boolean checkVal(String valToCheck) {
        // String correctedVal = valToCheck;
        boolean checked = false;
        // check number. prise en compte notation scifi
        // if (Pattern.matches("^(\\+|-)?[0-9]+(\\.[0-9]+E(\\+|-)[0-9]{2})?$",
        // valToCheck))
        checked = true;

        return checked;
    }

    /**
     * Converts input string to output JSON array acording to csv colums
     * configuration. Performs timestamp format check.
     * 
     * @param input
     *            string from csv file
     * @return string JSON
     * @throws ParseException 
     */
    @SuppressWarnings("unchecked")
    public String generate(String input) throws ParseException {
        // split ligne
        this.pointList = input;
        lineToArray();
        int maxIndice = splittedLine.length/lineReader.configuration.getColumnConfigurations().size();
        if(maxIndice>0) {
            
            // JSON array creation
            JSONArray points = new JSONArray();
            JSONObject p = new JSONObject();
            initTags(p);
    
            // loop JSON filling
            long date;
            long minDate = Long.MAX_VALUE;
            long maxDate = Long.MIN_VALUE;
            
            for (int i = 0; i < maxIndice; i++) {
                if (checkVal(splittedLine[i].trim())) {
                    date = lineReader.fillObject(p, splittedLine,i*lineReader.configuration.getColumnConfigurations().size());
                    points.add(p.clone());
                    
                    /* setting temporary start and end dates during parsing */
                    if (date < minDate) {
                        minDate = date;
                    }
                    if (date > maxDate) {
                        maxDate = date;
                    }
                }
            }
            /* update start and end dates attributes */
            setHighestTimeStampValue(maxDate);
            setLowestTimeStampValue(minDate);
            return points.toJSONString();
        } else {
            return null;
        }
        
        
    }

    /**
     * set the tags values and metric values from the instance attributes metric
     * and tags.
     * 
     * @param p
     *            objet point
     */
    @SuppressWarnings("unchecked")
    private void initTags(JSONObject p) {

        p.put(JsonConstants.KEY_METRIQUE, metric);
        // gestion de 2 etiquettes
        JSONObject jsonTags = new JSONObject();
        if (tags != null && !tags.isEmpty()) {
            for (String tagKey : tags.keySet()) {
                jsonTags.put(tagKey, tags.get(tagKey));
            }
        }
        else {
            jsonTags.put("metric", metric);
        }
        p.put(JsonConstants.KEY_TAGS, jsonTags);
    }

    /**
     * split line into tokens. authorized separators : ; OR : OR space TODO :
     * detect forbidden separator (, OU -)
     */
    private void lineToArray() {
        String sep;
        if (pointList.contains(";"))
            sep = ";";
        else if (pointList.contains(":"))
            sep = ":";
        else {
            sep = " ";
        }
        // replacement of , by . in numbers
        if (pointList.contains(","))
            this.splittedLine = pointList.replaceAll("\\s+", " ").replace(',', '.').split(sep);
        else
            this.splittedLine = pointList.replaceAll("\\s+", " ").split(sep);

        pointList = null;
    }

    /**
     * @return the lowestTimeStampValue
     */
    public Long getLowestTimeStampValue() {
        return lowestTimeStampValue;
    }

    /**
     * @param lowestTimeStampValue
     *            the lowestTimeStampValue to set
     */
    public void setLowestTimeStampValue(Long lowestTimeStampValue) {
        this.lowestTimeStampValue = lowestTimeStampValue;
    }

    /**
     * @return the highestTimeStampValue
     */
    public Long getHighestTimeStampValue() {
        return highestTimeStampValue;
    }

    /**
     * @param highestTimeStampValue
     *            the highestTimeStampValue to set
     */
    public void setHighestTimeStampValue(Long highestTimeStampValue) {
        this.highestTimeStampValue = highestTimeStampValue;
    }

}
