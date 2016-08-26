package fr.cs.ikats.datamanager.client.opentsdb.generator;

import java.text.ParseException;

import org.json.simple.JSONObject;

import fr.cs.ikats.datamanager.client.opentsdb.generator.ReaderConfiguration.ColumnConfiguration;

/**
 * to read splitted line from csv input file according to columns configuration
 * 
 * @author ikats
 *
 */
public class SplittedLineReader {

    ReaderConfiguration configuration;

    /**
     * constructor
     * @param configuration the reader configuration
     */
    public SplittedLineReader(ReaderConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * fill a simple JSON object from the index of the splitted line according
     * to columns configuration
     * 
     * @param p JSON object
     * @param splittedLine the splittedline to read
     * @param index of splittedline element
     * @return the dateValue
     * @throws ParseException if line cannot be parsed
     */
    @SuppressWarnings("unchecked")
    public Long fillObject(JSONObject p, String[] splittedLine,int index) throws ParseException {
        Long dateValue = null;
        for (ColumnConfiguration colConfig : configuration.getColumnConfigurations()) {
            int colIndex = configuration.getColumnConfigurations().indexOf(colConfig);
            if (colConfig != null) {
                if (colConfig.isTimeStampColumn) {
                       dateValue = getDateValue(splittedLine, colConfig, index+colIndex);
                       p.put(JsonConstants.KEY_TIME, dateValue);
                }
                else if (colConfig.isValueColumn) {
                    p.put(JsonConstants.KEY_VAL, splittedLine[index+colIndex]);
                }
            }
        }
        return dateValue;
    }

    /**
     * retrieve Epoch time from given timestamp format
     * 
     * @param splittedLine the splitted line to read
     * @param colConfig the col configuration fo the data
     * @param colIndex the col index of the date
     * @return the date
     * @throws ParseException if date is not paresable
     */
    protected Long getDateValue(String[] splittedLine, ColumnConfiguration colConfig, int colIndex) throws ParseException {
        String value = splittedLine[colIndex];
        Long longValue = colConfig.timestampFormat.parse(value).getTime();
        return longValue;
    }
    
}
