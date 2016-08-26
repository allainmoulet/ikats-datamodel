package fr.cs.ikats.datamanager.client.opentsdb.generator;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * this class is used to store and read csv columns configuration
 * 
 * @author ikats
 *
 */
public class ReaderConfiguration {

    List<ColumnConfiguration> colConfigs;

    /**
     * default constructor
     */
    public ReaderConfiguration() {
        colConfigs = new ArrayList<ColumnConfiguration>();
    }

    /**
     * to add a column configuration
     * 
     * @param tagName
     *            name of the tag
     * @param valueFormat
     *            format of the value
     * @param timestampFormat
     *            format of the timestamp
     * @param isValueColumn
     *            if column contents value
     * @param isTimeStampColumn
     *            if column contents timestamp
     */
    public void addColumnConfiguration(String tagName, String valueFormat, DateFormat timestampFormat, boolean isValueColumn,
            boolean isTimeStampColumn) {
        colConfigs.add(new ColumnConfiguration(tagName, valueFormat, timestampFormat, isValueColumn, isTimeStampColumn));
    }

    /**
     * to add a non read column in the configuration
     */
    public void addNonReadColumnConfiguration() {
        colConfigs.add(null);
    }

    /**
     * retrieve columns configuration
     * 
     * @return the configurations
     */
    public List<ColumnConfiguration> getColumnConfigurations() {
        return colConfigs;
    }

    /**
     * retrieve the number of columns in configuration
     * 
     * @return number of columns.
     */
    public int getNumberOfColumns() {
        return colConfigs.size();
    }

    /**
     * class describing csv columns configuration
     * 
     * @author ikats
     *
     */
    public class ColumnConfiguration {
        String tagName;
        String valueFormat;
        DateFormat timestampFormat;
        boolean isValueColumn;
        boolean isTimeStampColumn;

        /**
         * 
         * @param tagName
         *            name of the tag
         * @param valueFormat
         *            format of the value
         * @param timestampFormat
         *            format of the timestamp
         * @param isValueColumn
         *            if column contents value
         * @param isTimeStampColumn
         *            if column contents timestamp
         */
        public ColumnConfiguration(String tagName, String valueFormat, DateFormat timestampFormat, boolean isValueColumn, boolean isTimeStampColumn) {
            this.tagName = tagName;
            this.valueFormat = valueFormat;
            this.timestampFormat = timestampFormat;
            this.isValueColumn = isValueColumn;
            this.isTimeStampColumn = isTimeStampColumn;
        }

    }

}
