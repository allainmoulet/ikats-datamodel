/**
 */
package fr.cs.ikats.temporaldata.application;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ResourceBundle;

/**
 * Labels of the application
 */
public class ApplicationLabels {

    static {
        try {
            instance = new ApplicationLabels();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * resource bundles
     */
    private ResourceBundle bundle;
    /**
     * singleton instance
     */
    private static ApplicationLabels instance;
    
    /**
     * private contructor
     */
    private ApplicationLabels() throws IOException {
        bundle = ResourceBundle.getBundle("ApplicationLabels");
    }
    
    /**
     * singleton instance getter
     * @return instance
     */
    public static ApplicationLabels getInstance() {
        return instance;
    }
    
    /**
     * get message with label key.
     * @param label the label key
     * @return the label
     */
    public String getLabel(String label) {
        return bundle.getString(label);
    }
    
    /**
     * get Formated message with label key and params
     * @param label key of the label 
     * @param params list of parameters to use to format the message
     * @return the formatted string.
     */
    public String getLabel(String label,Object...params) {
        String pattern = bundle.getString(label);
        return MessageFormat.format(pattern, params);
    }
    
}
