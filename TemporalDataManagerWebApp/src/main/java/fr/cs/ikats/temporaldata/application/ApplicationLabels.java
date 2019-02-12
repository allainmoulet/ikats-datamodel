/**
 * Copyright 2018 CS Systèmes d'Information
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.cs.ikats.temporaldata.application;

import java.text.MessageFormat;
import java.util.ResourceBundle;

/**
 * Labels of the application
 */
public class ApplicationLabels {

    static {
        instance = new ApplicationLabels();
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
    private ApplicationLabels() {
        bundle = ResourceBundle.getBundle("ApplicationLabels");
    }

    /**
     * singleton instance getter
     *
     * @return instance
     */
    public static ApplicationLabels getInstance() {
        return instance;
    }

    /**
     * get message with label key.
     *
     * @param label the label key
     * @return the label
     */
    public String getLabel(String label) {
        return bundle.getString(label);
    }

    /**
     * get Formated message with label key and params
     *
     * @param label  key of the label
     * @param params list of parameters to use to format the message
     * @return the formatted string.
     */
    public String getLabel(String label, Object... params) {
        String pattern = bundle.getString(label);
        return MessageFormat.format(pattern, params);
    }

}
