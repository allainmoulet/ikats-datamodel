/**
 * LICENSE:
 * --------
 * Copyright 2017 CS SYSTEMES D'INFORMATION
 *
 * Licensed to CS SYSTEMES D'INFORMATION under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. CS licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * @author Fabien TORAL <fabien.toral@c-s.fr>
 * @author Fabien TORTORA <fabien.tortora@c-s.fr>
 */

package fr.cs.ikats.util.configuration;

import java.text.MessageFormat;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class with goal is to provide a "typed" configuration for IKATS.<br>
 * Each configuration property shall be defined in a {@link ConfigProperties} enumeration which also should define the properties file.<br>
 * This would be robust because each call to a configuration property is a constant widely available.<br>
 * <br>
 * In combination with {@link IkatsConfiguration} for the default implementation, that leads to define configuration into one java {@link Enum} only.
 *
 * @param <T> as an {@link Enum} implementing {@link ConfigProperties}
 */
public abstract class AbstractIkatsConfiguration<T extends Enum<T> & ConfigProperties> extends CompositeConfiguration {

    private T properties;

    private Logger logger = LoggerFactory.getLogger(AbstractIkatsConfiguration.class);

    public void init(Class<T> propertiesClass) {
        try {
            properties = propertiesClass.getEnumConstants()[0];
            Configurations configurations = new Configurations();
            PropertiesConfiguration conf = configurations.properties(getClass().getResource("/" + properties.getPropertiesFilename()));
            addConfiguration(conf);
        } catch (ConfigurationException e) {
            logger.error("Could not load configuration for {} from properties file {}", getClass().getName(), properties.getPropertiesFilename());
            logger.error("Exception: ", e);
            // throw new IngestionException(e);
        }
    }

    /**
     * Overriden implementation to check that the property is defined by the IngestionConfig enum.<br>
     * Avoid direct <code>getProperty(String key)</code> call with a key which is not present in the enum.
     */
    @Override
    protected Object getPropertyInternal(String key) {
        try {
            // Chek that the property is in the enum list
            @SuppressWarnings("unchecked")
            T valueEnum = getEnum((Class<T>) properties.getClass(), key);
            return getProperty(valueEnum);
        } catch (IllegalArgumentException | NullPointerException e) {
            logger.debug("Property '{}' not found in {}", key, properties.getPropertiesFilename(), e);
        }

        return null;
    }

    /**
     * Provide same behavior as the equivalent method in {@link AbstractConfiguration}, but for an IKATS {@link ConfigProperties} enum.
     *
     * @param propertyKey
     * @return
     */
    public Object getProperty(T propertyKey) {
        if (propertyKey == null) {
            return null;
        }

        // call the super implementation to get the property value or fall back to default if null
        Object value = super.getPropertyInternal(propertyKey.getPropertyName());
        if (value == null) {
            value = propertyKey.getDefaultValue();
        }

        return value;
    }

    /**
     * Use a {@link MessageFormat} internally to format the configuration property
     *
     * @param propertyKey
     * @param values
     * @return
     */
    public String formatProperty(T propertyKey, Object... values) {
        String message = (String) getProperty(propertyKey);
        if (message != null) {
            MessageFormat messageFormat = new MessageFormat(message);
            message = messageFormat.format(values);
        }
        return message;
    }

    /**
     * Get the IKATS {@link ConfigProperties} enum constant that matches the <code>key</code>
     *
     * @return the {@link ConfigProperties} matching constant
     */
    private T getEnum(Class<T> enumClass, String key) {
        T[] enumConstants = enumClass.getEnumConstants();
        for (int i = 0; i < enumConstants.length; i++) {
            T t = enumConstants[i];
            if (t.getPropertyName().equals(key)) {
                return t;
            }
        }
        return null;
    }

    /**
     * Provide same behavior as the equivalent method in {@link AbstractConfiguration}, but for an IKATS {@link ConfigProperties} enum.
     *
     * @see org.apache.commons.configuration2.AbstractConfiguration#getString(java.lang.String)
     */
    public String getString(T propertyKey) {
        return super.getString(propertyKey.getPropertyName());
    }

    /**
     * Provide same behavior as the equivalent method in {@link AbstractConfiguration}, but for an IKATS {@link ConfigProperties} enum.
     *
     * @see org.apache.commons.configuration2.AbstractConfiguration#getBoolean(java.lang.String)
     */
    public boolean getBoolean(T propertyKey) {
        return super.getBoolean(propertyKey.getPropertyName());
    }

    /**
     * Provide same behavior as the equivalent method in {@link AbstractConfiguration}, but for an IKATS {@link ConfigProperties} enum.
     *
     * @see org.apache.commons.configuration2.AbstractConfiguration#getDouble(java.lang.String)
     */
    public double getDouble(T propertyKey) {
        return super.getDouble(propertyKey.getPropertyName());
    }

    /**
     * Provide same behavior as the equivalent method in {@link AbstractConfiguration}, but for an IKATS {@link ConfigProperties} enum.
     *
     * @see org.apache.commons.configuration2.AbstractConfiguration#getFloat(java.lang.String)
     */
    public float getFloat(T propertyKey) {
        return super.getFloat(propertyKey.getPropertyName());
    }

    /**
     * Provide same behavior as the equivalent method in {@link AbstractConfiguration}, but for an IKATS {@link ConfigProperties} enum.
     *
     * @see org.apache.commons.configuration2.AbstractConfiguration#getInt(java.lang.String)
     */
    public int getInt(T propertyKey) {
        return super.getInt(propertyKey.getPropertyName());
    }

    /**
     * Provide same behavior as the equivalent method in {@link AbstractConfiguration}, but for an IKATS {@link ConfigProperties} enum.
     *
     * @see org.apache.commons.configuration2.AbstractConfiguration#getLong(java.lang.String)
     */
    public long getLong(T propertyKey) {
        return super.getLong(propertyKey.getPropertyName());
    }

}

