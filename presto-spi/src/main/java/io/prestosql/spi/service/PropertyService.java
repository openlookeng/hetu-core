/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.spi.service;

import io.airlift.units.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * PropertyService used to load configurations so different services can access easily
 *
 */
public class PropertyService
{
    private PropertyService()
    {
    }

    private static Map<String, Object> properties = new HashMap<>();

    /**
     * Return the value of the property from key, converted to String
     *
     * @param key property to get
     * @return property value as a String
     */
    public static String getStringProperty(String key)
    {
        requireNonNull(properties.get(key), String.format(Locale.ROOT, "Properties %s has not been loaded into PropertyServer correctly", key));
        return (String) properties.get(key);
    }

    /**
     * Return the value of the property from key, converted to Boolean
     *
     * @param key property to get
     * @return property value as a Boolean
     */
    public static Boolean getBooleanProperty(String key)
    {
        requireNonNull(properties.get(key), String.format(Locale.ROOT, "Properties %s has not been loaded into PropertyServer correctly", key));
        return (Boolean) properties.get(key);
    }

    /**
     * Return the value of the property from key, converted to Long
     *
     * @param key property to get
     * @return property value as a Long number
     */
    public static Long getLongProperty(String key)
    {
        requireNonNull(properties.get(key), String.format(Locale.ROOT, "Properties %s has not been loaded into PropertyServer correctly", key));
        return (Long) properties.get(key);
    }

    /**
     * Return the value of the property from key, converted to Double
     *
     * @param key property to get
     * @return property value as a Double number
     */
    public static Double getDoubleProperty(String key)
    {
        requireNonNull(properties.get(key), String.format(Locale.ROOT, "Properties %s has not been loaded into PropertyServer correctly", key));
        return (Double) properties.get(key);
    }

    /**
     * Return the value of the property from key, converted to Duration
     *
     * @param key property to get
     * @return property value as a Duration
     */
    public static List<String> getList(String key, char separator)
    {
        requireNonNull(properties.get(key), String.format("Properties %s has not been loaded into PropertyServer correctly", key));
        return new ArrayList<>(Arrays.asList(getStringProperty(key).split(separator + "")));
    }

    public static List<String> getCommaSeparatedList(String key)
    {
        return getList(key, ',');
    }

    public static Duration getDurationProperty(String key)
    {
        requireNonNull(properties.get(key), String.format(Locale.ROOT, "Properties %s has not been loaded into PropertyServer correctly", key));
        return (Duration) properties.get(key);
    }

    /**
     * Set the property by key
     *
     * @param key property to set
     * @param property value of the property
     */
    public static void setProperty(String key, Object property)
    {
        properties.put(key, property);
        return;
    }

    public static boolean containsProperty(String key)
    {
        return properties.containsKey(key);
    }
}
