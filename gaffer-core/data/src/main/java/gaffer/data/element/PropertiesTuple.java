/*
 * Copyright 2016 Crown Copyright
 *
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

package gaffer.data.element;

import gaffer.function.Tuple;

/**
 * An <code>PropertiesTuple</code> implements {@link gaffer.function.Tuple} wrapping a
 * {@link Properties} and providing a getter and setter for the element's property values.
 * This class allows Properties to be used with the function module whilst minimising dependencies.
 */
public class PropertiesTuple implements Tuple<ElementComponentKey> {

    private Properties properties;

    public PropertiesTuple() {
    }

    public PropertiesTuple(final Properties properties) {
        this.properties = properties;
    }

    @Override
    public Object get(final ElementComponentKey key) {
        return properties.get(key.getPropertyName());
    }

    @Override
    public void put(final ElementComponentKey key, final Object value) {
        properties.put(key.getPropertyName(), value);
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "PropertiesTuple{"
                + "properties=" + properties
                + '}';
    }
}
