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

package gaffer.accumulostore.key;

import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.utils.Pair;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.element.Properties;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import java.util.Map;

/**
 * The Accumulo ElementConverter Interface details the methods necessary to
 * convert Gaffer {@link Element}s to Accumulo {@link Key}s and {@link Value}s
 * Some of these methods may not be required in your client code, but some
 * iterators designed for common use may make use of them.
 */
public interface AccumuloElementConverter {

    /**
     * Converts an {@link Element} {@link Pair} of keys which represent the keys
     * created from the given element. If the given element was an entity only
     * one key will be created and the second item in the pair will be null.
     *
     * @param element the element to be converted
     * @return The key(s) that represent the given element.
     * @throws AccumuloElementConversionException If conversion fails
     */
    Pair<Key> getKeysFromElement(final Element element) throws AccumuloElementConversionException;

    /**
     * Converts an {@link gaffer.data.element.Edge} to a pair of
     * {@link org.apache.accumulo.core.data.Key}s.
     *
     * @param edge The edge to be converted
     * @return The key(s) that represent the given edge
     * @throws AccumuloElementConversionException If conversion fails
     */
    Pair<Key> getKeysFromEdge(final Edge edge) throws AccumuloElementConversionException;

    /**
     * Converts an {@link gaffer.data.element.Entity} to a
     * {@link org.apache.accumulo.core.data.Key}.
     *
     * @param entity the entity to be converted
     * @return The key(s) that represent the given entity
     * @throws AccumuloElementConversionException If conversion fails
     */
    Key getKeyFromEntity(final Entity entity) throws AccumuloElementConversionException;

    /**
     * Converts a set of {@link gaffer.data.element.Properties} to an Accumulo
     * {@link org.apache.accumulo.core.data.Value}.
     *
     * @param properties the properties to use to create a Value
     * @param group      the element group
     * @return A new Accumulo {@link Value} containing the serialised {@link gaffer.data.element.Properties}
     * @throws AccumuloElementConversionException If conversion fails
     */
    Value getValueFromProperties(final Properties properties, final String group)
            throws AccumuloElementConversionException;

    /**
     * Converts the {@link gaffer.data.element.Properties} in an element to an
     * Accumulo {@link Value} where the property has a position within the store
     * schema that indicates it should be stored in the value.
     *
     * @param element the element to be converted
     * @return An Accumulo {@link Value} representing the
     * {@link gaffer.data.element.Properties} that should be stored in the value.
     * @throws AccumuloElementConversionException If conversion fails
     */
    Value getValueFromElement(final Element element) throws AccumuloElementConversionException;

    /**
     * Converts an Accumulo {@link org.apache.accumulo.core.data.Value} to a
     * {@link gaffer.data.element.Properties} object.
     *
     * @param group the element group
     * @param value the Value containing the serialised properties
     * @return A set of {@link gaffer.data.element.Properties} that represent
     * the property stored within the {@link Value}
     * @throws AccumuloElementConversionException If conversion fails
     */
    Properties getPropertiesFromValue(final String group, final Value value) throws AccumuloElementConversionException;

    /**
     * Gets a new {@link Element} from an Accumulo {@link Key}.
     *
     * @param key the Key containing serialised parts of the Element
     * @return A new {@link Element} including a partial set of
     * {@link gaffer.data.element.Properties} that were gaffer.accumulostore in the {@link Key}
     * @throws AccumuloElementConversionException If conversion fails
     */
    Element getElementFromKey(final Key key) throws AccumuloElementConversionException;

    /**
     * Gets a new {@link Element} from an Accumulo {@link Key}.
     *
     * @param key     the Key containing serialised parts of the Element
     * @param options operation options
     * @return A new {@link Element} including a partial set of
     * {@link gaffer.data.element.Properties} that were store in the {@link Key}
     * @throws AccumuloElementConversionException If conversion fails
     */
    Element getElementFromKey(final Key key, final Map<String, String> options)
            throws AccumuloElementConversionException;

    /**
     * Returns an {@link Element} populated with all the properties defined
     * within the {@link Key} and {@link Value}.
     *
     * @param key   the accumulo Key containing serialised parts of the Element
     * @param value the accumulo Value containing serialised properties of the Element
     * @return Returns an {@link Element} populated with all the properties
     * defined within the {@link Key} and {@link Value}
     * @throws AccumuloElementConversionException If conversion fails
     */
    Element getFullElement(final Key key, final Value value) throws AccumuloElementConversionException;

    /**
     * Returns an {@link Element} populated with all the properties defined
     * within the {@link Key} and {@link Value}.
     *
     * @param key     the accumulo Key containing serialised parts of the Element
     * @param value   the accumulo Value containing serialised properties of the Element
     * @param options operation options
     * @return Returns an {@link Element} populated with all the properties defined within the {@link Key}
     * and {@link Value}
     * @throws AccumuloElementConversionException If conversion fails
     */
    Element getFullElement(final Key key, final Value value, final Map<String, String> options)
            throws AccumuloElementConversionException;

    /**
     * Helper Used to create Bloom Filters, method Serialises a given object
     * (from an {@link gaffer.operation.data.EntitySeed} ) with the Identifier
     * Serialiser defined in the Store Schema.
     *
     * @param vertex the vertex identifier to serialise
     * @return A byte array representing the given object
     * @throws AccumuloElementConversionException If conversion fails
     */
    byte[] serialiseVertexForBloomKey(final Object vertex) throws AccumuloElementConversionException;

    /**
     * Creates a byte array representing a set of
     * {@link gaffer.data.element.Properties} that are to be stored in the
     * column qualifier.
     *
     * @param group      the element group
     * @param properties the element properties
     * @return A byte array representing the provided {@link gaffer.data.element.Properties} that are marked as to be stored in the provided
     * position in the {@link gaffer.accumulostore} schema.
     * @throws AccumuloElementConversionException If conversion fails
     */
    byte[] buildColumnQualifier(final String group, final Properties properties)
            throws AccumuloElementConversionException;

    /**
     * Returns a set of {@link gaffer.data.element.Properties} that are stored
     * in the part of the key that is provided.
     *
     * @param group           the element group
     * @param columnQualifier the element column qualifier properties serialised into bytes
     * @return The Properties stored within the part of the {@link Key} specified e.g Column Qualifier
     * @throws AccumuloElementConversionException If conversion fails
     */
    Properties getPropertiesFromColumnQualifier(final String group, final byte[] columnQualifier)
            throws AccumuloElementConversionException;

    /**
     * Creates a byte array representing the group.
     *
     * @param group the element group
     * @return A byte array representing the group
     * @throws AccumuloElementConversionException If conversion fails
     */
    byte[] buildColumnFamily(final String group) throws AccumuloElementConversionException;

    /**
     * Returns the element class from the given bytes.
     *
     * @param columnFamily the column family bytes
     * @return The element class
     * @throws AccumuloElementConversionException If conversion fails
     */
    String getGroupFromColumnFamily(final byte[] columnFamily) throws AccumuloElementConversionException;

    /**
     * Creates a byte array representing a set of
     * {@link gaffer.data.element.Properties} that are to be stored in the
     * column visibility.
     *
     * @param group      the element group
     * @param properties the element properties
     * @return A byte array representing the provided {@link gaffer.data.element.Properties} that are marked as to be stored in the provided
     * position in the gaffer.accumulostore schema.
     * @throws AccumuloElementConversionException If conversion fails
     */
    byte[] buildColumnVisibility(final String group, final Properties properties)
            throws AccumuloElementConversionException;

    /**
     * Returns a set of {@link gaffer.data.element.Properties} that are stored
     * in the part of the key that is provided.
     *
     * @param group            the element group
     * @param columnVisibility the element visibility property serialised into bytes
     * @return The Properties stored within the part of the {@link Key} specified e.g Column Qualifier
     * @throws AccumuloElementConversionException If conversion fails
     */
    Properties getPropertiesFromColumnVisibility(String group, byte[] columnVisibility)
            throws AccumuloElementConversionException;
}
