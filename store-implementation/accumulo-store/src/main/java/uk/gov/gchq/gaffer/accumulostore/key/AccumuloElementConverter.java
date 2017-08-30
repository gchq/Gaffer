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

package uk.gov.gchq.gaffer.accumulostore.key;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import uk.gov.gchq.gaffer.accumulostore.utils.BytesAndRange;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.id.ElementId;

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
     */
    Pair<Key, Key> getKeysFromElement(final Element element);

    /**
     * Converts an {@link uk.gov.gchq.gaffer.data.element.Edge} to a pair of
     * {@link org.apache.accumulo.core.data.Key}s.
     *
     * @param edge The edge to be converted
     * @return The key(s) that represent the given edge
     */
    Pair<Key, Key> getKeysFromEdge(final Edge edge);

    /**
     * Converts an {@link uk.gov.gchq.gaffer.data.element.Entity} to a
     * {@link org.apache.accumulo.core.data.Key}.
     *
     * @param entity the entity to be converted
     * @return The key(s) that represent the given entity
     */
    Key getKeyFromEntity(final Entity entity);

    /**
     * Converts a set of {@link uk.gov.gchq.gaffer.data.element.Properties} to an Accumulo
     * {@link org.apache.accumulo.core.data.Value}.
     *
     * @param group      the element group
     * @param properties the properties to use to create a Value
     * @return A new Accumulo {@link Value} containing the serialised {@link uk.gov.gchq.gaffer.data.element.Properties}
     */
    Value getValueFromProperties(final String group, final Properties properties);

    /**
     * Converts the {@link uk.gov.gchq.gaffer.data.element.Properties} in an element to an
     * Accumulo {@link Value} where the property has a position within the store
     * schema that indicates it should be stored in the value.
     *
     * @param element the element to be converted
     * @return An Accumulo {@link Value} representing the
     * {@link uk.gov.gchq.gaffer.data.element.Properties} that should be stored in the value.
     */
    Value getValueFromElement(final Element element);

    /**
     * Converts an Accumulo {@link org.apache.accumulo.core.data.Value} to a
     * {@link uk.gov.gchq.gaffer.data.element.Properties} object.
     *
     * @param group the element group
     * @param value the Value containing the serialised properties
     * @return A set of {@link uk.gov.gchq.gaffer.data.element.Properties} that represent
     * the property stored within the {@link Value}
     */
    Properties getPropertiesFromValue(final String group, final Value value);

    /**
     * Gets a new {@link ElementId} from an Accumulo {@link Key}.
     *
     * @param key                  the Key containing serialised parts of the Element
     * @param includeMatchedVertex if true then the matchedVertex field is set on Edges
     * @return A new {@link ElementId}
     */
    ElementId getElementId(final Key key, final boolean includeMatchedVertex);

    /**
     * Gets a new {@link Element} from an Accumulo {@link Key}.
     *
     * @param key                  the Key containing serialised parts of the Element
     * @param includeMatchedVertex if true then the matchedVertex field is set on Edges
     * @return A new {@link Element} including a partial set of
     * {@link uk.gov.gchq.gaffer.data.element.Properties} that were store in the {@link Key}
     */
    Element getElementFromKey(final Key key, final boolean includeMatchedVertex);

    /**
     * Returns an {@link Element} populated with all the properties defined
     * within the {@link Key} and {@link Value}.
     *
     * @param key                  the accumulo Key containing serialised parts of the Element
     * @param value                the accumulo Value containing serialised properties of the Element
     * @param includeMatchedVertex if true then the matchedVertex field is set on Edges
     * @return Returns an {@link Element} populated with all the properties defined within the {@link Key}
     * and {@link Value}
     */
    Element getFullElement(final Key key, final Value value, final boolean includeMatchedVertex);

    /**
     * Helper Used to create Bloom Filters, method Serialises a given object
     * (from an {@link uk.gov.gchq.gaffer.data.element.id.EntityId} ) with the Identifier
     * Serialiser defined in the schema.
     *
     * @param vertex the vertex identifier to serialise
     * @return A byte array representing the given object
     */
    byte[] serialiseVertex(final Object vertex);

    /**
     * Creates a byte array representing a set of
     * {@link uk.gov.gchq.gaffer.data.element.Properties} that are to be stored in the
     * column qualifier.
     *
     * @param group      the element group
     * @param properties the element properties
     * @return A byte array representing the provided {@link uk.gov.gchq.gaffer.data.element.Properties} that are marked as to be stored in the provided
     * position in the {@link uk.gov.gchq.gaffer.accumulostore} schema.
     */
    byte[] buildColumnQualifier(final String group, final Properties properties);

    /**
     * Returns a set of {@link uk.gov.gchq.gaffer.data.element.Properties} that are stored
     * in the part of the key that is provided.
     *
     * @param group           the element group
     * @param columnQualifier the element column qualifier properties serialised into bytes
     * @return The Properties stored within the part of the {@link Key} specified e.g Column Qualifier
     */
    Properties getPropertiesFromColumnQualifier(final String group, final byte[] columnQualifier);

    /**
     * Truncates the provided columnQualifier, returning the byte representation of the serialised version of the specified
     * properties.
     *
     * @param group    the element group
     * @param bytes    the full list of property bytes
     * @param numProps the number of properties to extract
     * @return details of the bytes range.
     */
    BytesAndRange getPropertiesAsBytesFromColumnQualifier(final String group, final byte[] bytes, final int numProps);

    /**
     * Creates a byte array representing the group.
     *
     * @param group the element group
     * @return A byte array representing the group
     */
    byte[] buildColumnFamily(final String group);

    /**
     * Returns the element class from the given bytes.
     *
     * @param columnFamily the column family bytes
     * @return The element class
     */
    String getGroupFromColumnFamily(final byte[] columnFamily);

    /**
     * Creates a byte array representing a set of
     * {@link uk.gov.gchq.gaffer.data.element.Properties} that are to be stored in the
     * column visibility.
     *
     * @param group      the element group
     * @param properties the element properties
     * @return A byte array representing the provided {@link uk.gov.gchq.gaffer.data.element.Properties} that are marked as to be stored in the provided
     * position in the schema.
     */
    byte[] buildColumnVisibility(final String group, final Properties properties);

    /**
     * Returns a set of {@link uk.gov.gchq.gaffer.data.element.Properties} that are stored
     * in the part of the key that is provided.
     *
     * @param group            the element group
     * @param columnVisibility the element visibility property serialised into bytes
     * @return The Properties stored within the part of the {@link Key} specified e.g Column Qualifier
     */
    Properties getPropertiesFromColumnVisibility(final String group, final byte[] columnVisibility);

    /**
     * Creates a timestamp based on the provided {@link Properties}
     * will return a default value if properties do not have a valid timestamp value
     *
     * @param properties the element properties
     * @return the timestamp
     */
    long buildTimestamp(final Properties properties);


    /**
     * Creates a properties object based on the provided timestamp and group.
     * If the group contains a timestamp property then this property is populated
     * in the returned object. Otherwise, the result is an empty properties object.
     *
     * @param group     the group of the element - used to determine if the result
     *                  should contain the timestamp.
     * @param timestamp the timestamp
     * @return a properties object populated with the provided timestamp.
     */
    Properties getPropertiesFromTimestamp(final String group, final long timestamp);

}
