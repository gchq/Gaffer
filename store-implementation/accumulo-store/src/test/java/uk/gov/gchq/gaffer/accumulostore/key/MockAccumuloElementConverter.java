/*
 * Copyright 2016-2018 Crown Copyright
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
import uk.gov.gchq.gaffer.store.schema.Schema;

/**
 * A mock implementation of {@link AccumuloElementConverter} where all method calls
 * are delegated to a mock. The mock is a public static variable that you can set.
 * After using this class please call "cleanUp" to delete the reference to the mock.
 */
public class MockAccumuloElementConverter implements AccumuloElementConverter {
    public static AccumuloElementConverter mock;
    public static Schema schema;

    public MockAccumuloElementConverter(final Schema schema) {
        MockAccumuloElementConverter.schema = schema;
    }

    public static void cleanUp() {
        mock = null;
        schema = null;
    }

    @Override
    public Pair<Key, Key> getKeysFromElement(final Element element) {
        return mock.getKeysFromElement(element);
    }

    @Override
    public Pair<Key, Key> getKeysFromEdge(final Edge edge) {
        return mock.getKeysFromEdge(edge);
    }

    @Override
    public Key getKeyFromEntity(final Entity entity) {
        return mock.getKeyFromEntity(entity);
    }

    @Override
    public Value getValueFromProperties(final String group, final Properties properties) {
        return mock.getValueFromProperties(group, properties);
    }

    @Override
    public Value getValueFromElement(final Element element) {
        return mock.getValueFromElement(element);
    }

    @Override
    public Properties getPropertiesFromValue(final String group, final Value value) {
        return mock.getPropertiesFromValue(group, value);
    }

    @Override
    public ElementId getElementId(final Key key, final boolean includeMatchedVertex) {
        return mock.getElementId(key, includeMatchedVertex);
    }

    @Override
    public Element getElementFromKey(final Key key, final boolean includeMatchedVertex) {
        return mock.getElementFromKey(key, includeMatchedVertex);
    }

    @Override
    public Element getFullElement(final Key key, final Value value, final boolean includeMatchedVertex) {
        return mock.getFullElement(key, value, includeMatchedVertex);
    }

    @Override
    public byte[] serialiseVertex(final Object vertex) {
        return mock.serialiseVertex(vertex);
    }

    @Override
    public Pair<byte[], byte[]> getRowKeysFromElement(final Element element) {
        return mock.getRowKeysFromElement(element);
    }

    @Override
    public byte[] buildColumnQualifier(final String group, final Properties properties) {
        return mock.buildColumnQualifier(group, properties);
    }

    @Override
    public Properties getPropertiesFromColumnQualifier(final String group, final byte[] columnQualifier) {
        return mock.getPropertiesFromColumnQualifier(group, columnQualifier);
    }

    @Override
    public BytesAndRange getPropertiesAsBytesFromColumnQualifier(final String group, final byte[] bytes, final int numProps) {
        return mock.getPropertiesAsBytesFromColumnQualifier(group, bytes, numProps);
    }

    @Override
    public byte[] buildColumnFamily(final String group) {
        return mock.buildColumnFamily(group);
    }

    @Override
    public String getGroupFromColumnFamily(final byte[] columnFamily) {
        return mock.getGroupFromColumnFamily(columnFamily);
    }

    @Override
    public byte[] buildColumnVisibility(final String group, final Properties properties) {
        return mock.buildColumnVisibility(group, properties);
    }

    @Override
    public Properties getPropertiesFromColumnVisibility(final String group, final byte[] columnVisibility) {
        return mock.getPropertiesFromColumnVisibility(group, columnVisibility);
    }

    @Override
    public long buildTimestamp(final String group, final Properties properties) {
        return mock.buildTimestamp(group, properties);
    }

    @Override
    public Properties getPropertiesFromTimestamp(final String group, final long timestamp) {
        return mock.getPropertiesFromTimestamp(group, timestamp);
    }
}
