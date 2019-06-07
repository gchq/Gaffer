/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.key.impl.byteEntity;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityRangeElementPropertyFilterIterator;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ByteEntityRangeElementPropertyFilterIteratorTest {
    private static final Schema SCHEMA = new Schema.Builder()
            .type("string", String.class)
            .type("type", Boolean.class)
            .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                    .source("string")
                    .destination("string")
                    .directed("true")
                    .build())
            .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                    .vertex("string")
                    .build())
            .vertexSerialiser(new StringSerialiser())
            .build();

    private static final List<Element> ELEMENTS = Arrays.asList(
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("vertexA")
                    .dest("vertexB")
                    .directed(true)
                    .build(),

            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("vertexD")
                    .dest("vertexC")
                    .directed(true)
                    .build(),

            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("vertexE")
                    .dest("vertexE")
                    .directed(true)
                    .build(),

            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("vertexF")
                    .dest("vertexG")
                    .directed(false)
                    .build(),
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("vertexH")
                    .dest("vertexH")
                    .directed(false)
                    .build(),
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("vertexI")
                    .build());

    private final ByteEntityAccumuloElementConverter converter = new ByteEntityAccumuloElementConverter(SCHEMA);

    @Test
    public void shouldOnlyAcceptDeduplicatedEdges() throws IOException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {
            {
                put(AccumuloStoreConstants.OUTGOING_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.DEDUPLICATE_UNDIRECTED_EDGES, "true");
                put(AccumuloStoreConstants.INCLUDE_EDGES, "true");
            }
        };
        filter.init(null, options, null);

        final Value value = null; // value should not be used

        // When / Then
        for (final Element element : ELEMENTS) {
            final Pair<Key, Key> keys = converter.getKeysFromElement(element);
            // First key is deduplicated, but only edges should be excepted
            assertEquals("Failed for element: " + element.toString(), element instanceof Edge, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self elements are not added the other way round
                assertEquals("Failed for element: " + element.toString(), false, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptDeduplicatedDirectedEdges() throws IOException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {
            {
                put(AccumuloStoreConstants.DEDUPLICATE_UNDIRECTED_EDGES, "true");
                put(AccumuloStoreConstants.DIRECTED_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.OUTGOING_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.INCLUDE_EDGES, "true");
            }
        };
        filter.init(null, options, null);

        final Value value = null; // value should not be used

        // When / Then
        for (final Element element : ELEMENTS) {
            final Pair<Key, Key> keys = converter.getKeysFromElement(element);
            // First key is deduplicated, but only directed edges should be excepted
            final boolean expectedResult = element instanceof Edge && ((Edge) element).isDirected();
            assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self elements are not added the other way round
                assertEquals("Failed for element: " + element.toString(), false, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptDeduplicatedUndirectedEdges() throws IOException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {
            {
                put(AccumuloStoreConstants.DEDUPLICATE_UNDIRECTED_EDGES, "true");
                put(AccumuloStoreConstants.UNDIRECTED_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.OUTGOING_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.INCLUDE_EDGES, "true");
            }
        };
        filter.init(null, options, null);

        final Value value = null; // value should not be used

        // When / Then
        for (final Element element : ELEMENTS) {
            final Pair<Key, Key> keys = converter.getKeysFromElement(element);
            // First key is deduplicated, but only undirected edges should be excepted
            final boolean expectedResult = element instanceof Edge && !((Edge) element).isDirected();
            assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self elements are not added the other way round
                assertEquals("Failed for element: " + element.toString(), false, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptDirectedEdges() throws IOException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {
            {
                put(AccumuloStoreConstants.DIRECTED_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.INCLUDE_EDGES, "true");
            }
        };
        filter.init(null, options, null);

        final Value value = null; // value should not be used

        // When / Then
        for (final Element element : ELEMENTS) {
            final boolean expectedResult = element instanceof Edge && ((Edge) element).isDirected();
            final Pair<Key, Key> keys = converter.getKeysFromElement(element);
            assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self elements are not added the other way round
                assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptUndirectedEdges() throws IOException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {
            {
                put(AccumuloStoreConstants.UNDIRECTED_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.OUTGOING_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.INCLUDE_EDGES, "true");
            }
        };
        filter.init(null, options, null);

        final Value value = null; // value should not be used

        // When / Then
        for (final Element element : ELEMENTS) {
            final boolean expectedResult = element instanceof Edge && !((Edge) element).isDirected();
            final Pair<Key, Key> keys = converter.getKeysFromElement(element);
            assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self elements are not added the other way round
                assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptIncomingEdges() throws IOException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {
            {
                put(AccumuloStoreConstants.DIRECTED_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.INCOMING_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.INCLUDE_EDGES, "true");
            }
        };
        filter.init(null, options, null);

        final Value value = null; // value should not be used

        // When / Then
        for (final Element element : ELEMENTS) {
            final Pair<Key, Key> keys = converter.getKeysFromElement(element);
            assertEquals("Failed for element: " + element.toString(), false, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self elements are not added the other way round
                final boolean expectedResult = element instanceof Edge && ((Edge) element).isDirected();
                assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptOutgoingEdges() throws IOException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {
            {
                put(AccumuloStoreConstants.DIRECTED_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.OUTGOING_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.INCLUDE_EDGES, "true");
            }
        };
        filter.init(null, options, null);

        final Value value = null; // value should not be used

        // When / Then
        for (final Element element : ELEMENTS) {
            final Pair<Key, Key> keys = converter.getKeysFromElement(element);
            final boolean expectedResult = element instanceof Edge && ((Edge) element).isDirected();
            assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self elements are not added the other way round
                assertEquals("Failed for element: " + element.toString(), false, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldAcceptOnlyEntities() throws IOException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {
            {
                put(AccumuloStoreConstants.INCLUDE_ENTITIES, "true");
                put(AccumuloStoreConstants.OUTGOING_EDGE_ONLY, "true");
            }
        };
        filter.init(null, options, null);

        final Value value = null; // value should not be used

        // When / Then
        for (final Element element : ELEMENTS) {
            final boolean expectedResult = element instanceof Entity;
            final Pair<Key, Key> keys = converter.getKeysFromElement(element);
            assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // entities and self edges are not added the other way round
                assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getSecond(), value));
            }
        }
    }
}
