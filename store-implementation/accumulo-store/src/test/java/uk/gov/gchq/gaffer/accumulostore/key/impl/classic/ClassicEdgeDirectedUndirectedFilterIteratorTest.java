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

package uk.gov.gchq.gaffer.accumulostore.key.impl.classic;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic.ClassicAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic.ClassicEdgeDirectedUndirectedFilterIterator;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClassicEdgeDirectedUndirectedFilterIteratorTest {
    private static final Schema SCHEMA = new Schema.Builder()
            .type("string", String.class)
            .type("true", Boolean.class)
            .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                    .source("string")
                    .destination("string")
                    .directed("true")
                    .build())
            .vertexSerialiser(new StringSerialiser())
            .build();

    private static final List<Edge> EDGES = Arrays.asList(
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
                    .build()
    );

    private final ClassicAccumuloElementConverter converter = new ClassicAccumuloElementConverter(SCHEMA);

    @Test
    public void shouldOnlyAcceptDeduplicatedEdges() throws IOException {
        // Given
        final ClassicEdgeDirectedUndirectedFilterIterator filter = new ClassicEdgeDirectedUndirectedFilterIterator();
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
        for (final Edge edge : EDGES) {
            final Pair<Key, Key> keys = converter.getKeysFromEdge(edge);
            // First key is deduplicated
            assertTrue("Failed for edge: " + edge.toString(), filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self edges are not added the other way round
                assertFalse("Failed for edge: " + edge.toString(), filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptDeduplicatedDirectedEdges() throws IOException {
        // Given
        final ClassicEdgeDirectedUndirectedFilterIterator filter = new ClassicEdgeDirectedUndirectedFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {
            {
                put(AccumuloStoreConstants.OUTGOING_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.DIRECTED_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.INCLUDE_EDGES, "true");
            }
        };
        filter.init(null, options, null);

        final Value value = null; // value should not be used

        // When / Then
        for (final Edge edge : EDGES) {
            final Pair<Key, Key> keys = converter.getKeysFromEdge(edge);
            // First key is deduplicated
            assertEquals("Failed for edge: " + edge.toString(), edge.isDirected(), filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self edges are not added the other way round
                assertFalse("Failed for edge: " + edge.toString(), filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptDeduplicatedUndirectedEdges() throws IOException {
        // Given
        final ClassicEdgeDirectedUndirectedFilterIterator filter = new ClassicEdgeDirectedUndirectedFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {
            {
                put(AccumuloStoreConstants.DEDUPLICATE_UNDIRECTED_EDGES, "true");
                put(AccumuloStoreConstants.UNDIRECTED_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.INCLUDE_EDGES, "true");
            }
        };
        filter.init(null, options, null);

        final Value value = null; // value should not be used

        // When / Then
        for (final Edge edge : EDGES) {
            final Pair<Key, Key> keys = converter.getKeysFromEdge(edge);
            // First key is deduplicated
            assertEquals("Failed for edge: " + edge.toString(), !edge.isDirected(), filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self edges are not added the other way round
                assertFalse("Failed for edge: " + edge.toString(), filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptDirectedEdges() throws IOException {
        // Given
        final ClassicEdgeDirectedUndirectedFilterIterator filter = new ClassicEdgeDirectedUndirectedFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {
            {
                put(AccumuloStoreConstants.DIRECTED_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.INCLUDE_EDGES, "true");
            }
        };
        filter.init(null, options, null);

        final Value value = null; // value should not be used

        // When / Then
        for (final Edge edge : EDGES) {
            final boolean expectedResult = edge.isDirected();
            final Pair<Key, Key> keys = converter.getKeysFromEdge(edge);
            assertEquals("Failed for edge: " + edge.toString(), expectedResult, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self edges are not added the other way round
                assertEquals("Failed for edge: " + edge.toString(), expectedResult, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptUndirectedEdges() throws IOException {
        // Given
        final ClassicEdgeDirectedUndirectedFilterIterator filter = new ClassicEdgeDirectedUndirectedFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {
            {
                put(AccumuloStoreConstants.UNDIRECTED_EDGE_ONLY, "true");
                put(AccumuloStoreConstants.INCLUDE_EDGES, "true");
            }
        };
        filter.init(null, options, null);

        final Value value = null; // value should not be used

        // When / Then
        for (final Edge edge : EDGES) {
            final boolean expectedResult = !edge.isDirected();
            final Pair<Key, Key> keys = converter.getKeysFromEdge(edge);
            assertEquals("Failed for edge: " + edge.toString(), expectedResult, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self edges are not added the other way round
                assertEquals("Failed for edge: " + edge.toString(), expectedResult, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptIncomingEdges() throws IOException {
        // Given
        final ClassicEdgeDirectedUndirectedFilterIterator filter = new ClassicEdgeDirectedUndirectedFilterIterator();
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
        for (final Edge edge : EDGES) {
            final Pair<Key, Key> keys = converter.getKeysFromEdge(edge);
            assertEquals("Failed for edge: " + edge.toString(), false, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self edges are not added the other way round
                final boolean expectedResult = edge.isDirected();
                assertEquals("Failed for edge: " + edge.toString(), expectedResult, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptOutgoingEdges() throws IOException {
        // Given
        final ClassicEdgeDirectedUndirectedFilterIterator filter = new ClassicEdgeDirectedUndirectedFilterIterator();
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
        for (final Edge edge : EDGES) {
            final Pair<Key, Key> keys = converter.getKeysFromEdge(edge);
            final boolean expectedResult = edge.isDirected();
            assertEquals("Failed for edge: " + edge.toString(), expectedResult, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self edges are not added the other way round
                assertEquals("Failed for edge: " + edge.toString(), false, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldThrowExceptionWhenValidateOptionsWithDirectedAndUndirectedEdgeFlags() {
        // Given
        final ClassicEdgeDirectedUndirectedFilterIterator filter = new ClassicEdgeDirectedUndirectedFilterIterator();
        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.DIRECTED_EDGE_ONLY, "true");
        options.put(AccumuloStoreConstants.UNDIRECTED_EDGE_ONLY, "true");

        // When / Then
        try {
            filter.validateOptions(options);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenValidateOptionsWithInAndOutEdgeFlags() throws OperationException, IOException {
        // Given
        final ClassicEdgeDirectedUndirectedFilterIterator filter = new ClassicEdgeDirectedUndirectedFilterIterator();
        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.INCOMING_EDGE_ONLY, "true");
        options.put(AccumuloStoreConstants.OUTGOING_EDGE_ONLY, "true");

        // When / Then
        try {
            filter.validateOptions(options);

            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldValidateOptionsSuccessfully() throws OperationException, IOException {
        // Given
        final ClassicEdgeDirectedUndirectedFilterIterator filter = new ClassicEdgeDirectedUndirectedFilterIterator();
        final Map<String, String> options = new HashMap<>();

        // When
        final boolean result = filter.validateOptions(options);

        // Then
        assertTrue(result);
    }
}

