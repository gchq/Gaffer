/*
 * Copyright 2022 Crown Copyright
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
package uk.gov.gchq.gaffer.federatedstore;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;

public class MatchedVertexInconsistencyTest {

    public static final String TSTV = TypeSubTypeValue.class.getSimpleName();
    public static final String A = "a";
    public static final String AA = "aa";
    public static final String AAA = "aaa";
    public static final String B = "b";
    public static final String BB = "bb";
    public static final String BBB = "bbb";
    public static final Context CONTEXT = new Context(new User("user"));
    public static final TypeSubTypeValue SOURCE = new TypeSubTypeValue(A, AA, AAA);
    public static final TypeSubTypeValue DEST = new TypeSubTypeValue(B, BB, BBB);
    public static final String TEST_EDGE = "testEdge";
    public static final String TEST_ENTITY = "testEntity";
    public static final String EXPECTED = "{\n" +
            "  \"class\" : \"uk.gov.gchq.gaffer.data.element.Edge\",\n" +
            "  \"group\" : \"testEdge\",\n" +
            "  \"source\" : {\n" +
            "    \"uk.gov.gchq.gaffer.types.TypeSubTypeValue\" : {\n" +
            "      \"type\" : \"a\",\n" +
            "      \"subType\" : \"aa\",\n" +
            "      \"value\" : \"aaa\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"destination\" : {\n" +
            "    \"uk.gov.gchq.gaffer.types.TypeSubTypeValue\" : {\n" +
            "      \"type\" : \"b\",\n" +
            "      \"subType\" : \"bb\",\n" +
            "      \"value\" : \"bbb\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"directed\" : false,\n" +
            "  \"matchedVertex\" : \"SOURCE\",\n" +
            "  \"properties\" : { }\n" +
            "}";
    public static final Edge EDGE = new Edge.Builder()
            .group(TEST_EDGE)
            .source(SOURCE)
            .dest(DEST)
            .build();

    @Test
    public void testGetAllElementsFromMap() throws Exception {
        // Given
        Graph graph = getMapStoreGraph();
        addElement(graph);

        // When
        Iterable<? extends Element> results = graph.execute(new GetAllElements.Builder()
                .build(), CONTEXT);
        final List list = Streams.toStream(results).collect(Collectors.toList());

        // Then
        assertThat(list).hasSize(1).containsExactly(EDGE);
        assertThat(new String(JSONSerialiser.serialise(list.get(0), true))).isEqualTo(EXPECTED);
    }

    @Test
    public void testGetAllElementsFromAccumulo() throws Exception {
        // Given
        Graph graph = getAccumuloStoreGraph();
        addElement(graph);

        // When
        Iterable<? extends Element> results = graph.execute(new GetAllElements.Builder()
                .build(), CONTEXT);
        final List list = Streams.toStream(results).collect(Collectors.toList());

        // Then
        // Fail: Missing matchedVertex
        assertThat(list).hasSize(1).containsExactly(EDGE);
        assertThat(new String(JSONSerialiser.serialise(list.get(0), true))).isEqualTo(EXPECTED);
    }

    private Graph getMapStoreGraph() {
        return new Graph.Builder()
                .config(new GraphConfig.Builder().graphId("store1").build())
                .addSchema(getSchema())
                .storeProperties(new MapStoreProperties())
                .build();
    }

    private Graph getAccumuloStoreGraph() {
        final AccumuloProperties props = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);
        return new Graph.Builder()
                .config(new GraphConfig.Builder().graphId("store1").build())
                .addSchema(getSchema())
                .storeProperties(props)
                .build();
    }

    private Schema getSchema() {
        return new Schema.Builder()
                .edge(TEST_EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TSTV)
                        .destination(TSTV)
                        .build())
                .entity(TEST_ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TSTV)
                        .build())
                .type(TSTV, TypeSubTypeValue.class)
                .build();
    }

    private void addElement(final Graph store) throws OperationException {
        store.execute(new AddElements.Builder()
                .input(EDGE)
                .build(), CONTEXT);
    }

}
