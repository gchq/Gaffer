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

package gaffer.example.gettingstarted.analytic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import gaffer.commonutil.StreamUtil;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.exception.SerialisationException;
import gaffer.graph.Graph;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.user.User;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;
import java.util.List;

public class LoadAndQuery7Test {
    private static final String RESOURCE_PREFIX = "/example/gettingstarted/";
    private static final String RESOURCE_EXAMPLE_PREFIX = RESOURCE_PREFIX + "7/";
    public static final String COUNT = "count";
    public static final String ENTITY_GROUP = "entity";
    public static final String EDGE_GROUP = "edge";

    @Test
    public void shouldReturnExpectedEdges() throws OperationException {
        // Given
        final LoadAndQuery7 query = new LoadAndQuery7();

        // When
        final Iterable<Element> results = query.run();

        // Then
        verifyResults(results);
    }

    @Test
    public void shouldReturnExpectedStringsViaJson() throws OperationException, SerialisationException {
        // Given
        final User user01 = new User("user01");
        final JSONSerialiser serialiser = new JSONSerialiser();
        final OperationChain<?> addOpChain = serialiser.deserialise(StreamUtil.openStream(LoadAndQuery.class, RESOURCE_EXAMPLE_PREFIX + "json/load.json"), OperationChain.class);
        final OperationChain<Iterable<Element>> queryOpChain = serialiser.deserialise(StreamUtil.openStream(LoadAndQuery.class, RESOURCE_EXAMPLE_PREFIX + "json/query.json"), OperationChain.class);

        // Setup graph
        final Graph graph = new Graph.Builder()
                .storeProperties(StreamUtil.openStream(LoadAndQuery.class, RESOURCE_PREFIX + "mockaccumulostore.properties"))
                .addSchemas(StreamUtil.openStreams(LoadAndQuery.class, RESOURCE_EXAMPLE_PREFIX + "schema"))
                .build();

        // When
        graph.execute(addOpChain, user01); // Execute the add operation chain on the graph
        final Iterable<Element> results = graph.execute(queryOpChain, user01); // Execute the query operation on the graph.

        // Then
        verifyResults(results);
    }

    private void verifyResults(final Iterable<Element> resultsItr) {
        final Element[] expectedResults = {
                new Entity.Builder()
                        .vertex("1")
                        .group(ENTITY_GROUP)
                        .property(COUNT, 3)
                        .build(),
                new Entity.Builder()
                        .vertex("2")
                        .group(ENTITY_GROUP)
                        .property(COUNT, 1)
                        .build(),
                new Entity.Builder()
                        .vertex("3")
                        .group(ENTITY_GROUP)
                        .property(COUNT, 2)
                        .build(),
                new Edge.Builder()
                        .source("1")
                        .dest("2")
                        .directed(true)
                        .group(EDGE_GROUP)
                        .property(COUNT, 3)
                        .build(),
                new Edge.Builder()
                        .source("2")
                        .dest("3")
                        .directed(true)
                        .group(EDGE_GROUP)
                        .property(COUNT, 2)
                        .build()
        };

        final List<Element> results = Lists.newArrayList(resultsItr);
        assertEquals(expectedResults.length, results.size());
        assertThat(results, IsCollectionContaining.hasItems(expectedResults));
    }
}