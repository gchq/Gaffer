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
import gaffer.exception.SerialisationException;
import gaffer.graph.Graph;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.OperationException;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.user.User;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;
import java.util.List;

public class LoadAndQuery5Test {
    private static final String RESOURCE_PREFIX = "/example/gettingstarted/";
    private static final String RESOURCE_EXAMPLE_PREFIX = RESOURCE_PREFIX + "5/";
    private static final String GROUP = "red";
    private static final String COUNT = "count";
    public static final String VISIBILITY = "visibility";

    @Test
    public void shouldReturnExpectedEdges() throws OperationException {
        // Given
        final LoadAndQuery5 query = new LoadAndQuery5();

        // When
        final Iterable<Edge> results = query.run();

        // Then
        verifyResults(results);
    }


    @Test
    public void shouldReturnExpectedEdgesViaJson() throws OperationException, SerialisationException {
        // Given
        final User basicUser = new User("basicUser");
        final User privateUser = new User.Builder()
                .userId("privateUser")
                .dataAuth("private")
                .build();
        final JSONSerialiser serialiser = new JSONSerialiser();
        final AddElements addElements = serialiser.deserialise(StreamUtil.openStream(LoadAndQuery.class, RESOURCE_EXAMPLE_PREFIX + "json/load.json"), AddElements.class);
        final GetRelatedEdges<?> getRelatedEdges = serialiser.deserialise(StreamUtil.openStream(LoadAndQuery.class, RESOURCE_EXAMPLE_PREFIX + "json/query.json"), GetRelatedEdges.class);

        // Setup graph
        final Graph graph = new Graph.Builder()
                .storeProperties(StreamUtil.openStream(LoadAndQuery.class, RESOURCE_PREFIX + "mockaccumulostore.properties"))
                .addSchemas(StreamUtil.openStreams(LoadAndQuery.class, RESOURCE_EXAMPLE_PREFIX + "schema"))
                .build();

        // When
        graph.execute(addElements, basicUser); // Execute the add operation chain on the graph
        final Iterable<Edge> results = graph.execute(getRelatedEdges, privateUser); // Execute the query operation on the graph.

        // Then
        verifyResults(results);
    }

    private void verifyResults(final Iterable<Edge> resultsItr) {
        final Edge[] expectedResults = {
                new Edge.Builder()
                        .group(GROUP)
                        .source("1")
                        .dest("2")
                        .directed(false)
                        .property(VISIBILITY, "private")
                        .property(COUNT, 4)
                        .build(),
                new Edge.Builder()
                        .group(GROUP)
                        .source("1")
                        .dest("3")
                        .directed(false)
                        .property(VISIBILITY, "private")
                        .property(COUNT, 1)
                        .build(),
                new Edge.Builder()
                        .group(GROUP)
                        .source("1")
                        .dest("4")
                        .directed(false)
                        .property(VISIBILITY, "public")
                        .property(COUNT, 2)
                        .build()
        };

        final List<Edge> results = Lists.newArrayList(resultsItr);
        assertEquals(expectedResults.length, results.size());
        assertThat(results, IsCollectionContaining.hasItems(expectedResults));
    }

}