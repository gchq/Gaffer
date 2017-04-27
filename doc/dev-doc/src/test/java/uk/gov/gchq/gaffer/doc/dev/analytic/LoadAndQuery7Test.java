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

package uk.gov.gchq.gaffer.doc.dev.analytic;

import com.google.common.collect.Lists;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.doc.dev.walkthrough.Subgraphs;
import uk.gov.gchq.gaffer.doc.walkthrough.AbstractWalkthrough;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.user.User;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class LoadAndQuery7Test {
    private static final String RESOURCE_EXAMPLE_PREFIX = "7/";
    public static final String COUNT = "count";
    public static final String EDGE_GROUP = "data";

    @Test
    public void shouldReturnExpectedEdges() throws OperationException {
        // Given
        final Subgraphs query = new Subgraphs();

        // When
        final Iterable<? extends Element> results = query.run();

        // Then
        verifyResults(results);
    }

    @Test
    public void shouldReturnExpectedStringsViaJson() throws OperationException, SerialisationException {
        // Given
        final User user01 = new User("user01");
        final JSONSerialiser serialiser = new JSONSerialiser();
        final OperationChain<?> addOpChain = serialiser.deserialise(StreamUtil.openStream(AbstractWalkthrough.class, RESOURCE_EXAMPLE_PREFIX + "json/load.json"), OperationChain.class);
        final OperationChain<CloseableIterable<Element>> queryOpChain = serialiser.deserialise(StreamUtil.openStream(AbstractWalkthrough.class, RESOURCE_EXAMPLE_PREFIX + "json/query.json"), OperationChain.class);

        // Setup graph
        final Graph graph = new Graph.Builder()
                .storeProperties(StreamUtil.openStream(AbstractWalkthrough.class, "mockaccumulostore.properties"))
                .addSchemas(StreamUtil.openStreams(AbstractWalkthrough.class, RESOURCE_EXAMPLE_PREFIX + "schema"))
                .build();

        // When
        graph.execute(addOpChain, user01); // Execute the add operation chain on the graph
        final CloseableIterable<Element> results = graph.execute(queryOpChain, user01); // Execute the query operation on the graph.

        // Then
        verifyResults(results);
    }

    private void verifyResults(final Iterable<? extends Element> resultsItr) {
        final Edge[] expectedResults = {
                new Edge.Builder()
                        .source("RoadUse")
                        .dest("2")
                        .directed(true)
                        .group(EDGE_GROUP)
                        .property(COUNT, 3)
                        .build(),
                new Edge.Builder()
                        .source("RoadUse")
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