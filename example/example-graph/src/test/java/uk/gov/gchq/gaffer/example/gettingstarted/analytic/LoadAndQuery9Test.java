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

package uk.gov.gchq.gaffer.example.gettingstarted.analytic;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.collect.Iterables;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEntities;
import uk.gov.gchq.gaffer.user.User;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LoadAndQuery9Test {
    private static final String RESOURCE_PREFIX = "/example/gettingstarted/";
    private static final String RESOURCE_EXAMPLE_PREFIX = RESOURCE_PREFIX + "9/";
    private static final String CARDINALITY = "Cardinality";
    private static final String COUNT = "count";
    public static final String HLLP = "hllp";

    @Test
    public void shouldReturnExpectedEdges() throws OperationException {
        // Given
        final LoadAndQuery9 query = new LoadAndQuery9();

        // When
        final Iterable<Entity> results = query.run();

        // Then
        verifyResults(results);
    }

    @Test
    public void shouldReturnExpectedEdgesViaJson() throws OperationException, SerialisationException {
        // Given
        final User user = new User("user01");
        final JSONSerialiser serialiser = new JSONSerialiser();
        final OperationChain<?> addOpChain = serialiser.deserialise(StreamUtil.openStream(LoadAndQuery.class, RESOURCE_EXAMPLE_PREFIX + "json/load.json"), OperationChain.class);
        final GetAllEntities getRelatedEdges = serialiser.deserialise(StreamUtil.openStream(LoadAndQuery.class, RESOURCE_EXAMPLE_PREFIX + "json/query.json"), GetAllEntities.class);

        // Setup graph
        final Graph graph = new Graph.Builder()
                .storeProperties(StreamUtil.openStream(LoadAndQuery.class, RESOURCE_PREFIX + "mockaccumulostore.properties"))
                .addSchemas(StreamUtil.openStreams(LoadAndQuery.class, RESOURCE_EXAMPLE_PREFIX + "schema"))
                .build();

        // When
        graph.execute(addOpChain, user); // Execute the add operation chain on the graph
        final Iterable<Entity> results = graph.execute(getRelatedEdges, user); // Execute the query operation on the graph.

        // Then
        verifyResults(results);
    }

    private void verifyResults(final Iterable<Entity> resultsItr) {
        final Map<String, Entity> expectedResults = new HashMap<>();
        expectedResults.put("1", new Entity.Builder()
                .group(CARDINALITY)
                .vertex("1")
                .property(COUNT, 8)
                .property("edgeGroup", CollectionUtil.treeSet(new String[]{"blue", "red"}))
                .property(HLLP, 4L)
                .build());
        expectedResults.put("2", new Entity.Builder()
                .group(CARDINALITY)
                .vertex("2")
                .property(COUNT, 6)
                .property("edgeGroup", CollectionUtil.treeSet(new String[]{"blue", "red"}))
                .property(HLLP, 3L)
                .build());
        expectedResults.put("3", new Entity.Builder()
                .group(CARDINALITY)
                .vertex("3")
                .property(COUNT, 3)
                .property("edgeGroup", CollectionUtil.treeSet("red"))
                .property(HLLP, 2L)
                .build());
        expectedResults.put("4", new Entity.Builder()
                .group(CARDINALITY)
                .vertex("4")
                .property(COUNT, 1)
                .property("edgeGroup", CollectionUtil.treeSet("red"))
                .property(HLLP, 1L)
                .build());
        expectedResults.put("5", new Entity.Builder()
                .group(CARDINALITY)
                .vertex("5")
                .property(COUNT, 1)
                .property("edgeGroup", CollectionUtil.treeSet("blue"))
                .property(HLLP, 1L)
                .build());
        expectedResults.put("6", new Entity.Builder()
                .group(CARDINALITY)
                .vertex("6")
                .property(COUNT, 1)
                .property("edgeGroup", CollectionUtil.treeSet("blue"))
                .property(HLLP, 1L)
                .build());

        assertEquals(expectedResults.size(), Iterables.size(resultsItr));
        for (Entity entity : resultsItr) {
            System.out.println(entity);
            // HyperLogLogPlus has not overridden the equals method so this is a work around to check the cardinality values are the same.
            entity.putProperty(HLLP, ((HyperLogLogPlus) entity.getProperty(HLLP)).cardinality());
            assertEquals(expectedResults.get(entity.getVertex().toString()), entity);
        }
    }
}