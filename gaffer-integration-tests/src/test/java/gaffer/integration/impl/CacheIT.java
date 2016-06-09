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
package gaffer.integration.impl;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Edge;
import gaffer.integration.AbstractStoreIT;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationChain.Builder;
import gaffer.operation.OperationException;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.data.generator.EntitySeedExtractor;
import gaffer.operation.impl.cache.FetchCache;
import gaffer.operation.impl.cache.FetchCachedResult;
import gaffer.operation.impl.cache.UpdateCache;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetRelatedEdges;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.Map;

public class CacheIT extends AbstractStoreIT {

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    /**
     * Adds edges dest[X] -> source[X+1]
     *
     * @return map of edges
     */
    @Override
    protected Map<EdgeSeed, Edge> createEdges() {
        final Map<EdgeSeed, Edge> edges = super.createEdges();
        for (int i = 0; i <= 10; i++) {
            final Edge thirdEdge = new Edge(TestGroups.EDGE, DEST_DIR + i, SOURCE_DIR + (i + 1), true);
            thirdEdge.putProperty(TestPropertyNames.INT, 1);
            thirdEdge.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(thirdEdge, edges);
        }

        return edges;
    }

    @Test
    public void shouldCacheResultsInHashMap() throws OperationException, IOException {
        // Given
        final OperationChain<Map<String, Iterable<?>>> cacheOpChain = new Builder()
                .first(new GetRelatedEdges.Builder<EntitySeed>()
                        .addSeed(new EntitySeed(SOURCE_DIR_0))
                        .build())
                .then(new UpdateCache())
                .then(new GenerateObjects.Builder<Edge, EntitySeed>()
                        .generator(new EntitySeedExtractor())
                        .build())
                .then(new GetRelatedEdges<EntitySeed>())
                .then(new UpdateCache())
                .then(new FetchCache())
                .build();

        // When
        final Map<String, Iterable<?>> cache = graph.execute(cacheOpChain, getUser());

        // Then
        assertEquals(2, Iterables.size(cache.get(UpdateCache.ALL)));
    }

    @Test
    public void shouldCacheResultsInHashMapAndFetchResult() throws OperationException, IOException {
        // Given
        final OperationChain<Iterable<?>> fetchOpChain = new OperationChain.Builder()
                .first(new GetRelatedEdges.Builder<EntitySeed>()
                        .addSeed(new EntitySeed(SOURCE_DIR_0))
                        .build())
                .then(new UpdateCache())
                .then(new GenerateObjects.Builder<Edge, EntitySeed>()
                        .generator(new EntitySeedExtractor())
                        .build())
                .then(new GetRelatedEdges<EntitySeed>())
                .then(new UpdateCache())
                .then(new FetchCachedResult())
                .build();

        // When
        final Iterable<?> results = graph.execute(fetchOpChain, getUser());

        // Then
        assertEquals(2, Iterables.size(results));
    }
}