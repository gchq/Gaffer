/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.mapstore;

import com.google.common.collect.Lists;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class MapStoreTest {
    @Test
    public void testTraits() throws StoreException {
        final MapStore mapStore = new MapStore();
        mapStore.initialise(new Schema(), new MapStoreProperties());
        final Set<StoreTrait> expectedTraits = new HashSet<>(Arrays.asList(
                StoreTrait.STORE_AGGREGATION,
                StoreTrait.PRE_AGGREGATION_FILTERING,
                StoreTrait.POST_AGGREGATION_FILTERING,
                StoreTrait.TRANSFORMATION,
                StoreTrait.POST_TRANSFORMATION_FILTERING));
        assertEquals(expectedTraits, mapStore.getTraits());
    }

    @Test
    public void test() throws StoreException, OperationException {
        //TODO fix this test - currently throws a null pointer.
        final Graph graph = new Graph.Builder()
                .addSchemas(StreamUtil.openStreams(getClass(), "example-schema"))
                .storeProperties(StreamUtil.storeProps(getClass()))
                .build();

        graph.execute(new AddElements.Builder()
                .elements(new Entity.Builder()
                                .vertex("vertex1")
                                .group("entity")
                                .property("count", 1)
                                .build(),
                        new Entity.Builder()
                                .vertex("vertex2")
                                .group("entity")
                                .property("count", 2)
                                .build(),
                        new Edge.Builder()
                                .source("vertex1")
                                .dest("vertex2")
                                .directed(true)
                                .group("edge")
                                .property("count", 1)
                                .build())
                .build(), new User());

        final ArrayList<Edge> results = Lists.newArrayList(graph.execute(new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds.Builder()
                        .addSeed(new EntitySeed("vertex1"))
                        .build())
                .then(new GetEdges<>())
                .build(), new User()));

        System.out.println(results);

    }
}
