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

package gaffer.accumulostore.key.impl;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.MockAccumuloStoreForTest;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import gaffer.accumulostore.utils.AccumuloPropertyNames;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewEdgeDefinition;
import gaffer.data.elementdefinition.view.ViewEntityDefinition;
import gaffer.operation.OperationException;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.store.StoreException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class AggregatorIteratorTest {

    private static View defaultView;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;

    @BeforeClass
    public static void setup() throws IOException, StoreException {
        byteEntityStore = new MockAccumuloStoreForTest(ByteEntityKeyPackage.class);
        gaffer1KeyStore = new MockAccumuloStoreForTest(ClassicKeyPackage.class);

        byteEntityStore.getProperties().setTable("Test");
        gaffer1KeyStore.getProperties().setTable("Test2");

        defaultView = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).entity(TestGroups.ENTITY, new ViewEntityDefinition()).build();
    }

    @AfterClass
    public static void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
        defaultView = null;
    }

    @Test
    public void test() throws OperationException {
        test(byteEntityStore);
        test(gaffer1KeyStore);
    }

    public void test(final AccumuloStore store) throws OperationException {
        // Given
        final long timestamp = new Date().getTime();
        Edge expectedResult = new Edge(TestGroups.EDGE);
        expectedResult.setSource("1");
        expectedResult.setDestination("2");
        expectedResult.setDirected(true);
        expectedResult.putProperty(AccumuloPropertyNames.COUNT, 13);
        expectedResult.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
        expectedResult.putProperty(AccumuloPropertyNames.TIMESTAMP, timestamp);
        expectedResult.putProperty(AccumuloPropertyNames.F1, 0);
        expectedResult.putProperty(AccumuloPropertyNames.F2, 0);
        expectedResult.putProperty(AccumuloPropertyNames.F3, 1);
        expectedResult.putProperty(AccumuloPropertyNames.F4, 1);

        Edge edge1 = new Edge(TestGroups.EDGE);
        edge1.setSource("1");
        edge1.setDestination("2");
        edge1.setDirected(true);
        edge1.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
        edge1.putProperty(AccumuloPropertyNames.TIMESTAMP, timestamp);
        edge1.putProperty(AccumuloPropertyNames.COUNT, 1);
        edge1.putProperty(AccumuloPropertyNames.F3, 1);

        Edge edge2 = new Edge(TestGroups.EDGE);
        edge2.setSource("1");
        edge2.setDestination("2");
        edge2.setDirected(true);
        edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
        edge2.putProperty(AccumuloPropertyNames.TIMESTAMP, timestamp);
        edge2.putProperty(AccumuloPropertyNames.COUNT, 2);
        edge2.putProperty(AccumuloPropertyNames.F4, 1);

        Edge edge3 = new Edge(TestGroups.EDGE);
        edge3.setSource("1");
        edge3.setDestination("2");
        edge3.setDirected(true);
        edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
        edge3.putProperty(AccumuloPropertyNames.TIMESTAMP, timestamp);
        edge3.putProperty(AccumuloPropertyNames.COUNT, 10);

        store.execute(new AddElements(Arrays.asList((Element) edge1, edge2, edge3)));

        GetRelatedEdges get = new GetRelatedEdges(defaultView, Collections.singletonList(((ElementSeed) new EntitySeed("1"))));

        // When
        final List<Edge> results = Lists.newArrayList(store.execute(get));

        // Then
        assertEquals(1, results.size());

        final Edge aggregatedEdge = results.get(0);
        assertEquals(expectedResult, aggregatedEdge);
        assertEquals(expectedResult.getProperties(), aggregatedEdge.getProperties());
    }
}
