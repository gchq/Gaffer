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

package uk.gov.gchq.gaffer.accumulostore.key.impl;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AggregatorIteratorTest {

    private static final Schema schema = Schema.fromJson(StreamUtil.schemas(AggregatorIteratorTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil
            .storeProps(AggregatorIteratorTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties
            .loadStoreProperties(StreamUtil.openStream(AggregatorIteratorTest.class, "/accumuloStoreClassicKeys.properties"));
    private static View defaultView;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;

    @BeforeClass
    public static void setup() throws IOException, StoreException {
        byteEntityStore = new SingleUseMockAccumuloStore();
        gaffer1KeyStore = new SingleUseMockAccumuloStore();

        defaultView = new View.Builder()
                .edge(TestGroups.EDGE)
                .entity(TestGroups.ENTITY)
                .build();
    }

    @AfterClass
    public static void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
        defaultView = null;
    }

    @Before
    public void reInitialise() throws StoreException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        byteEntityStore.initialise(schema, PROPERTIES);
        gaffer1KeyStore.initialise(schema, CLASSIC_PROPERTIES);
    }

    @Test
    public void test() throws OperationException {
        test(byteEntityStore);
        test(gaffer1KeyStore);
    }

    private void test(final AccumuloStore store) throws OperationException {
        // Given
        final Edge expectedResult = new Edge(TestGroups.EDGE);
        expectedResult.setSource("1");
        expectedResult.setDestination("2");
        expectedResult.setDirected(true);
        expectedResult.putProperty(AccumuloPropertyNames.COUNT, 13);
        expectedResult.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
        expectedResult.putProperty(AccumuloPropertyNames.PROP_1, 0);
        expectedResult.putProperty(AccumuloPropertyNames.PROP_2, 0);
        expectedResult.putProperty(AccumuloPropertyNames.PROP_3, 1);
        expectedResult.putProperty(AccumuloPropertyNames.PROP_4, 1);

        final Edge edge1 = new Edge(TestGroups.EDGE);
        edge1.setSource("1");
        edge1.setDestination("2");
        edge1.setDirected(true);
        edge1.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
        edge1.putProperty(AccumuloPropertyNames.COUNT, 1);
        edge1.putProperty(AccumuloPropertyNames.PROP_1, 0);
        edge1.putProperty(AccumuloPropertyNames.PROP_2, 0);
        edge1.putProperty(AccumuloPropertyNames.PROP_3, 1);
        edge1.putProperty(AccumuloPropertyNames.PROP_4, 0);

        final Edge edge2 = new Edge(TestGroups.EDGE);
        edge2.setSource("1");
        edge2.setDestination("2");
        edge2.setDirected(true);
        edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
        edge2.putProperty(AccumuloPropertyNames.COUNT, 2);
        edge2.putProperty(AccumuloPropertyNames.PROP_1, 0);
        edge2.putProperty(AccumuloPropertyNames.PROP_2, 0);
        edge2.putProperty(AccumuloPropertyNames.PROP_3, 0);
        edge2.putProperty(AccumuloPropertyNames.PROP_4, 1);

        final Edge edge3 = new Edge(TestGroups.EDGE);
        edge3.setSource("1");
        edge3.setDestination("2");
        edge3.setDirected(true);
        edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
        edge3.putProperty(AccumuloPropertyNames.COUNT, 10);
        edge3.putProperty(AccumuloPropertyNames.PROP_1, 0);
        edge3.putProperty(AccumuloPropertyNames.PROP_2, 0);
        edge3.putProperty(AccumuloPropertyNames.PROP_3, 0);
        edge3.putProperty(AccumuloPropertyNames.PROP_4, 0);

        final User user = new User();
        store.execute(new AddElements(Arrays.asList((Element) edge1, edge2, edge3)), user);

        final GetEdges<EntitySeed> get = new GetEdges.Builder<EntitySeed>()
                .view(defaultView)
                .addSeed(new EntitySeed("1"))
                .build();

        // When
        final List<Edge> results = Lists.newArrayList(store.execute(get, user));

        // Then
        assertEquals(1, results.size());

        final Edge aggregatedEdge = results.get(0);
        assertEquals(expectedResult, aggregatedEdge);
        assertEquals(expectedResult.getProperties(), aggregatedEdge.getProperties());
    }
}
