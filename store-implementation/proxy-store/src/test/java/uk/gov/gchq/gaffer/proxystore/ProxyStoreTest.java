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

package uk.gov.gchq.gaffer.proxystore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Iterables;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.rest.RestApiTestUtil;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ProxyStoreTest {
    private Graph graph;

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);


    @BeforeClass
    public static void beforeClass() throws Exception {
        RestApiTestUtil.startServer();
    }

    @AfterClass
    public static void afterClass() {
        RestApiTestUtil.stopServer();
    }

    @Before
    public void before() throws IOException {
        RestApiTestUtil.reinitialiseGraph(testFolder, StreamUtil.SCHEMA, "accumulo-store.properties");

        // setup ProxyStore
        graph = new Graph.Builder()
                .storeProperties(StreamUtil.openStream(ProxyStoreTest.class, "proxy-store.properties"))
                .build();
    }

    @Test
    public void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated() throws OperationException, JsonProcessingException {
        final List<Element> elements = new ArrayList<>();
        final Entity e = new Entity(TestGroups.ENTITY, "1");
        e.putProperty(TestPropertyNames.PROP_1, 1);
        e.putProperty(TestPropertyNames.PROP_2, 2);
        e.putProperty(TestPropertyNames.PROP_3, 3);
        e.putProperty(TestPropertyNames.PROP_4, 4);
        e.putProperty(TestPropertyNames.COUNT, 1);

        final User user = new User();
        elements.add(e);
        final AddElements add = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(add, user);

        final EntitySeed entitySeed1 = new EntitySeed("1");

        final GetElements<EntitySeed, Element> getBySeed = new GetElements.Builder<EntitySeed, Element>()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .addSeed(entitySeed1)
                .build();
        final CloseableIterable<Element> results = graph.execute(getBySeed, user);

        assertEquals(1, Iterables.size(results));
        assertThat(results, hasItem(e));

        final GetElements<EntitySeed, Element> getRelated = new GetElements.Builder<EntitySeed, Element>()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .addSeed(entitySeed1)
                .build();
        CloseableIterable<Element> relatedResults = graph.execute(getRelated, user);
        assertEquals(1, Iterables.size(relatedResults));
        assertThat(relatedResults, hasItem(e));

        final GetElements<EntitySeed, Element> getRelatedWithPostAggregationFilter = new GetElements.Builder<EntitySeed, Element>()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(TestPropertyNames.PROP_1)
                                        .execute(new IsMoreThan(0))
                                        .build())
                                .postAggregationFilter(new ElementFilter.Builder()
                                        .select(TestPropertyNames.COUNT)
                                        .execute(new IsMoreThan(6))
                                        .build())
                                .build())
                        .build())
                .addSeed(entitySeed1)
                .build();
        relatedResults = graph.execute(getRelatedWithPostAggregationFilter, user);
        assertEquals(0, Iterables.size(relatedResults));
    }

    @Test
    public void testStoreTraits() {
        assertEquals(AccumuloStore.TRAITS, graph.getStoreTraits());
    }
}
