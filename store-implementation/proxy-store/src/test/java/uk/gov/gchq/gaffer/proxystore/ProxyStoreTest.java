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

import com.google.common.collect.Iterables;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Test;
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
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static uk.gov.gchq.gaffer.store.StoreTrait.AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_TRANSFORMATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.STORE_VALIDATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.VISIBILITY;

public class ProxyStoreTest {

    private static final Schema schema = Schema.fromJson(StreamUtil.schemas(ProxyStoreTest.class));

    private ProxyStore store;

    @Before
    public void before() {
        store = new ProxyStore();
    }

    @Test
    public void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated() throws OperationException {
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
        store.execute(add, user);

        final EntitySeed entitySeed1 = new EntitySeed("1");

        final GetElements<EntitySeed, Element> getBySeed = new GetElements.Builder<EntitySeed, Element>()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .addSeed(entitySeed1)
                .build();
        final CloseableIterable<Element> results = store.execute(getBySeed, user);

        assertEquals(1, Iterables.size(results));
        assertThat(results, IsCollectionContaining.hasItem(e));

        final GetElements<EntitySeed, Element> getRelated = new GetElements.Builder<EntitySeed, Element>()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .addSeed(entitySeed1)
                .build();
        CloseableIterable<Element> relatedResults = store.execute(getRelated, user);
        assertEquals(1, Iterables.size(relatedResults));
        assertThat(relatedResults, IsCollectionContaining.hasItem(e));

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
        relatedResults = store.execute(getRelatedWithPostAggregationFilter, user);
        assertEquals(0, Iterables.size(relatedResults));
    }

    @Test
    public void testStoreTraits() {
        final Collection<StoreTrait> traits = store.getTraits();
        assertNotNull(traits);
        assertTrue("Collection size should be 8", traits.size() == 8);
        assertTrue("Collection should contain AGGREGATION trait", traits.contains(AGGREGATION));
        assertTrue("Collection should contain PRE_AGGREGATION_FILTERING trait", traits.contains(PRE_AGGREGATION_FILTERING));
        assertTrue("Collection should contain POST_AGGREGATION_FILTERING trait", traits.contains(POST_AGGREGATION_FILTERING));
        assertTrue("Collection should contain TRANSFORMATION trait", traits.contains(TRANSFORMATION));
        assertTrue("Collection should contain POST_TRANSFORMATION_FILTERING trait", traits.contains(POST_TRANSFORMATION_FILTERING));
        assertTrue("Collection should contain STORE_VALIDATION trait", traits.contains(STORE_VALIDATION));
        assertTrue("Collection should contain ORDERED trait", traits.contains(ORDERED));
        assertTrue("Collection should contain VISIBILITY trait", traits.contains(VISIBILITY));
    }

}
