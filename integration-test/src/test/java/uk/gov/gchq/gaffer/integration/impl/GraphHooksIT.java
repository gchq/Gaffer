/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.Lists;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.koryphe.impl.predicate.IsIn;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class GraphHooksIT extends AbstractStoreIT {
    @Override
    @Before
    public void setup() throws Exception {
        final Properties properties = new Properties();
        properties.setProperty("gaffer.cache.service.class", "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService");
        CacheServiceLoader.initialise(properties);
        super.setup();
        addDefaultElements();
    }

    @After
    public void cleanUp() {
        CacheServiceLoader.shutdown();
    }

    @Test
    public void shouldResolveNamedViewWithinNamedOperation() throws OperationException {
        // Given
        final Edge edge1 = getEdges().get(new EdgeSeed(SOURCE_1, DEST_1, false)).emptyClone();
        edge1.putProperty(TestPropertyNames.INT, 100);

        final Edge edge2 = edge1.emptyClone();
        edge2.putProperty(TestPropertyNames.INT, 101);

        graph.execute(new AddElements.Builder()
                        .input(edge1, edge2)
                        .build(),
                getUser());

        final AddNamedView addNamedView = new AddNamedView.Builder()
                .name("Test View")
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(TestPropertyNames.INT)
                                        .execute(new IsIn(Arrays.asList((Object) 100)))
                                        .build())
                                .build())
                        .build())
                .overwrite(true)
                .build();

        graph.execute(addNamedView, getUser());

        final AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
                .operationChain(new OperationChain.Builder()
                        .first(new GetAllElements.Builder()
                                .view(new NamedView.Builder()
                                        .name("Test View")
                                        .build())
                                .build())
                        .build())
                .description("named operation GetAllElements test query")
                .name("GetAllElements test")
                .overwrite(true)
                .build();

        graph.execute(addNamedOperation, getUser());

        final NamedOperation<EntityId, CloseableIterable<? extends Element>> operation =
                new NamedOperation.Builder<EntityId, CloseableIterable<? extends Element>>()
                        .name("GetAllElements test")
                        .input(new EntitySeed("10"))
                        .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(operation, getUser());

        // Then
        final List<Element> resultList = Lists.newArrayList(results);
        assertEquals(1, resultList.size());
        assertThat(resultList, IsCollectionContaining.hasItems(
                (Element) edge1));
    }
}
