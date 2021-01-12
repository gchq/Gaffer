/*
 * Copyright 2018-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.template;

import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.integration.extensions.GafferTestCase;
import org.junit.jupiter.api.AfterEach;

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
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.util.TestUtil;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsIn;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST_1;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_1;

public class GraphHooksITTemplate extends AbstractStoreIT {

    @AfterEach
    public void cleanUp() {
        CacheServiceLoader.shutdown();
    }

    @GafferTest
    public void shouldResolveNamedViewWithinNamedOperation(final GafferTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        final Edge edge1 = TestUtil.getEdges().get(new EdgeSeed(SOURCE_1, DEST_1, false)).emptyClone();
        edge1.putProperty(TestPropertyNames.INT, 100);

        final Edge edge2 = edge1.emptyClone();
        edge2.putProperty(TestPropertyNames.INT, 101);

        graph.execute(new AddElements.Builder()
                        .input(edge1, edge2)
                        .build(),
                new User());

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

        graph.execute(addNamedView, new User());

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
                .labels(Arrays.asList("label 1", "Label 2"))
                .overwrite(true)
                .build();

        graph.execute(addNamedOperation, new User());

        final NamedOperation<EntityId, CloseableIterable<? extends Element>> operation =
                new NamedOperation.Builder<EntityId, CloseableIterable<? extends Element>>()
                        .name("GetAllElements test")
                        .input(new EntitySeed("10"))
                        .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(operation, new User());

        // Then
        final List<Element> resultList = Lists.newArrayList(results);
        assertEquals(1, resultList.size());
        assertTrue(resultList.contains(edge1));
    }
}
