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

package uk.gov.gchq.gaffer.integration.impl.loader;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.integration.AbstractStoreWithCustomGraphIT;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.TestSchemas;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.user.User;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.gov.gchq.gaffer.data.util.ElementUtil.assertElementEquals;

public abstract class AbstractLoaderIT<T extends Operation> extends AbstractStoreWithCustomGraphIT {

    protected Iterable<? extends Element> input;

    @Override
    public void setup() throws Exception {
        super.setup();
        input = getInputElements();
        configure(input);
    }

    @Test
    public void shouldAddElements() throws Exception {
        // Given
        createGraph(getSchema());

        // When
        addElements();
        final List<? extends Element> result = Lists.newArrayList(getAllElements());

        // Then
        shouldGetAllElements();
//        shouldGetElementsWithMatchedVertex();
//        shouldGetElementsWithMatchedVertexFilter();
//        shouldGetElementsWithProvidedProperties();
    }

    protected void shouldGetAllElements() throws Exception {
        for (final boolean includeEntities : Arrays.asList(true, false)) {
            for (final boolean includeEdges : Arrays.asList(true, false)) {
                if (!includeEntities && !includeEdges) {
                    // Cannot query for nothing!
                    continue;
                }
                for (final DirectedType directedType : DirectedType.values()) {
                    try {
                        shouldGetAllElements(includeEntities, includeEdges, directedType);
                    } catch (final AssertionError e) {
                        throw new AssertionError("GetAllElements failed with parameters: includeEntities=" + includeEntities
                                + ", includeEdges=" + includeEdges + ", directedType=" + directedType.name(), e);
                    }
                }
            }
        }
    }

    private void shouldGetAllElements(final boolean includeEntities, final boolean includeEdges, final DirectedType directedType) throws Exception {
        // Given
        final List<Element> expectedElements = new ArrayList<>();
        if (includeEntities) {
            expectedElements.addAll(getEntities().values());
        }

        if (includeEdges) {
            for (final Edge edge : getEdges().values()) {
                if (DirectedType.EITHER == directedType
                        || (edge.isDirected() && DirectedType.DIRECTED == directedType)
                        || (!edge.isDirected() && DirectedType.UNDIRECTED == directedType)) {
                    expectedElements.add(edge);
                }
            }
        }

        final View.Builder viewBuilder = new View.Builder();
        if (includeEntities) {
            viewBuilder.entity(TestGroups.ENTITY);
        }
        if (includeEdges) {
            viewBuilder.edge(TestGroups.EDGE);
        }
        final GetAllElements op = new GetAllElements.Builder()
                .directedType(directedType)
                .view(viewBuilder.build())
                .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, getUser());

        // Then
        assertElementEquals(expectedElements, results);
    }

    protected void addElements() throws OperationException {
        graph.execute(createOperation(input), getUser());
    }

    protected Iterable<? extends Element> getAllElements() throws OperationException {
        return graph.execute(new GetAllElements(), getUser());
    }

    protected Iterable<? extends Element> getInputElements() {
        final Iterable<? extends Element> edges = getEdges().values();
        final Iterable<? extends Element> entities = getEntities().values();

        return Iterables.concat(edges, entities);
    }

    protected abstract void configure(final Iterable<? extends Element> elements) throws Exception ;

    protected abstract T createOperation(final Iterable<? extends Element> elements);

    protected abstract Schema getSchema();
}
