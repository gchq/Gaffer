/*
 * Copyright 2016-2019 Crown Copyright
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
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;

import java.util.ArrayList;
import java.util.Arrays;

public class NoAggregationIT extends AbstractStoreIT {

    @Override
    public void _setup() throws Exception {
        addDuplicatedTestElements();
    }

    @Test
    public void shouldReturnDuplicateEntitiesWhenNoAggregationIsUsed() throws OperationException {
        //Given
        final ArrayList<Entity> expected = Lists.newArrayList(getEntity(), getEntity());

        //When
        final CloseableIterable<? extends Element> result = graph.execute(
                new GetElements.Builder()
                        .input(ElementSeed.createSeed(getEntity()))
                        .view(new View.Builder()
                                .entity(TestGroups.ENTITY)
                                .build())
                        .build(),
                getUser());

        //Then
        ElementUtil.assertElementEquals(expected, result);
    }

    @Test
    public void shouldReturnDuplicateEdgesWhenNoAggregationIsUsed() throws OperationException {
        //Given
        final ArrayList<Edge> expected = Lists.newArrayList(getEdge(), getEdge());

        //When
        final CloseableIterable<? extends Element> result = graph.execute(
                new GetElements.Builder()
                        .input(ElementSeed.createSeed(getEdge()))
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE)
                                .build())
                        .build(),
                getUser());

        //Then
        ElementUtil.assertElementEquals(expected, result);
    }

    public void addDuplicatedTestElements() throws OperationException {
        graph.execute(new AddElements.Builder()
                .input(Arrays.asList(getEdge(), getEdge()))
                .build(), getUser());

        graph.execute(new AddElements.Builder()
                .input(Arrays.asList(getEntity(), getEntity()))
                .build(), getUser());
    }

    private Entity getEntity() {
        return new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex(VERTEX_PREFIXES[0])
                .property(TestPropertyNames.STRING, "prop1")
                .build();
    }

    private Edge getEdge() {
        return new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(SOURCE_1)
                .dest(DEST_1)
                .property(TestPropertyNames.STRING, "prop1")
                .build();
    }

    @Override
    protected Schema createSchema() {
        return new Schema.Builder()
                .entity(TestGroups.ENTITY,
                        new SchemaEntityDefinition.Builder()
                                .vertex(TestTypes.ID_STRING)
                                .property(TestPropertyNames.STRING, TestTypes.PROP_STRING)
                                .aggregate(false)
                                .build())
                .edge(TestGroups.EDGE,
                        new SchemaEdgeDefinition.Builder()
                                .source(TestTypes.ID_STRING)
                                .directed(TestTypes.DIRECTED_EITHER)
                                .destination(TestTypes.ID_STRING)
                                .property(TestPropertyNames.STRING, TestTypes.PROP_STRING)
                                .aggregate(false)
                                .build())
                .type(TestTypes.ID_STRING,
                        new TypeDefinition.Builder()
                                .clazz(String.class)
                                .build())
                .type(TestTypes.PROP_STRING,
                        new TypeDefinition.Builder()
                                .clazz(String.class)
                                .aggregateFunction(null)
                                .build())
                .type(TestTypes.DIRECTED_EITHER,
                        new TypeDefinition.Builder()
                                .clazz(Boolean.class)
                                .build())
                .build();
    }
}
