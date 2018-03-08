/*
 * Copyright 2016-2018 Crown Copyright
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
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationChain.Builder;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NoAggregationIT extends AbstractStoreIT {

    public static final String PROPERTY_VALUE = "p1";
    public static final String B = "B";
    public static final String A = "A";
    private static final User USER_DEFAULT = new User();

    @Test
    public void shouldReturnDuplicateEntitiesWhenNoAggregationIsUsed() throws OperationException {
        //Given
        final ArrayList<Entity> input = Lists.newArrayList(getEntity(), getEntity());

        //When
        final CloseableIterable<? extends Element> elements = graph.execute(
                new Builder()
                        .first(new AddElements.Builder()
                                .input(input)
                                .build())
                        .then(new GetAllElements.Builder()
                                .build())
                        .build(),
                USER_DEFAULT);

        //Then
        assertContainsSame(elements.iterator(), input);
    }

    private Entity getEntity() {
        return new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex(A)
                .property(TestPropertyNames.STRING, PROPERTY_VALUE)
                .build();
    }


    @Test
    public void shouldReturnDuplicateEdgesWhenNoAggregationIsUsed() throws OperationException {
        //Given
        final ArrayList<Edge> input = Lists.newArrayList(getEdge(), getEdge());

        //When
        final CloseableIterable<? extends Element> elements = graph.execute(
                new Builder()
                        .first(new AddElements.Builder()
                                .input(input)
                                .build())
                        .then(new GetAllElements.Builder()
                                .build())
                        .build(),
                USER_DEFAULT);

        //Then
        assertContainsSame(elements.iterator(), input);
    }

    private Edge getEdge() {
        return new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(A)
                .dest(B)
                .property(TestPropertyNames.STRING, PROPERTY_VALUE)
                .build();
    }


    private void assertContainsSame(final CloseableIterator<? extends Element> elementIterator, final ArrayList<? extends Element> elements) {
        if (elementIterator == null) {
            fail("Iterator with expected size was null.");
        }

        @SuppressWarnings("unchecked") final ArrayList shallowClone = (ArrayList) elements.clone();

        while (elementIterator.hasNext()) {
            Element next = elementIterator.next();
            assertTrue("ArrayList did not contain: " + next, shallowClone.remove(next));
        }

        assertTrue("ArrayList contains more items than Iterator", shallowClone.isEmpty());
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
                .build();
    }
}
