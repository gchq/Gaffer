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

package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PartAggregationIT extends AbstractStoreIT {
    @Test
    public void shouldAggregateOnlyRequiredGroups() throws OperationException {
        //Given
        addDefaultElements();
        addDefaultElements();

        //When
        final CloseableIterable<? extends Element> elements = graph.execute(
                new GetAllElements(), getUser());

        //Then
        final List<Element> resultElements = Lists.newArrayList(elements);
        final List<Element> expectedElements = new ArrayList<>();
        getEntities().values().forEach(e -> {
            final Entity clone = e.emptyClone();
            clone.copyProperties(e.getProperties());
            expectedElements.add(clone);
        });
        getEdges().values().forEach(e -> {
            final Edge clone = e.emptyClone();
            clone.copyProperties(e.getProperties());
            expectedElements.add(clone);
        });
        expectedElements.forEach(e -> {
            if (TestGroups.ENTITY.equals(e.getGroup())) {
                e.putProperty(TestPropertyNames.STRING, "3,3");
            } else if (TestGroups.EDGE.equals(e.getGroup())) {
                e.putProperty(TestPropertyNames.COUNT, 2L);
            } else if (TestGroups.EDGE_2.equals(e.getGroup())) {
                e.putProperty(TestPropertyNames.INT, 2);
            }
        });

        // Non aggregated elements should appear twice
        expectedElements.addAll(getNonAggregatedEntities());
        expectedElements.addAll(getNonAggregatedEntities());
        expectedElements.addAll(getNonAggregatedEdges());
        expectedElements.addAll(getNonAggregatedEdges());

        resultElements.sort(getJsonSort());
        expectedElements.sort(getJsonSort());

        // This will help with displaying errors by only comparing the incorrect results.
        final List<Element> expectedElementsClone = new ArrayList<>(expectedElements);
        final List<Element> resultElementsClone = new ArrayList<>(resultElements);
        resultElementsClone.removeAll(expectedElements);
        expectedElementsClone.removeAll(resultElements);
        if (expectedElementsClone.isEmpty())
            assertEquals("Expected first element: \n  " + (expectedElementsClone.isEmpty() ? "" : expectedElementsClone.get(0))
                            + "\nbut the first element was\n  " + (resultElementsClone.isEmpty() ? "" : resultElementsClone.get(1))
                            + "\n\n",
                    expectedElementsClone, resultElementsClone);
    }

    @Override
    public void addDefaultElements() throws OperationException {
        super.addDefaultElements();

        graph.execute(new AddElements.Builder()
                .input(getNonAggregatedEntities())
                .build(), getUser());

        graph.execute(new AddElements.Builder()
                .input(getNonAggregatedEdges())
                .build(), getUser());
    }

    @Override
    protected Schema createSchema() {
        final Schema defaultSchema = super.createSchema();

        // Add 2 new groups that are the same as the defaults but with no aggregation.
        return new Schema.Builder()
                .merge(defaultSchema)
                .entity(TestGroups.ENTITY_3,
                        new SchemaEntityDefinition.Builder()
                                .vertex(TestTypes.ID_STRING)
                                .property(TestPropertyNames.STRING, TestTypes.PROP_STRING)
                                .property("NonAggregatedProperty", "NonAggregatedString")
                                .property("NonAggregatedProperty", "AggregatedString")
                                .aggregate(false)
                                .groupBy()
                                .build())
                .edge(TestGroups.EDGE_3,
                        new SchemaEdgeDefinition.Builder()
                                .source(TestTypes.ID_STRING)
                                .destination(TestTypes.ID_STRING)
                                .directed(TestTypes.DIRECTED_EITHER)
                                .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER)
                                .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                                .property("NonAggregatedProperty", "NonAggregatedString")
                                .aggregate(false)
                                .groupBy()
                                .build())
                .type("NonAggregatedString",
                        new TypeDefinition.Builder()
                                .clazz(String.class)
                                .build())
                .type("AggregatedString",
                        new TypeDefinition.Builder()
                                .clazz(String.class)
                                .aggregateFunction(new StringConcat())
                                .build())
                .build();
    }

    private List<Edge> getNonAggregatedEdges() {
        final Function<Edge, Edge> edgeTransform = e -> {
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE_3)
                    .source(e.getSource())
                    .dest(e.getDestination())
                    .directed(e.isDirected())
                    .property("NonAggregatedProperty", "some value")
                    .build();
            edge.copyProperties(e.getProperties());
            return edge;
        };

        return Lists.transform(
                Lists.newArrayList(getEdges().values()),
                edgeTransform);
    }

    private List<Entity> getNonAggregatedEntities() {
        final Function<Entity, Entity> entityTransform = e -> {
            final Entity entity = new Entity.Builder()
                    .group(TestGroups.ENTITY_3)
                    .vertex(e.getVertex())
                    .property("NonAggregatedProperty", "some value")
                    .build();
            entity.copyProperties(e.getProperties());
            return entity;
        };
        return Lists.transform(
                Lists.newArrayList(getEntities().values()),
                entityTransform);
    }

    private Comparator<Element> getJsonSort() {
        return Comparator.comparing(a -> {
            try {
                return new String(JSONSerialiser.serialise(a));
            } catch (final SerialisationException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
