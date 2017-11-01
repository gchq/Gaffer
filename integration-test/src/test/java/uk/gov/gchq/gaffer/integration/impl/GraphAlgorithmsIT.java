/*
 * Copyright 2017 Crown Copyright
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

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Max;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.AgeOff;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class GraphAlgorithmsIT extends AbstractStoreIT {

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        // Replace with AddElements operation

        addDefaultElements();
    }

    @Test
    public void shouldGetPaths() throws Exception {
        // Given
        final User user = new User();

        final EntitySeed seed = new EntitySeed("A");

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seed)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, user);

        // Then
        assertThat(getPaths(results), is(equalTo("AED,ABC")));
    }

    @Test
    public void shouldGetPathsWithMultipleSeeds() throws Exception {
        // Given
        final User user = new User();

        final EntitySeed seed1 = new EntitySeed("A");
        final EntitySeed seed2 = new EntitySeed("E");

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seed1, seed2)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, user);

        // Then
        assertThat(getPaths(results), is(equalTo("AED,ABC,EDA")));
    }

    @Test
    public void shouldGetPathsWithMultipleEdgeTypes() throws Exception {
        // Given
        final User user = new User();

        final EntitySeed seed = new EntitySeed("A");

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seed)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, user);

        // Then
        assertThat(getPaths(results), is(equalTo("AED,AEF,ABC")));
    }

    @Test
    public void shouldGetPathsWithMultipleSeedsAndMultipleEdgeTypes() throws Exception {
        // Given
        final User user = new User();

        final EntitySeed seed1 = new EntitySeed("A");
        final EntitySeed seed2 = new EntitySeed("E");

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seed1, seed2)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, user);

        // Then
        assertThat(getPaths(results), is(equalTo("AED,AEF,ABC,EDA,EFC")));
    }

    @Test
    public void shouldGetPathsWithLoops() throws Exception {
        // Given
        final User user = new User();

        final EntitySeed seed = new EntitySeed("A");

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seed)
                .operations(operation, operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, user);

        // Then
        assertThat(getPaths(results), is(equalTo("AEDA,AEFC")));
    }

    @Test
    public void shouldGetPathsWithLoops_2() throws Exception {
        // Given
        final User user = new User();

        final EntitySeed seed = new EntitySeed("A");

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seed)
                .operations(operation, operation, operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, user);

        // Then
        assertThat(getPaths(results), is(equalTo("AEDAE,AEDAB")));
    }

    @Test
    public void shouldGetPathsWithLoops_3() throws Exception {
        // Given
        final User user = new User();

        final EntitySeed seed = new EntitySeed("A");

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE_3, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seed)
                .operations(operation, operation, operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, user);

        // Then
        assertThat(getPaths(results), is(equalTo("AAAAA")));
    }

    private Set<Entity> createEntitySet() {
        final Set<Entity> entities = new HashSet<>();

        final Entity firstEntity = new Entity(TestGroups.ENTITY, "A");
        firstEntity.putProperty(TestPropertyNames.STRING, "3");
        entities.add(firstEntity);

        final Entity secondEntity = new Entity(TestGroups.ENTITY, "B");
        secondEntity.putProperty(TestPropertyNames.STRING, "3");
        entities.add(secondEntity);

        final Entity thirdEntity = new Entity(TestGroups.ENTITY, "C");
        thirdEntity.putProperty(TestPropertyNames.STRING, "3");
        entities.add(thirdEntity);

        final Entity fourthEntity = new Entity(TestGroups.ENTITY, "D");
        fourthEntity.putProperty(TestPropertyNames.STRING, "3");
        entities.add(fourthEntity);

        final Entity fifthEntity = new Entity(TestGroups.ENTITY, "E");
        fifthEntity.putProperty(TestPropertyNames.STRING, "3");
        entities.add(fifthEntity);

        final Entity sixthEntity = new Entity(TestGroups.ENTITY, "F");
        sixthEntity.putProperty(TestPropertyNames.STRING, "3");
        entities.add(sixthEntity);

        return entities;
    }

    private Set<Edge> createEdgeSet() {
        final Set<Edge> edges = new HashSet<>();

        final Edge firstEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("A")
                .dest("B")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(firstEdge);

        final Edge secondEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("B")
                .dest("C")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(secondEdge);

        final Edge thirdEdge = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .source("B")
                .dest("C")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(thirdEdge);

        final Edge fourthEdge = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .source("F")
                .dest("C")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(fourthEdge);

        final Edge fifthEdge = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .source("E")
                .dest("F")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(fifthEdge);

        final Edge sixthEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("E")
                .dest("D")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(sixthEdge);

        final Edge seventhEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("D")
                .dest("A")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(seventhEdge);

        final Edge eighthEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("A")
                .dest("E")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(eighthEdge);

        final Edge ninthEdge = new Edge.Builder()
                .group(TestGroups.EDGE_3)
                .source("A")
                .dest("A")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(ninthEdge);

        return edges;
    }

    @Override
    public void addDefaultElements() throws OperationException {
        graph.execute(new AddElements.Builder()
                .input(createEntitySet())
                .build(), getUser());

        graph.execute(new AddElements.Builder()
                .input(createEdgeSet())
                .build(), getUser());
    }

    @Override
    protected Schema createSchema() {
        return new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_EITHER, new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .build())
                .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new StringConcat())
                        .build())
                .type(TestTypes.PROP_INTEGER, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(new Max())
                        .build())
                .type(TestTypes.PROP_COUNT, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Sum())
                        .build())
                .type(TestTypes.TIMESTAMP, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Max())
                        .build())
                .type(TestTypes.TIMESTAMP_2, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Max())
                        .validateFunctions(new AgeOff(AGE_OFF_TIME))
                        .build())
                .type(TestTypes.PROP_INTEGER_2, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(new Max())
                        .validateFunctions(new IsLessThan(10))
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.STRING, TestTypes.PROP_STRING)
                        .groupBy(TestPropertyNames.INT)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_EITHER)
                        .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER)
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .groupBy(TestPropertyNames.INT)
                        .build())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_EITHER)
                        .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER)
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .groupBy(TestPropertyNames.INT)
                        .build())
                .edge(TestGroups.EDGE_3, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_EITHER)
                        .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER)
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .groupBy(TestPropertyNames.INT)
                        .build())
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP_2)
                        .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER_2)
                        .build())
                .build();
    }

    private String getPaths(final Iterable<Walk> walks) {
        final StringBuilder sb = new StringBuilder();
        for (final Walk walk : walks) {
            sb.append(walk.getVerticesOrdered().stream().map(Object::toString).collect(Collectors.joining("")));
            sb.append(',');
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
}
