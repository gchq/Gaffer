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

package uk.gov.gchq.gaffer.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.fail;

public class WalkTest {

    private final static Edge EDGE_AB = new Edge.Builder().group(TestGroups.EDGE).source("A").dest("B").directed(true).build();
    private final static Edge EDGE_BC = new Edge.Builder().group(TestGroups.EDGE).source("B").dest("C").directed(true).build();
    private final static Edge EDGE_CB = new Edge.Builder().group(TestGroups.EDGE).source("C").dest("B").directed(true).build();
    private final static Edge EDGE_FC = new Edge.Builder().group(TestGroups.EDGE).source("F").dest("C").directed(true).build();
    private final static Edge EDGE_EF = new Edge.Builder().group(TestGroups.EDGE).source("E").dest("F").directed(true).build();
    private final static Edge EDGE_ED = new Edge.Builder().group(TestGroups.EDGE).source("E").dest("D").directed(true).build();
    private final static Edge EDGE_DA = new Edge.Builder().group(TestGroups.EDGE).source("D").dest("A").directed(true).build();
    private final static Edge EDGE_AE = new Edge.Builder().group(TestGroups.EDGE).source("A").dest("E").directed(true).build();

    private final static Entity ENTITY_A = new Entity.Builder().group(TestGroups.ENTITY).vertex("A").build();
    private final static Entity ENTITY_B = new Entity.Builder().group(TestGroups.ENTITY).vertex("B").build();
    private final static Entity ENTITY_C = new Entity.Builder().group(TestGroups.ENTITY).vertex("C").build();
    private final static Entity ENTITY_D = new Entity.Builder().group(TestGroups.ENTITY).vertex("D").build();
    private final static Entity ENTITY_E = new Entity.Builder().group(TestGroups.ENTITY).vertex("E").build();
    private final static Entity ENTITY_F = new Entity.Builder().group(TestGroups.ENTITY).vertex("F").build();
    private final static Entity ENTITY_G = new Entity.Builder().group(TestGroups.ENTITY).vertex("G").build();

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws Exception {
        // Given
        final Walk walk = new Walk.Builder()
                .edge(EDGE_AB)
                .entity(ENTITY_B)
                .edge(EDGE_BC)
                .build();

        // When
        final byte[] json = JSONSerialiser.serialise(walk);
        final Walk deserialisedWalk = JSONSerialiser.deserialise(json, Walk.class);


        // Then
        assertThat(walk, is(equalTo(deserialisedWalk)));
        JsonAssert.assertEquals(String.format("{" +
                "  \"edges\": [" +
                "  [" +
                "    {\"group\": \"BasicEdge\"," +
                "     \"source\": \"A\"," +
                "     \"destination\": \"B\"," +
                "     \"directed\": true," +
                "     \"properties\": {}," +
                "     \"class\": \"uk.gov.gchq.gaffer.data.element.Edge\"}" +
                "  ]," +
                "  [" +
                "    {\"group\": \"BasicEdge\"," +
                "     \"source\": \"B\"," +
                "     \"destination\": \"C\"," +
                "     \"directed\": true," +
                "     \"properties\": {}," +
                "     \"class\": \"uk.gov.gchq.gaffer.data.element.Edge\"}" +
                "    ]" +
                "  ]," +
                "  \"entities\": [" +
                "    {\"A\": []}," +
                "    {\"B\": [" +
                "      {\"group\": \"BasicEntity\"," +
                "      \"vertex\": \"B\"," +
                "      \"properties\": {}," +
                "      \"class\": \"uk.gov.gchq.gaffer.data.element.Entity\"}]" +
                "    }," +
                "    {\"C\": []}" +
                "  ]" +
                "}\n"), new String(json));
    }

    @Test
    public void shouldFailToAddEdgeWithInvalidEntitySource() {
        // Given
        final Walk.Builder builder = new Walk.Builder().entity(ENTITY_A);

        // When
        try {
            builder.edge(EDGE_BC);

            fail("Expecting exception to be thrown when attempting to add an invalid edge.");
        } catch (final Exception e) {
            // Then
            assertThat(e, is(instanceOf(IllegalArgumentException.class)));
            assertThat(e.getMessage(), containsString("Edge must continue the current walk."));
        }
    }

    @Test
    public void shouldFailToAddEdgeWithInvalidEdgeSource() {
        // Given
        final Walk.Builder builder = new Walk.Builder().edge(EDGE_AB);

        // When
        try {
            builder.edge(EDGE_AB);

            fail("Expecting exception to be thrown when attempting to add an invalid edge.");
        } catch (final Exception e) {
            // Then
            assertThat(e, is(instanceOf(IllegalArgumentException.class)));
            assertThat(e.getMessage(), containsString("Edge must continue the current walk."));
        }
    }

    @Test
    public void shouldFailToAddEntityWithInvalidEdgeSource() {
        // Given
        final Walk.Builder builder = new Walk.Builder().edge(EDGE_AB);

        // When
        try {
            builder.entity(ENTITY_A);

            fail("Expecting exception to be thrown when attempting to add an invalid entity.");
        } catch (final Exception e) {
            // Then
            assertThat(e, is(instanceOf(IllegalArgumentException.class)));
            assertThat(e.getMessage(), containsString("Entity must be added to correct vertex."));
        }
    }

    @Test
    public void shouldFailToAddEntityWithInvalidEntitySource() {
        // Given
        final Walk.Builder builder = new Walk.Builder().entity(ENTITY_A);

        // When
        try {
            builder.entity(ENTITY_B);

            fail("Expecting exception to be thrown when attempting to add an invalid entity.");
        } catch (final Exception e) {
            // Then
            assertThat(e, is(instanceOf(IllegalArgumentException.class)));
            assertThat(e.getMessage(), containsString("Entity must be added to correct vertex."));
        }
    }

    @Test
    public void shouldFailToAddEntitiesWithDifferentVertices() {
        // Given
        final Walk.Builder builder = new Walk.Builder();

        // When
        try {
            builder.entities(ENTITY_A, ENTITY_B);

            fail("Expecting exception to be thrown when attempting to add an invalid entity.");
        } catch (final Exception e) {
            // Then
            assertThat(e, is(instanceOf(IllegalArgumentException.class)));
            assertThat(e.getMessage(), containsString("Entities must all have the same vertex."));
        }
    }

    @Test
    public void shouldFailToAddEntitiesWithInvalidEdgeSource() {
        // Given
        final Walk.Builder builder = new Walk.Builder().edge(EDGE_AB);

        // When
        try {
            builder.entities(ENTITY_A, ENTITY_A);

            fail("Expecting exception to be thrown when attempting to add an invalid entity.");
        } catch (final Exception e) {
            // Then
            assertThat(e, is(instanceOf(IllegalArgumentException.class)));
            assertThat(e.getMessage(), containsString("Entity must be added to correct vertex."));
        }
    }

    @Test
    public void shouldFailToAddEntitiesWithInvalidEntitySource() {
        // Given
        final Walk.Builder builder = new Walk.Builder().entity(ENTITY_A);

        // When
        try {
            builder.entities(ENTITY_B, ENTITY_B);

            fail("Expecting exception to be thrown when attempting to add an invalid entity.");
        } catch (final Exception e) {
            // Then
            assertThat(e, is(instanceOf(IllegalArgumentException.class)));
            assertThat(e.getMessage(), containsString("Entity must be added to correct vertex."));
        }
    }

    @Test
    public void shouldBuildWalkStartingWithEdge() {
        // Given
        // [A] -> [B] -> [C]
        //         \
        //          (BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .edge(EDGE_AB)
                .entity(ENTITY_B)
                .edge(EDGE_BC)
                .build();

        // Then
        assertThat(walk.getEntitiesAsEntries(), hasSize(3)); // A, B, C
        assertThat(walk.getEdges(), hasSize(2)); // A -> B, B -> C
        assertThat(walk.getEdges().stream().flatMap(Set::stream).collect(Collectors.toList()), contains(EDGE_AB, EDGE_BC));
        assertThat(walk.getEntities(), contains(Collections.emptySet(), Sets.newHashSet(ENTITY_B), Collections.emptySet()));
        assertThat(walk.getVerticesOrdered(), contains("A", "B", "C"));
    }

    @Test
    public void shouldBuildWalkStartingWithEntity() {
        // Given
        // [A] -> [B] -> [C]
        //  \             \
        //   (BasicEntity) (BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .entity(ENTITY_A)
                .edges(EDGE_AB, EDGE_BC)
                .entity(ENTITY_C)
                .build();

        // Then
        assertThat(walk.getEntitiesAsEntries(), hasSize(3)); // A, B, C
        assertThat(walk.getEdges(), hasSize(2)); // A -> B, B -> C
        assertThat(walk.getEdges().stream().flatMap(Set::stream).collect(Collectors.toList()), contains(EDGE_AB, EDGE_BC));
        assertThat(walk.getEntities(), contains(Sets.newHashSet(ENTITY_A), Collections.emptySet(), Sets.newHashSet(ENTITY_C)));
        assertThat(walk.getVerticesOrdered(), contains("A", "B", "C"));
    }

    @Test
    public void shouldBuildWalkStartingWithEntities() {
        // Given
        // [A] -> [B] -> [C]
        //  \             \
        //   (BasicEntity) (BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .entities(ENTITY_A, ENTITY_A)
                .edges(EDGE_AB, EDGE_BC)
                .entity(ENTITY_C)
                .build();

        // Then
        assertThat(walk.getEntitiesAsEntries(), hasSize(3)); // A, B, C
        assertThat(walk.getEdges(), hasSize(2)); // A -> B, B -> C
        assertThat(walk.getEdges().stream().flatMap(Set::stream).collect(Collectors.toList()), contains(EDGE_AB, EDGE_BC));
        assertThat(walk.getEntities(), contains(Sets.newHashSet(ENTITY_A, ENTITY_A), Collections.emptySet(), Sets.newHashSet(ENTITY_C)));
        assertThat(walk.getVerticesOrdered(), contains("A", "B", "C"));
    }

    @Test
    public void shouldBuildWalkWithLoop() {
        // Given
        // [A] -> [E] -> [D] -> [A]
        //         \             \
        //          (BasicEntity) (BasicEntity, BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .edge(EDGE_AE)
                .entity(ENTITY_E)
                .edges(EDGE_ED, EDGE_DA)
                .entities(ENTITY_A, ENTITY_A)
                .build();

        // Then
        assertThat(walk.getEntitiesAsEntries(), hasSize(4)); // A, D, E, A
        assertThat(walk.getEdges(), hasSize(3)); // A -> E, E -> D, D -> A
        assertThat(walk.getEdges().stream().flatMap(Set::stream).collect(Collectors.toList()), contains(EDGE_AE, EDGE_ED, EDGE_DA));
        assertThat(walk.getEntities(), contains(Collections.emptySet(), Sets.newHashSet(ENTITY_E),
                Collections.emptySet(), Sets.newHashSet(ENTITY_A, ENTITY_A)));
        assertThat(walk.getVerticesOrdered(), contains("A", "E", "D", "A"));
    }

    @Test
    public void shouldAddEmptyIterableOfEntities() {
        // Given
        // [A] -> [E] -> [D] -> [A]
        //         \             \
        //          (BasicEntity) (EmptyIterable)

        // When
        final Walk walk = new Walk.Builder()
                .edge(EDGE_AE)
                .entity(ENTITY_E)
                .edges(EDGE_ED, EDGE_DA)
                .entities(new EmptyClosableIterable<>())
                .build();

        // Then
        assertThat(walk.getEntitiesAsEntries(), hasSize(4)); // A, D, E, A
        assertThat(walk.getEdges(), hasSize(3)); // A -> E, E -> D, D -> A
        assertThat(walk.getEdges().stream().flatMap(Set::stream).collect(Collectors.toList()), contains(EDGE_AE, EDGE_ED, EDGE_DA));
        assertThat(walk.getEntities(), contains(Collections.emptySet(), Sets.newHashSet(ENTITY_E),
                Collections.emptySet(), Collections.emptySet()));
        assertThat(walk.getVerticesOrdered(), contains("A", "E", "D", "A"));
    }

    @Test
    public void shouldGetEntitiesForVertices() {
        // Given
        // [A]     ->    [E]     ->    [D]
        //  \             \             \
        //   (BasicEntity) (BasicEntity) (BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .entity(ENTITY_A)
                .edge(EDGE_AE)
                .entities(ENTITY_E)
                .edge(EDGE_ED)
                .entity(ENTITY_D)
                .build();

        // Then
        assertThat(walk.getEntitiesForVertex("E"), hasSize(1));
        assertThat(walk.getEntitiesForVertex("E"), contains(ENTITY_E));
    }

    @Test
    public void shouldGetEntitiesAtDistance() {
        // Given
        // [A]     ->    [E]     ->    [D]
        //  \             \             \
        //   (BasicEntity) (BasicEntity) (BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .entity(ENTITY_A)
                .edge(EDGE_AE)
                .entities(ENTITY_E, ENTITY_E)
                .edge(EDGE_ED)
                .entity(ENTITY_D)
                .build();

        // Then
        assertThat(walk.getEntitiesAtDistance(2), hasSize(1));
        assertThat(walk.getEntitiesAtDistance(2), contains(ENTITY_D));
    }

    private List<EdgeId> getEdges() {
        return IntStream.range(0, 10).mapToObj(i -> {
            return new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(i)
                    .dest(i + 1)
                    .build();
        }).collect(toList());
    }

    private List<EntityId> getEntities() {
        return IntStream.range(0, 10).mapToObj(i -> {
            return new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex(i)
                    .build();
        }).collect(Collectors.toList());
    }

    private List<EntityId> getEntities(final Object vertex) {
        final List<Entity> entities = new ArrayList<>();

        entities.add(new Entity.Builder().group(TestGroups.ENTITY).vertex(vertex).build());
        entities.add(new Entity.Builder().group(TestGroups.ENTITY_2).vertex(vertex).build());

        return Collections.unmodifiableList(entities);
    }

}
