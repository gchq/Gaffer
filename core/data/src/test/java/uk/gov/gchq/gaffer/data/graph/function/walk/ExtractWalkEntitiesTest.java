/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.data.graph.function.walk;

import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.data.graph.function.walk.ExtractWalkEntities;

import java.util.Set;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class ExtractWalkEntitiesTest {
    private static final Edge EDGE_AB = new Edge.Builder().group(TestGroups.EDGE).source("A").dest("B").directed(true).build();
    private static final Edge EDGE_BC = new Edge.Builder().group(TestGroups.EDGE).source("B").dest("C").directed(true).build();
    private static final Edge EDGE_CA = new Edge.Builder().group(TestGroups.EDGE).source("C").dest("A").directed(true).build();

    private static final Entity ENTITY_A = new Entity.Builder().group(TestGroups.ENTITY).vertex("A").build();
    private static final Entity ENTITY_B = new Entity.Builder().group(TestGroups.ENTITY).vertex("B").build();
    private static final Entity ENTITY_C = new Entity.Builder().group(TestGroups.ENTITY).vertex("C").build();

    @Test
    public void shouldReturnEntitiesFromWalkObject() {
        // Given
        final Function<Walk, Iterable<Set<Entity>>> function = new ExtractWalkEntities();
        final Walk walk = new Walk.Builder()
                .entity(ENTITY_A)
                .edge(EDGE_AB)
                .entity(ENTITY_B)
                .edge(EDGE_BC)
                .entity(ENTITY_C)
                .edge(EDGE_CA)
                .entity(ENTITY_A)
                .build();

        // When
        final Iterable<Set<Entity>> results = function.apply(walk);

        // Then
        assertThat(results, containsInAnyOrder(
                Sets.newHashSet(ENTITY_A),
                Sets.newHashSet(ENTITY_B),
                Sets.newHashSet(ENTITY_C),
                Sets.newHashSet(ENTITY_A)));
    }
}
