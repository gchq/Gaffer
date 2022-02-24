/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.data.graph.entity;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Entity;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleEntityMapsTest {

    @Test
    public void shouldIterate() {
        // Given
        final EntityMaps entityMaps = getEntityMaps();
        final Iterator<EntityMap> it = entityMaps.iterator();

        // When
        final EntityMap first = it.next();
        final EntityMap second = it.next();

        assertThat(first.getVertices()).hasSize(3);
        assertThat(second.getVertices()).hasSize(4);
    }

    @Test
    public void shouldGetSize() {
        final EntityMaps entityMaps = getEntityMaps();

        assertThat(entityMaps).hasSize(2);
    }

    @Test
    public void shouldGetNth() {
        final EntityMaps entityMaps = getEntityMaps();

        // Then
        assertThat(entityMaps.get(0).get(0)).contains(makeEntity(0));
        assertThat(entityMaps.get(0).get(1)).contains(makeEntity(1));
        assertThat(entityMaps.get(0).get(2)).contains(makeEntity(2));

        assertThat(entityMaps.get(1).get(0)).contains(makeEntity(0));
        assertThat(entityMaps.get(1).get(1)).contains(makeEntity(1));
        assertThat(entityMaps.get(1).get(2)).contains(makeEntity(2));
        assertThat(entityMaps.get(1).get(3)).contains(makeEntity(3));
    }

    @Test
    public void shouldCheckEmpty() {
        final EntityMaps first = new SimpleEntityMaps();
        final EntityMaps second = new SimpleEntityMaps();

        second.add(getEntityMap(3));

        assertTrue(first.empty());
        assertFalse(second.empty());
    }


    private EntityMaps getEntityMaps() {
        final EntityMaps entityMaps = new SimpleEntityMaps();

        entityMaps.add(getEntityMap(3));
        entityMaps.add(getEntityMap(4));

        return entityMaps;
    }

    private EntityMap getEntityMap(final int size) {

        final EntityMap entityMap = new EntityMap();

        for (int i = 0; i < size; i++) {
            entityMap.putEntity(i, makeEntity(i));
        }

        return entityMap;
    }

    private Entity makeEntity(final Object vertex) {
        return makeEntity(TestGroups.ENTITY, vertex);
    }

    private Entity makeEntity(final String group, final Object vertex) {
        return new Entity.Builder().group(group).vertex(vertex).build();
    }
}
