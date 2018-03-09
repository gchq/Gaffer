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

package uk.gov.gchq.gaffer.data.graph.entity;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Entity;

import java.util.Iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleEntityMapsTest {

    @Test
    public void shouldIterate() {
        // When
        final EntityMaps entityMaps = getEntityMaps();

        // Then
        final Iterator<EntityMap> it = entityMaps.iterator();

        final EntityMap first = it.next();
        final EntityMap second = it.next();

        assertThat(first.getVertices(), hasSize(3));
        assertThat(second.getVertices(), hasSize(4));
    }

    @Test
    public void shouldGetSize() {
        // When
        final EntityMaps entityMaps = getEntityMaps();

        // Then
        assertThat(entityMaps.size(), is(equalTo(2)));
    }

    @Test
    public void shouldGetNth() {
        // When
        final EntityMaps entityMaps = getEntityMaps();

        // Then
        assertThat(entityMaps.get(0).get(0), hasItem(makeEntity(0)));
        assertThat(entityMaps.get(0).get(1), hasItem(makeEntity(1)));
        assertThat(entityMaps.get(0).get(2), hasItem(makeEntity(2)));

        assertThat(entityMaps.get(1).get(0), hasItem(makeEntity(0)));
        assertThat(entityMaps.get(1).get(1), hasItem(makeEntity(1)));
        assertThat(entityMaps.get(1).get(2), hasItem(makeEntity(2)));
        assertThat(entityMaps.get(1).get(3), hasItem(makeEntity(3)));
    }

    @Test
    public void shouldCheckEmpty() {
        // When
        final EntityMaps first = new SimpleEntityMaps();
        final EntityMaps second = new SimpleEntityMaps();

        second.add(getEntityMap(3));

        // Then
        assertTrue(first.empty());
        assertFalse(second.empty());
    }

    private EntityMap getEntityMap(final int size) {

        final EntityMap entityMap = new EntityMap();

        for (int i = 0; i < size; i++) {
            entityMap.putEntity(i, makeEntity(i));
        }

        return entityMap;
    }

    private EntityMaps getEntityMaps() {
        final EntityMaps entityMaps = new SimpleEntityMaps();

        entityMaps.add(getEntityMap(3));
        entityMaps.add(getEntityMap(4));

        return entityMaps;
    }

    private Entity makeEntity(final Object vertex) {
        return makeEntity(TestGroups.ENTITY, vertex);
    }

    private Entity makeEntity(final String group, final Object vertex) {
        return new Entity.Builder().group(group).vertex(vertex).build();
    }
}
