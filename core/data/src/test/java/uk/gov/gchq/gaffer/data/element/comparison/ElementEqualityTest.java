/*
 * Copyright 2018-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.data.element.comparison;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Entity;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ElementEqualityTest {

    @Test
    public void shouldBeEqualOnSameElement() {
        // Given
        final Entity testEntity1 = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 3L)
                .build();
        final Entity testEntity2 = testEntity1.shallowClone();

        final ElementJoinComparator elementJoinComparator = new ElementJoinComparator();

        // When / Then
        assertTrue(elementJoinComparator.test(testEntity1, testEntity2));
    }

    @Test
    public void shouldStaySameWithUpdatedSet() {
        final Set<String> groupBys = Sets.newHashSet("one", "two");
        final ElementJoinComparator elementJoinComparator = new ElementJoinComparator(groupBys);

        groupBys.remove("two");

        assertThat(elementJoinComparator.getGroupByProperties()).containsExactly("one", "two");
    }

    @Test
    public void shouldBeEqualOnSameElementWithDifferentPropertyWhenNoGroupBys() {
        // Given
        final Entity testEntity1 = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        final Entity testEntity2 = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 5L)
                .build();

        final ElementJoinComparator elementJoinComparator = new ElementJoinComparator();

        // When / Then
        assertTrue(elementJoinComparator.test(testEntity1, testEntity2));
    }

    @Test
    public void shouldNotBeEqualOnDifferentElement() {
        // Given
        final Entity testEntity1 = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        final Entity testEntity2 = new Entity.Builder()
                .group(TestGroups.ENTITY_4)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        final ElementJoinComparator elementJoinComparator = new ElementJoinComparator();

        // When / Then
        assertFalse(elementJoinComparator.test(testEntity1, testEntity2));
    }

    @Test
    public void shouldNotBeEqualOnSameElementWithDifferentPropertyWithGroupBy() {
        // Given
        final Entity testEntity1 = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        final Entity testEntity2 = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 5L)
                .build();

        final ElementJoinComparator elementJoinComparator = new ElementJoinComparator("count");

        // When / Then
        assertFalse(elementJoinComparator.test(testEntity1, testEntity2));
    }
}
