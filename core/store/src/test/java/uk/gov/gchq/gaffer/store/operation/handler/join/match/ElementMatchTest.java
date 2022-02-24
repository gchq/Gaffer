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

package uk.gov.gchq.gaffer.store.operation.handler.join.match;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class ElementMatchTest {

    @Test
    public void shouldFullyMatchEqualElementsWithNoGroupBy() {
        // Given
        Entity testEntity = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        List<Entity> comparisonEntityList = Arrays.asList(testEntity.shallowClone(), testEntity.shallowClone());

        ElementMatch elementMatch = new ElementMatch();
        elementMatch.init(comparisonEntityList);

        // When
        List<Element> matchingElements = elementMatch.matching(testEntity);

        // Then
        assertThat(matchingElements)
                .hasSize(2)
                .isEqualTo(comparisonEntityList);
    }

    @Test
    public void shouldPartiallyMatchEqualElementsWithNoGroupBy() {
        // Given
        Entity testEntity = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        Entity testEntity2 = new Entity.Builder()
                .group(TestGroups.ENTITY_4)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        List<Entity> comparisonEntityList = Arrays.asList(testEntity.shallowClone(), testEntity2.shallowClone());

        ElementMatch elementMatch = new ElementMatch();
        elementMatch.init(comparisonEntityList);

        // When
        List<Element> matchingElements = elementMatch.matching(testEntity);

        // Then
        assertThat(matchingElements).hasSize(1);
        assertThat(matchingElements.get(0)).isEqualTo(testEntity);
    }

    @Test
    public void shouldGiveNoMatchForNonEqualElementsWithNoGroupBy() {
        // Given
        Entity testEntity = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        Entity testEntity2 = new Entity.Builder()
                .group(TestGroups.ENTITY_4)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        List<Entity> comparisonEntityList = Arrays.asList(testEntity2.shallowClone(), testEntity2.shallowClone());

        ElementMatch elementMatch = new ElementMatch();
        elementMatch.init(comparisonEntityList);

        // When
        List<Element> matchingElements = elementMatch.matching(testEntity);

        // Then
        assertThat(matchingElements).isEmpty();
    }

    @Test
    public void shouldThrowExceptionIfInitialisedWithNullValue() {
        // Given

        ElementMatch elementMatch = new ElementMatch();

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> elementMatch.init(null))
                .withMessage("ElementMatch must be initialised with non-null match candidates");
    }

    @Test
    public void shouldThrowExceptionIfNotInitialised() {
        // Given

        ElementMatch elementMatch = new ElementMatch();

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> elementMatch.matching(new Entity("testGroup", "test")))
                .withMessage("ElementMatch must be initialised with non-null match candidates");
    }


    @Test
    public void shouldFullyMatchEqualElementsWithGroupBy() {
        // Given
        Entity testEntity = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        List<Entity> comparisonEntityList = Arrays.asList(testEntity.shallowClone(), testEntity.shallowClone());

        ElementMatch elementMatch = new ElementMatch("count");
        elementMatch.init(comparisonEntityList);

        // When
        List<Element> matchingElements = elementMatch.matching(testEntity);

        // Then
        assertThat(matchingElements)
                .hasSize(2)
                .isEqualTo(comparisonEntityList);
    }

    @Test
    public void shouldPartiallyMatchEqualElementsWithGroupBy() {
        // Given
        Entity testEntity = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        Entity testEntity2 = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 5L)
                .build();

        List<Entity> comparisonEntityList = Arrays.asList(testEntity.shallowClone(), testEntity2.shallowClone());

        ElementMatch elementMatch = new ElementMatch("count");
        elementMatch.init(comparisonEntityList);

        // When
        List<Element> matchingElements = elementMatch.matching(testEntity);

        // Then
        assertThat(matchingElements).hasSize(1);
        assertThat(matchingElements.get(0)).isEqualTo(testEntity);
    }

    @Test
    public void shouldGiveNoMatchForEqualElementsWithGroupBy() {
        // Given
        Entity testEntity = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        Entity testEntity2 = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 5L)
                .build();

        Entity testEntity3 = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, 7L)
                .build();


        List<Entity> comparisonEntityList = Arrays.asList(testEntity2.shallowClone(), testEntity3.shallowClone());

        ElementMatch elementMatch = new ElementMatch("count");
        elementMatch.init(comparisonEntityList);

        // When
        List<Element> matchingElements = elementMatch.matching(testEntity);

        // Then
        assertThat(matchingElements).isEmpty();
    }
}
