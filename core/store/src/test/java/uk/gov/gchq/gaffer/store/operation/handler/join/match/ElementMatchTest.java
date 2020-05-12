/*
 * Copyright 2018-2020 Crown Copyright
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ElementMatchTest {

    @Test
    public void shouldFullyMatchEqualElementsWithNoGroupBy() {
        // Given
        final Entity testEntity = makeEntity(TestGroups.ENTITY_3, 3L);
        final List<Entity> comparisonEntityList = Arrays.asList(testEntity.shallowClone(), testEntity.shallowClone());

        ElementMatch elementMatch = new ElementMatch();
        elementMatch.init(comparisonEntityList);

        // When
        List<Element> matchingElements = elementMatch.matching(testEntity);

        // Then
        assertEquals(2, matchingElements.size());
        assertEquals(matchingElements, comparisonEntityList);
    }

    @Test
    public void shouldPartiallyMatchEqualElementsWithNoGroupBy() {
        // Given
        final Entity testEntity = makeEntity(TestGroups.ENTITY_3, 3L);
        final Entity testEntity2 = makeEntity(TestGroups.ENTITY_4, 3L);

        List<Entity> comparisonEntityList = Arrays.asList(testEntity.shallowClone(), testEntity2.shallowClone());

        ElementMatch elementMatch = new ElementMatch();
        elementMatch.init(comparisonEntityList);

        // When
        List<Element> matchingElements = elementMatch.matching(testEntity);

        // Then
        assertEquals(1, matchingElements.size());
        assertEquals(matchingElements.get(0), testEntity);
    }

    @Test
    public void shouldGiveNoMatchForNonEqualElementsWithNoGroupBy() {
        // Given
        final Entity testEntity = makeEntity(TestGroups.ENTITY_3, 3L);
        final Entity testEntity2 = makeEntity(TestGroups.ENTITY_4, 3L);

        List<Entity> comparisonEntityList = Arrays.asList(testEntity2.shallowClone(), testEntity2.shallowClone());

        ElementMatch elementMatch = new ElementMatch();
        elementMatch.init(comparisonEntityList);

        // When
        List<Element> matchingElements = elementMatch.matching(testEntity);

        // Then
        assertEquals(0, matchingElements.size());
    }

    @Test
    public void shouldThrowExceptionIfInitialisedWithNullValue() {
        // Given
        ElementMatch elementMatch = new ElementMatch();

        // When / Then
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> elementMatch.init(null));
        assertEquals("ElementMatch must be initialised with non-null match candidates", exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfNotInitialised() {
        // Given
        final ElementMatch elementMatch = new ElementMatch();

        // When / Then
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> elementMatch.matching(new Entity("testGroup", "test")));
        assertEquals("ElementMatch must be initialised with non-null match candidates", exception.getMessage());
    }


    @Test
    public void shouldFullyMatchEqualElementsWithGroupBy() {
        // Given
        final Entity testEntity = makeEntity(TestGroups.ENTITY_3, 3L);
        List<Entity> comparisonEntityList = Arrays.asList(testEntity.shallowClone(), testEntity.shallowClone());

        ElementMatch elementMatch = new ElementMatch("count");
        elementMatch.init(comparisonEntityList);

        // When
        List<Element> matchingElements = elementMatch.matching(testEntity);

        // Then
        assertEquals(2, matchingElements.size());
        assertEquals(matchingElements, comparisonEntityList);
    }

    @Test
    public void shouldPartiallyMatchEqualElementsWithGroupBy() {
        // Given
        final Entity testEntity = makeEntity(TestGroups.ENTITY_3, 3L);
        final Entity testEntity2 = makeEntity(TestGroups.ENTITY_3, 5L);

        List<Entity> comparisonEntityList = Arrays.asList(testEntity.shallowClone(), testEntity2.shallowClone());

        ElementMatch elementMatch = new ElementMatch("count");
        elementMatch.init(comparisonEntityList);

        // When
        List<Element> matchingElements = elementMatch.matching(testEntity);

        // Then
        assertEquals(1, matchingElements.size());
        assertEquals(matchingElements.get(0), testEntity);
    }

    @Test
    public void shouldGiveNoMatchForEqualElementsWithGroupBy() {
        // Given
        final Entity testEntity = makeEntity(TestGroups.ENTITY_3, 3L);
        final Entity testEntity2 = makeEntity(TestGroups.ENTITY_3, 5L);
        final Entity testEntity3 = makeEntity(TestGroups.ENTITY_3, 7L);


        List<Entity> comparisonEntityList = Arrays.asList(testEntity2.shallowClone(), testEntity3.shallowClone());

        ElementMatch elementMatch = new ElementMatch("count");
        elementMatch.init(comparisonEntityList);

        // When
        List<Element> matchingElements = elementMatch.matching(testEntity);

        // Then
        assertEquals(0, matchingElements.size());
    }

    private Entity makeEntity(final String groupName, long countProperty) {
        return new Entity.Builder()
                .group(groupName)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, countProperty)
                .build();
    }
}
