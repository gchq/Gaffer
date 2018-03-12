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
package uk.gov.gchq.gaffer.store.operation.handler.compare;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;

import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class MaxHandlerTest {

    @Test
    public void shouldFindMaxBasedOnProperty() throws OperationException, JsonProcessingException {
        // Given
        final Entity entity1 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property", 1)
                .build();
        final Entity entity2 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property", 2)
                .build();
        final Entity entity3 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property", 3)
                .build();
        final Entity entity4 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property", 1)
                .build();

        final List<Entity> input = Lists.newArrayList(entity1, entity2, entity3, entity4);

        final Max max = new Max.Builder().input(input)
                .comparators(new ElementPropertyComparator.Builder()
                        .groups(TestGroups.ENTITY)
                        .property("property")
                        .build())
                .build();

        final MaxHandler handler = new MaxHandler();

        // When
        final Element result = handler.doOperation(max, null, null);

        // Then
        assertTrue(result instanceof Entity);
        assertEquals(3, result.getProperty("property"));
    }

    @Test
    public void shouldFindMaxBasedOnMultipleProperties() throws OperationException, JsonProcessingException {
        // Given
        final Entity entity1 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property1", 1)
                .property("property2", 1)
                .build();
        final Entity entity2 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property1", 1)
                .property("property2", 2)
                .build();
        final Entity entity3 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property1", 2)
                .property("property2", 2)
                .build();
        final Entity entity4 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property1", 2)
                .property("property2", 1)
                .build();

        final List<Entity> input = Lists.newArrayList(entity1, entity2, entity3, entity4);

        final Max max = new Max.Builder().input(input)
                .comparators(new ElementPropertyComparator.Builder()
                                .groups(TestGroups.ENTITY)
                                .property("property1")
                                .build(),
                        new ElementPropertyComparator.Builder()
                                .groups(TestGroups.ENTITY)
                                .property("property2")
                                .build())
                .build();

        final MaxHandler handler = new MaxHandler();

        // When
        final Element result = handler.doOperation(max, null, null);

        // Then
        assertSame(entity3, result);
    }

    @Test
    public void shouldFindMaxBasedOnPropertyWithMissingProperty() throws OperationException, JsonProcessingException {
        // Given
        final Entity entity1 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property1", 1)
                .build();
        final Entity entity2 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property1", 2)
                .build();
        final Entity entity3 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property1", 3)
                .build();
        final Entity entity4 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property2", 1)
                .build();
        final Entity entity5 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property2", 2)
                .build();

        final List<Entity> input = Lists.newArrayList(entity1, entity2, entity3, entity4, entity5);

        final Max max1 = new Max.Builder().input(input)
                .comparators(new ElementPropertyComparator.Builder()
                        .groups(TestGroups.ENTITY)
                        .property("property1")
                        .build())
                .build();

        final Max max2 = new Max.Builder().input(input)
                .comparators(new ElementPropertyComparator.Builder()
                        .groups(TestGroups.ENTITY)
                        .property("property2")
                        .build())
                .build();

        final MaxHandler handler = new MaxHandler();

        // When
        final Element result1 = handler.doOperation(max1, null, null);
        final Element result2 = handler.doOperation(max2, null, null);

        // Then
        assertTrue(result1 instanceof Entity);
        assertTrue(result2 instanceof Entity);
        assertEquals(3, result1.getProperty("property1"));
        assertEquals(2, result2.getProperty("property2"));
    }

    @Test
    public void shouldFindMaxBasedOnElement() throws OperationException {
        // Given
        final Entity entity1 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property1", 1)
                .property("property2", 1)
                .build();
        final Entity entity2 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property1", 2)
                .property("property2", 2)
                .build();
        final Entity entity3 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property1", 3)
                .property("property2", 3)
                .build();
        final Entity entity4 = new Entity.Builder().group(TestGroups.ENTITY)
                .property("property1", 4)
                .property("property2", 4)
                .build();

        final List<Entity> input = Lists.newArrayList(entity1, entity2, entity3, entity4);

        final Max max = new Max.Builder().input(input)
                .comparators(new SimpleElementComparator())
                .build();

        final MaxHandler handler = new MaxHandler();

        // When
        final Element result = handler.doOperation(max, null, null);

        // Then
        assertTrue(result instanceof Entity);
        assertEquals(4, result.getProperty("property1"));
        assertEquals(4, result.getProperty("property2"));
    }

    @Test
    public void shouldReturnNullIfOperationInputIsNull() throws OperationException {
        // Given
        final Max max = new Max.Builder().build();

        final MaxHandler handler = new MaxHandler();

        // When
        final Element result = handler.doOperation(max, null, null);

        // Then
        assertNull(result);
    }

    @Test
    public void shouldReturnNullIfBothComparatorsAreNull() throws OperationException {
        // Given
        final List<Entity> input = Lists.newArrayList();

        final Max max = new Max.Builder().input(input)
                .build();

        final MaxHandler handler = new MaxHandler();

        // When
        final Element result = handler.doOperation(max, null, null);

        // Then
        assertNull(result);
    }

    private static class SimpleElementComparator implements Comparator<Element> {
        @Override
        public int compare(final Element obj1, final Element obj2) {
            final int v1 = (int) obj1.getProperty("property1") * (int) obj1.getProperty("property2");
            final int v2 = (int) obj2.getProperty("property1") * (int) obj2.getProperty("property2");
            return v1 - v2;
        }
    }
}
