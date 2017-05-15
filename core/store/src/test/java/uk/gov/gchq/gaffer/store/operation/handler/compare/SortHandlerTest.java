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
package uk.gov.gchq.gaffer.store.operation.handler.compare;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.comparison.ElementObjectComparator;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SortHandlerTest {

    @Test
    public void shouldSortBasedOnProperty() throws OperationException, JsonProcessingException {
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
                                                   .property("property", 4)
                                                   .build();

        final List<Entity> input = Lists.newArrayList(entity1, entity2, entity3, entity4);

        final Sort sort = new Sort.Builder().input(input)
                                            .comparator(new ElementPropertyComparator.Builder()
                                                    .groupName(TestGroups.ENTITY)
                                                    .propertyName("property")
                                                    .build())
                                            .build();

        final SortHandler handler = new SortHandler();

        // When
        final Iterable<Element> result = handler.doOperation(sort, null, null);

        // Then
        assertTrue(result instanceof CloseableIterable);

        int prev = Integer.MIN_VALUE;

        for (final Element element : result) {
            final int curr = (int) element.getProperty("property");
            assertTrue(curr > prev);
            prev = curr;
        }
    }

    @Test
    public void shouldSortBasedOnProperty_reversed() throws OperationException, JsonProcessingException {
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
                                                   .property("property", 4)
                                                   .build();

        final List<Entity> input = Lists.newArrayList(entity1, entity2, entity3, entity4);

        final Sort sort = new Sort.Builder().input(input)
                                            .comparator(new ElementPropertyComparator.Builder()
                                                    .groupName(TestGroups.ENTITY)
                                                    .propertyName("property")
                                                    .comparator(new PropertyComparatorImpl())
                                                    .reverse(true)
                                                    .build())
                                            .build();

        final SortHandler handler = new SortHandler();

        // When
        final Iterable<Element> result = handler.doOperation(sort, null, null);

        // Then
        assertTrue(result instanceof CloseableIterable);

        int prev = Integer.MAX_VALUE;

        for (final Element element : result) {
            final int curr = (int) element.getProperty("property");
            assertTrue(curr < prev);
            prev = curr;
        }
    }

    @Test
    public void shouldSortBasedOnPropertyExcludingNulls() throws OperationException, JsonProcessingException {
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
                                                   .build();

        final List<Entity> input = Lists.newArrayList(entity1, entity2, entity3, entity4);

        final Sort sort = new Sort.Builder().input(input)
                                            .comparator(new ElementPropertyComparator.Builder()
                                                    .propertyName("property")
                                                    .groupName(TestGroups.ENTITY)
                                                    .comparator(new PropertyComparatorImpl())
                                                    .build())
                                            .build();

        final SortHandler handler = new SortHandler();

        // When
        final Iterable<Element> result = handler.doOperation(sort, null, null);

        // Then
        assertTrue(result instanceof CloseableIterable);
        assertEquals(3, Iterables.size(result));

        int prev = Integer.MIN_VALUE;

        for (final Element element : result) {
            final int curr = (int) element.getProperty("property");
            assertTrue(curr > prev);
            prev = curr;
        }
    }

    @Test
    public void shouldSortBasedOnPropertyIncludingNulls() throws OperationException, JsonProcessingException {
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
                                                   .build();
        final Entity entity5 = new Entity.Builder().group(TestGroups.ENTITY)
                                                   .build();

        final List<Entity> input = Lists.newArrayList(entity1, entity2, entity3, entity4, entity5);

        final Sort sort = new Sort.Builder().input(input)
                                            .comparator(new ElementPropertyComparator.Builder()
                                                    .propertyName("property")
                                                    .groupName(TestGroups.ENTITY)
                                                    .comparator(new PropertyComparatorImpl())
                                                    .includeNulls(true)
                                                    .build())
                                            .build();

        final SortHandler handler = new SortHandler();

        // When
        final Iterable<Element> result = handler.doOperation(sort, null, null);

        // Then
        assertTrue(result instanceof CloseableIterable);
        assertEquals(5, Iterables.size(result));

        int prev = Integer.MIN_VALUE;
        int nullCount = 0;

        for (final Element element : result) {
            if (null != element.getProperty("property")) {
                final int curr = (int) element.getProperty("property");
                assertTrue(curr > prev);
                prev = curr;
            } else {
                nullCount++;
            }
        }

        assertEquals(2, nullCount);
    }

    @Test
    public void shouldReturnNullsLast() throws OperationException, JsonProcessingException {
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
                                                   .build();
        final Entity entity5 = new Entity.Builder().group(TestGroups.ENTITY)
                                                   .build();

        final List<Entity> input = Lists.newArrayList(entity1, entity2, entity3, entity4, entity5);

        final Sort sort = new Sort.Builder().input(input)
                                            .comparator(new ElementPropertyComparator.Builder()
                                                    .propertyName("property")
                                                    .groupName(TestGroups.ENTITY)
                                                    .comparator(new PropertyComparatorImpl())
                                                    .includeNulls(true)
                                                    .build())
                                            .build();

        final SortHandler handler = new SortHandler();

        // When
        final Iterable<Element> result = handler.doOperation(sort, null, null);

        final List<Element> resultList = Streams.toStream(result)
                                                .collect(Collectors.toList());

        // Then
        assertTrue(result instanceof CloseableIterable);
        assertEquals(5, Iterables.size(result));

        assertNull(Iterables.getLast(result).getProperty("property"));
        assertNotNull(Iterables.getFirst(result, null).getProperty("property"));
    }

    @Test
    public void shouldSortBasedOnElement() throws OperationException {
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

        final Sort sort = new Sort.Builder().input(input)
                                            .comparator(new ElementObjectComparator.Builder()
                                                    .comparator(new ElementComparatorImpl())
                                                    .build())
                                            .build();

        final SortHandler handler = new SortHandler();

        // When
        final Iterable<Element> result = handler.doOperation(sort, null, null);

        // Then
        assertTrue(result instanceof CloseableIterable);

        int prev = Integer.MIN_VALUE;

        for (final Element element : result) {
            final int curr = (int) element.getProperty("property1");
            assertTrue(curr > prev);
            prev = curr;
        }
    }

    @Test
    public void shouldSortBasedOnElement_reversed() throws OperationException {
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

        final Sort sort = new Sort.Builder().input(input)
                                            .comparator(new ElementObjectComparator.Builder()
                                                    .comparator(new ElementComparatorImpl())
                                                    .reverse(true)
                                                    .build())
                                            .build();

        final SortHandler handler = new SortHandler();

        // When
        final Iterable<Element> result = handler.doOperation(sort, null, null);

        // Then
        assertTrue(result instanceof CloseableIterable);

        int prev = Integer.MAX_VALUE;

        for (final Element element : result) {
            final int curr = (int) element.getProperty("property1");
            assertTrue(curr < prev);
            prev = curr;
        }
    }

    @Test
    public void shouldNotThrowExceptionIfIterableIsEmpty() throws OperationException {
        // Given
        final List<Entity> input = Lists.newArrayList();

        final Sort sort = new Sort.Builder().input(input)
                                            .comparator(new ElementPropertyComparator.Builder()
                                                    .groupName(TestGroups.ENTITY)
                                                    .propertyName("property")
                                                    .build())
                                            .build();

        final SortHandler handler = new SortHandler();

        // When
        final Iterable<Element> result = handler.doOperation(sort, null, null);

        // Then
        assertTrue(result instanceof CloseableIterable);
        assertTrue(Streams.toStream(result)
                          .collect(Collectors.toList())
                          .isEmpty());
    }

    @Test
    public void shouldReturnNullIfOperationInputIsNull() throws OperationException {
        // Given
        final Sort sort = new Sort.Builder().build();

        final SortHandler handler = new SortHandler();

        // When
        final Iterable<Element> result = handler.doOperation(sort, null, null);

        // Then
        assertNull(result);
    }

    @Test
    public void shouldReturnNullIfBothComparatorsAreNull() throws OperationException {
        // Given
        final List<Entity> input = Lists.newArrayList();

        final Sort sort = new Sort.Builder().input(input)
                                            .build();

        final SortHandler handler = new SortHandler();

        // When
        final Iterable<Element> result = handler.doOperation(sort, null, null);

        // Then
        assertNull(result);
    }

    @Test
    public void shouldSortLargeNumberOfElements() throws OperationException {
        // Given
        final int streamSize = 10000;

        final Stream<Element> stream = new Random().ints(streamSize)
                                                   .mapToObj(i -> new Entity.Builder()
                                                           .group(TestGroups.ENTITY)
                                                           .property("property", i)
                                                           .build());

        final Sort sort = new Sort.Builder().input(() -> stream.iterator())
                                            .comparator(new ElementPropertyComparator.Builder()
                                                    .groupName(TestGroups.ENTITY)
                                                    .propertyName("property")
                                                    .build())
                                            .build();

        final SortHandler handler = new SortHandler();

        // When
        final Iterable<Element> result = handler.doOperation(sort, null, null);

        // Then
        assertNotNull(result);
        assertEquals(streamSize, Iterables.size(result));
    }

    private static class ElementComparatorImpl implements Comparator<Element> {
        @Override
        public int compare(final Element o1, final Element o2) {
            final int v1 = (int) o1.getProperty("property1") * (int) o1.getProperty("property2");
            final int v2 = (int) o2.getProperty("property1") * (int) o2.getProperty("property2");
            return v1 - v2;
        }
    }

    private static class PropertyComparatorImpl implements Comparator<Object> {
        @Override
        public int compare(final Object o1, final Object o2) {
            return ((Integer) o1).compareTo((Integer) o2);
        }
    }
}
