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
package uk.gov.gchq.gaffer.data.element.comparison;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.util.Comparator;
import java.util.function.Predicate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ElementPropertyComparatorTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldSerialiseAndDeserialiseEmptyComparator() throws SerialisationException, JsonProcessingException {
        // Given
        final ElementPropertyComparator comparator = new ElementPropertyComparator();

        // When
        byte[] json = serialiser.serialise(comparator, true);
        final ElementPropertyComparator deserialisedComparator = serialiser.deserialise(json, ElementPropertyComparator.class);

        // Then
        assertNotNull(deserialisedComparator);
    }

    @Test
    public void shouldSerialiseAndDeserialisePopulatedComparator() throws SerialisationException, JsonProcessingException {
        // Given
        final ElementComparator comparator = new ElementPropertyComparator.Builder()
                .groupName(TestGroups.ENTITY)
                .propertyName(TestPropertyNames.PROP_1)
                .comparator(new ComparatorImpl())
                .includeNulls(true)
                .reverse(true)
                .build();

        // When
        byte[] json = serialiser.serialise(comparator, true);
        final ElementPropertyComparator deserialisedComparator = serialiser.deserialise(json, ElementPropertyComparator.class);

        // Then
        assertNotNull(deserialisedComparator);
    }

    @Test
    public void shouldCompare() {
        // Given
        final ElementComparator comparator = new ElementPropertyComparator.Builder()
                .groupName(TestGroups.ENTITY)
                .propertyName(TestPropertyNames.PROP_1)
                .comparator(new ComparatorImpl())
                .build();

        final Entity smallEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 1)
                                                       .build();
        final Entity largeEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 2)
                                                       .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result, lessThan(0));
    }

    @Test
    public void shouldCompareWhenBothElementsHaveMissingProperties() {
        // Given
        final ElementComparator comparator = new ElementPropertyComparator.Builder()
                .groupName(TestGroups.ENTITY)
                .propertyName(TestPropertyNames.PROP_2)
                .comparator(new ComparatorImpl())
                .build();

        final Entity smallEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 1)
                                                       .build();
        final Entity largeEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 2)
                                                       .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result, equalTo(0));
    }

    @Test
    public void shouldCompareWhenFirstElementHasMissingProperties() {
        // Given
        final ElementComparator comparator = new ElementPropertyComparator.Builder()
                .groupName(TestGroups.ENTITY)
                .propertyName(TestPropertyNames.PROP_2)
                .comparator(new ComparatorImpl())
                .build();

        final Entity smallEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 1)
                                                       .build();
        final Entity largeEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_2, 2)
                                                       .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result, lessThan(0));
    }

    @Test
    public void shouldCompareWhenSecondElementHasMissingProperties() {
        // Given
        final ElementComparator comparator = new ElementPropertyComparator.Builder()
                .groupName(TestGroups.ENTITY)
                .propertyName(TestPropertyNames.PROP_2)
                .comparator(new ComparatorImpl())
                .build();

        final Entity smallEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_2, 1)
                                                       .build();
        final Entity largeEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 2)
                                                       .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result, greaterThan(0));
    }

    @Test
    public void shouldCompareWhenBothElementsHaveWrongGroup() {
        // Given
        final ElementComparator comparator = new ElementPropertyComparator.Builder()
                .groupName(TestGroups.ENTITY_2)
                .propertyName(TestPropertyNames.PROP_1)
                .comparator(new ComparatorImpl())
                .build();

        final Entity smallEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 1)
                                                       .build();
        final Entity largeEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 2)
                                                       .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result, lessThan(0));
    }

    @Test
    public void shouldCompareWhenFirstElementsHasWrongGroup() {
        // Given
        final ElementComparator comparator = new ElementPropertyComparator.Builder()
                .groupName(TestGroups.ENTITY_2)
                .propertyName(TestPropertyNames.PROP_1)
                .comparator(new ComparatorImpl())
                .build();

        final Entity smallEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 1)
                                                       .build();
        final Entity largeEntity = new Entity.Builder().group(TestGroups.ENTITY_2)
                                                       .property(TestPropertyNames.PROP_1, 2)
                                                       .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result, lessThan(0));
    }

    @Test
    public void shouldCompareWhenSecondElementsHasWrongGroup() {
        // Given
        final ElementComparator comparator = new ElementPropertyComparator.Builder()
                .groupName(TestGroups.ENTITY_2)
                .propertyName(TestPropertyNames.PROP_1)
                .comparator(new ComparatorImpl())
                .build();

        final Entity smallEntity = new Entity.Builder().group(TestGroups.ENTITY_2)
                                                       .property(TestPropertyNames.PROP_1, 1)
                                                       .build();
        final Entity largeEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 2)
                                                       .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result, lessThan(0));
    }

    @Test
    public void shouldCompareReversed() {
        // Given
        final ElementComparator comparator = new ElementPropertyComparator.Builder()
                .groupName(TestGroups.ENTITY)
                .propertyName(TestPropertyNames.PROP_1)
                .comparator(new ComparatorImpl())
                .reverse(true)
                .build();

        final Entity smallEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 1)
                                                       .build();
        final Entity largeEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 2)
                                                       .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result, greaterThan(0));
    }

    @Test
    public void shouldCompareWithNoProvidedComparatorInstance() {
        // Given
        final ElementComparator comparator = new ElementPropertyComparator.Builder()
                .groupName(TestGroups.ENTITY)
                .propertyName(TestPropertyNames.PROP_1)
                .build();

        final Entity smallEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 1)
                                                       .build();
        final Entity largeEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 2)
                                                       .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result, lessThan(0));
    }

    @Test
    public void shouldCreatePredicateWithCorrectBehaviour() {
        // Given
        final ElementPropertyComparator comparator = (ElementPropertyComparator) new ElementPropertyComparator.Builder()
                .groupName(TestGroups.ENTITY)
                .propertyName(TestPropertyNames.PROP_1)
                .comparator(new ComparatorImpl())
                .build();

        final Predicate<Element> predicate = comparator.asPredicate();

        final Entity smallEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 1)
                                                       .build();
        final Entity largeEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .property(TestPropertyNames.PROP_1, 2)
                                                       .build();

        // Then
        assertTrue(predicate.test(smallEntity));
        assertTrue(predicate.test(largeEntity));
    }

    private static class ComparatorImpl implements Comparator<Object> {

        @Override
        public int compare(final Object o1, final Object o2) {
            if (null == o1) {
                return (o2 == null) ? 0 : -1;
            } else if (null == o2) {
                return 1;
            }
            return ((Comparable) o1).compareTo(o2);
        }
    }
}
