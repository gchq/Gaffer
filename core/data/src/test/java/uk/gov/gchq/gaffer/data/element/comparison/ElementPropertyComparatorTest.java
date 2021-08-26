/*
 * Copyright 2017-2021 Crown Copyright
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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ElementPropertyComparatorTest extends JSONSerialisationTest<ElementPropertyComparator> {

    @Test
    public void shouldSerialiseAndDeserialisePopulatedComparator() throws SerialisationException {
        // Given
        final ElementPropertyComparator comparator = new ElementPropertyComparator.Builder()
                .groups(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1)
                .comparator(new ComparatorImpl())
                .build();

        // When
        final byte[] json = JSONSerialiser.serialise(comparator, true);
        final ElementPropertyComparator deserialisedComparator = JSONSerialiser.deserialise(json, ElementPropertyComparator.class);

        // Then
        assertNotNull(deserialisedComparator);
    }

    @Test
    public void shouldCompare() {
        // Given
        final Entity smallEntity = makeEntity(TestGroups.ENTITY, TestPropertyNames.PROP_1, 1);
        final Entity largeEntity = makeEntity(TestGroups.ENTITY, TestPropertyNames.PROP_1, 2);

        final ElementPropertyComparator comparator = new ElementPropertyComparator.Builder()
                .groups(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1)
                .comparator(new ComparatorImpl())
                .build();


        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result).isNegative();
    }

    @Test
    public void shouldCompareWhenBothElementsHaveMissingProperties() {
        // Given
        final Entity smallEntity = makeEntity(TestGroups.ENTITY, TestPropertyNames.PROP_1, 1);
        final Entity largeEntity = makeEntity(TestGroups.ENTITY, TestPropertyNames.PROP_1, 2);

        final ElementPropertyComparator comparator = new ElementPropertyComparator.Builder()
                .groups(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_2)
                .comparator(new ComparatorImpl())
                .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result).isZero();
    }

    @Test
    public void shouldCompareWhenFirstElementHasMissingProperties() {
        // Given
        final Entity smallEntity = makeEntity(TestGroups.ENTITY, TestPropertyNames.PROP_1, 1);
        final Entity largeEntity = makeEntity(TestGroups.ENTITY, TestPropertyNames.PROP_2, 2);

        final ElementPropertyComparator comparator = new ElementPropertyComparator.Builder()
                .groups(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_2)
                .comparator(new ComparatorImpl())
                .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result).isPositive();
    }

    @Test
    public void shouldCompareWhenSecondElementHasMissingProperties() {
        // Given
        final Entity smallEntity = makeEntity(TestGroups.ENTITY, TestPropertyNames.PROP_2, 1);
        final Entity largeEntity = makeEntity(TestGroups.ENTITY, TestPropertyNames.PROP_1, 2);

        final ElementPropertyComparator comparator = new ElementPropertyComparator.Builder()
                .groups(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_2)
                .comparator(new ComparatorImpl())
                .build();


        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result).isNegative();
    }

    @Test
    public void shouldCompareWhenBothElementsHaveWrongGroup() {
        // Given
        final Entity smallEntity = makeEntity(TestGroups.ENTITY, TestPropertyNames.PROP_1, 1);
        final Entity largeEntity = makeEntity(TestGroups.ENTITY, TestPropertyNames.PROP_1, 2);

        final ElementPropertyComparator comparator = new ElementPropertyComparator.Builder()
                .groups(TestGroups.ENTITY_2)
                .property(TestPropertyNames.PROP_1)
                .comparator(new ComparatorImpl())
                .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result).isZero();
    }

    @Test
    public void shouldCompareWhenFirstElementsHasWrongGroup() {
        // Given
        final Entity smallEntity = makeEntity(TestGroups.ENTITY, TestPropertyNames.PROP_1, 1);
        final Entity largeEntity = makeEntity(TestGroups.ENTITY_2, TestPropertyNames.PROP_1, 2);

        final ElementPropertyComparator comparator = new ElementPropertyComparator.Builder()
                .groups(TestGroups.ENTITY_2)
                .property(TestPropertyNames.PROP_1)
                .comparator(new ComparatorImpl())
                .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result).isPositive();
    }

    @Test
    public void shouldCompareWhenSecondElementsHasWrongGroup() {
        // Given
        final Entity smallEntity = makeEntity(TestGroups.ENTITY_2, TestPropertyNames.PROP_1, 1);
        final Entity largeEntity = makeEntity(TestGroups.ENTITY, TestPropertyNames.PROP_1, 2);

        final ElementPropertyComparator comparator = new ElementPropertyComparator.Builder()
                .groups(TestGroups.ENTITY_2)
                .property(TestPropertyNames.PROP_1)
                .comparator(new ComparatorImpl())
                .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result).isNegative();
    }

    @Test
    public void shouldCompareReversed() {
        // Given
        final Entity smallEntity = makeEntity(TestGroups.ENTITY, TestPropertyNames.PROP_1, 1);
        final Entity largeEntity = makeEntity(TestGroups.ENTITY, TestPropertyNames.PROP_1, 2);

        final ElementComparator comparator = new ElementPropertyComparator.Builder()
                .groups(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1)
                .comparator(new ComparatorImpl())
                .reverse(true)
                .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result).isPositive();
    }

    @Test
    public void shouldCompareWithNoProvidedComparatorInstance() {
        // Given
        final Entity smallEntity = makeEntityWithPropertyValueAsPrimitiveInt(1);
        final Entity largeEntity = makeEntityWithPropertyValueAsPrimitiveInt(2);

        final ElementPropertyComparator comparator = new ElementPropertyComparator.Builder()
                .groups(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1)
                .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result).isNegative();
    }

    private Entity makeEntityWithPropertyValueAsPrimitiveInt(final int propertyValue) {
        return new Entity.Builder().group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, propertyValue)
                .build();
    }

    private Entity makeEntity(final String entityGroup, final String propertyName, final int propertyValue) {
        return new Entity.Builder().group(entityGroup)
                .property(propertyName, new IntegerWrapper(propertyValue))
                .build();
    }

    @Override
    protected ElementPropertyComparator getTestObject() {
        return new ElementPropertyComparator();
    }

    private static class IntegerWrapper {
        private Integer field;

        IntegerWrapper(final Integer field) {
            this.field = field;
        }
    }

    private static class ComparatorImpl implements Comparator<IntegerWrapper> {

        @Override
        public int compare(final IntegerWrapper o1, final IntegerWrapper o2) {
            if (null == o1) {
                return (o2 == null) ? 0 : -1;
            } else if (null == o2) {
                return 1;
            }
            return (o1.field).compareTo(o2.field);
        }
    }
}
