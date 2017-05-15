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
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.util.Comparator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class ElementObjectComparatorTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldSerialiseAndDeserialiseEmptyComparator() throws SerialisationException, JsonProcessingException {
        // Given
        final ElementObjectComparator comparator = new ElementObjectComparator();

        // When
        byte[] json = serialiser.serialise(comparator, true);
        final ElementObjectComparator deserialisedComparator = serialiser.deserialise(json, ElementObjectComparator.class);

        // Then
        assertNotNull(deserialisedComparator);
    }

    @Test
    public void shouldSerialiseAndDeserialisePopulatedComparator() throws SerialisationException, JsonProcessingException {
        // Given
        final ElementComparator comparator = new ElementObjectComparator.Builder()
                .comparator(new ComparatorImpl())
                .includeNulls(true)
                .reverse(true)
                .build();

        // When
        byte[] json = serialiser.serialise(comparator, true);
        final ElementObjectComparator deserialisedComparator = serialiser.deserialise(json, ElementObjectComparator.class);

        // Then
        assertNotNull(deserialisedComparator);
    }

    @Test
    public void shouldCompare() {
        // Given
        final ElementComparator comparator = new ElementObjectComparator.Builder()
                .comparator(new ComparatorImpl())
                .build();

        final Entity smallEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .build();
        final Entity largeEntity = new Entity.Builder().group(TestGroups.ENTITY_2)
                                                       .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result, lessThan(0));
    }

    @Test
    public void shouldCompareReversed() {
        // Given
        final ElementComparator comparator = new ElementObjectComparator.Builder()
                .comparator(new ComparatorImpl())
                .reverse(true)
                .build();

        final Entity smallEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .build();
        final Entity largeEntity = new Entity.Builder().group(TestGroups.ENTITY_2)
                                                       .build();

        // When
        final int result = comparator.compare(smallEntity, largeEntity);

        // Then
        assertThat(result, greaterThan(0));
    }

    @Test
    public void shouldThrowExceptionIfNoComparatorProvided() {
        // Given
        final ElementComparator comparator = new ElementObjectComparator.Builder()
                .build();

        final Entity smallEntity = new Entity.Builder().group(TestGroups.ENTITY)
                                                       .build();
        final Entity largeEntity = new Entity.Builder().group(TestGroups.ENTITY_2)
                                                       .build();

        // When
        try {
            final int result = comparator.compare(smallEntity, largeEntity);
            fail("Was expecting exception.");
        } catch (final Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
        }
    }

    private static class ComparatorImpl implements Comparator<Element> {

        @Override
        public int compare(final Element obj1, final Element obj2) {
            return obj1.getGroup().compareTo(obj2.getGroup());
        }
    }
}
