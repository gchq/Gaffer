/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.function.filter;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.FilterFunctionTest;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class AgeOffFromDaysTest extends FilterFunctionTest {
    public static final int MINUTE_IN_MILLISECONDS = 60000;
    public static final int DAY_IN_MILLISECONDS = 24 * 60 * 60 * 1000;
    public static final int AGE_OFF_DAYS = 14;
    public static final long AGE_OFF_MILLISECONDS = AGE_OFF_DAYS * DAY_IN_MILLISECONDS;

    @Test
    public void shouldAcceptWhenWithinAgeOffLimit() {
        // Given
        final AgeOffFromDays filter = new AgeOffFromDays();

        // When
        final Object[] input = new Object[]{System.currentTimeMillis() - AGE_OFF_MILLISECONDS + MINUTE_IN_MILLISECONDS, AGE_OFF_DAYS};
        final boolean accepted = filter.isValid(input);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptWhenOutsideAgeOffLimit() {
        // Given
        final AgeOffFromDays filter = new AgeOffFromDays();

        // When
        final Object[] input = new Object[]{System.currentTimeMillis() - AGE_OFF_MILLISECONDS - DAY_IN_MILLISECONDS, AGE_OFF_DAYS};
        final boolean accepted = filter.isValid(input);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldNotAcceptWhenInputIsNull() {
        // Given
        final AgeOffFromDays filter = new AgeOffFromDays();

        // When
        final Object[] input = null;
        final boolean accepted = filter.isValid(input);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldNotAcceptWhenInputIsEmpty() {
        // Given
        final AgeOffFromDays filter = new AgeOffFromDays();

        // When
        final Object[] input = new Object[]{};
        final boolean accepted = filter.isValid(input);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldNotAcceptWhenInputIsIncorrectSize() {
        // Given
        final AgeOffFromDays filter = new AgeOffFromDays();

        // When
        final Object[] smallInput = new Object[]{""};
        final Object[] largeInput = new Object[]{"", "", ""};

        final boolean acceptedSmall = filter.isValid(smallInput);
        final boolean acceptedLarge = filter.isValid(largeInput);

        // Then
        assertFalse(acceptedSmall);
        assertFalse(acceptedLarge);
    }

    @Test
    public void shouldNotAcceptWhenTimestampIsNull() {
        // Given
        final AgeOffFromDays filter = new AgeOffFromDays();

        // When
        final Object[] input = new Object[]{null, 0};

        final boolean accepted = filter.isValid(input);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldNotAcceptWhenDaysIsNull() {
        // Given
        final AgeOffFromDays filter = new AgeOffFromDays();

        // When
        final Object[] input = new Object[]{0L, null};

        final boolean accepted = filter.isValid(input);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldClone() {
        // Given
        final AgeOffFromDays filter = new AgeOffFromDays();

        // When
        final AgeOffFromDays clonedFilter = filter.statelessClone();

        // Then
        assertNotSame(filter, clonedFilter);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final AgeOffFromDays filter = new AgeOffFromDays();

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.AgeOffFromDays\"%n" +
                "}"), json);

        // When 2
        final AgeOffFromDays deserialisedFilter = new JSONSerialiser().deserialise(json
                .getBytes(), AgeOffFromDays.class);

        // Then 2
        assertNotNull(deserialisedFilter);
    }

    @Override
    protected Class<AgeOffFromDays> getFunctionClass() {
        return AgeOffFromDays.class;
    }

    @Override
    protected AgeOffFromDays getInstance() {
        return new AgeOffFromDays();
    }
}
