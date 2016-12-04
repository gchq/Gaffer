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
package uk.gov.gchq.gaffer.function.simple.filter;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.FilterFunctionTest;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class AgeOffFromDaysTest extends FilterFunctionTest {
    public static final int MINUTE_IN_MILLISECONDS = 60000;
    public static final int DAY_IN_MILLISECONDS = 24 * 60 * 60 * 1000;
    public static final int CUSTOM_AGE_OFF_DAYS = 14;
    public static final long CUSTOM_AGE_OFF_MILLISECONDS = CUSTOM_AGE_OFF_DAYS * DAY_IN_MILLISECONDS;

    @Test
    public void shouldUseDefaultAgeOffTime() {
        // Given
        final AgeOffFromDays filter = new AgeOffFromDays();

        // When
        final long ageOffTime = filter.getAgeOffTime();

        // Then
        assertEquals(AgeOffFromDays.AGE_OFF_TIME_DEFAULT, ageOffTime);
    }

    @Test
    public void shouldSetAgeOffInWeeks() {
        // Given
        final int ageOffInWeeks = 1;
        final AgeOffFromDays filter = new AgeOffFromDays();
        filter.setAgeOffWeeks(ageOffInWeeks);

        // When
        final long ageOffTime = filter.getAgeOffTime();

        // Then
        assertEquals(ageOffInWeeks * 7, ageOffTime);
    }

    @Test
    public void shouldSetAgeOffInHoursUnder() {
        // Given
        final int ageOffInHours = 10;
        final AgeOffFromDays filter = new AgeOffFromDays();
        filter.setAgeOffHours(ageOffInHours);

        // When
        final long ageOffTime = filter.getAgeOffTime();

        // Then
        assertEquals(1, ageOffTime);
    }

    @Test
    public void shouldSetAgeOffInHoursOver() {
        // Given
        final int ageOffInHours = 25;
        final AgeOffFromDays filter = new AgeOffFromDays();
        filter.setAgeOffHours(ageOffInHours);

        // When
        final long ageOffTime = filter.getAgeOffTime();

        // Then
        assertEquals(2, ageOffTime);
    }

    @Test
    public void shouldAcceptWhenWithinAgeOffLimit() {
        // Given
        final AgeOffFromDays filter = new AgeOffFromDays(CUSTOM_AGE_OFF_DAYS);

        // When
        final Object[] input = new Object[]{System.currentTimeMillis() - CUSTOM_AGE_OFF_MILLISECONDS + MINUTE_IN_MILLISECONDS, CUSTOM_AGE_OFF_DAYS};
        final boolean accepted = filter.isValid(input);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptWhenOutsideAgeOffLimit() {
        // Given
        final AgeOffFromDays filter = new AgeOffFromDays(CUSTOM_AGE_OFF_DAYS);

        // When
        final Object[] input = new Object[]{System.currentTimeMillis() - CUSTOM_AGE_OFF_MILLISECONDS - DAY_IN_MILLISECONDS, CUSTOM_AGE_OFF_DAYS};
        final boolean accepted = filter.isValid(input);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldClone() {
        // Given
        final AgeOffFromDays filter = new AgeOffFromDays(CUSTOM_AGE_OFF_DAYS);

        // When
        final AgeOffFromDays clonedFilter = filter.statelessClone();

        // Then
        assertNotSame(filter, clonedFilter);
        assertNotSame(CUSTOM_AGE_OFF_DAYS, clonedFilter.getAgeOffTime());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final AgeOffFromDays filter = new AgeOffFromDays(CUSTOM_AGE_OFF_DAYS);

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.simple.filter.AgeOffFromDays\",%n" +
                "  \"ageOffTime\" : 14%n" +
                "}"), json);

        // When 2
        final AgeOffFromDays deserialisedFilter = new JSONSerialiser().deserialise(json
                .getBytes(), AgeOffFromDays.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(CUSTOM_AGE_OFF_DAYS, deserialisedFilter.getAgeOffTime());
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
