/*
 * Copyright 2021 Crown Copyright
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

package uk.gov.gchq.gaffer.time.function;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.time.CommonTimeUtil.TimeBucket;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DateToTimeBucketEndTest extends FunctionTest<DateToTimeBucketEnd> {
    private static final Date CURRENT_DATE = java.util.Calendar.getInstance().getTime();

    @Test
    public void shouldConvertDateToEndOfTimeBucket() {
        // Given
        final DateToTimeBucketEnd dateToTimeBucketEnd = new DateToTimeBucketEnd();
        dateToTimeBucketEnd.setBucket(TimeBucket.DAY);
        // When
        Date result = dateToTimeBucketEnd.apply(CURRENT_DATE);
        long days = TimeUnit.MILLISECONDS.toDays(CURRENT_DATE.getTime());
        long daysToMilliRounded = (days + 1) * (1000 * 60 * 60 * 24) - 1;
        Date expected = new Date(new Timestamp(daysToMilliRounded).getTime());
        // Then
        assertEquals(expected, result);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Date.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{Date.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final DateToTimeBucketEnd dateToTimeBucketEnd =
                new DateToTimeBucketEnd();
        // When
        final String json = new String(JSONSerialiser.serialise(dateToTimeBucketEnd));
        DateToTimeBucketEnd deserialisedDateToTimeBucketEnd = JSONSerialiser.deserialise(json, DateToTimeBucketEnd.class);
        // Then
        assertEquals(dateToTimeBucketEnd, deserialisedDateToTimeBucketEnd);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.time.function.DateToTimeBucketEnd\"}", json);
    }

    @Override
    protected DateToTimeBucketEnd getInstance() {
        return new DateToTimeBucketEnd();
    }

    @Override
    protected Iterable<DateToTimeBucketEnd> getDifferentInstancesOrNull() {
        return null;
    }
}
