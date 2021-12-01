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
import uk.gov.gchq.gaffer.time.CommonTimeUtil;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DateToTimeBucketStartTest extends FunctionTest<DateToTimeBucketStart> {
    private static final Date CURRENT_DATE = java.util.Calendar.getInstance().getTime();

    @Test
    public void shouldConvertDateToStartOfTimeBucket() {
        // Given
        final DateToTimeBucketStart dateToTimeBucketStart = new DateToTimeBucketStart();
        dateToTimeBucketStart.setBucket(CommonTimeUtil.TimeBucket.DAY);
        // When
        Date result = dateToTimeBucketStart.apply(CURRENT_DATE);
        long days = TimeUnit.MILLISECONDS.toDays(CURRENT_DATE.getTime());
        long daysToMilliRounded = days * (1000 * 60 * 60 * 24);
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
        final DateToTimeBucketStart dateToTimeBucketStart =
                new DateToTimeBucketStart();
        // When
        final String json = new String(JSONSerialiser.serialise(dateToTimeBucketStart));
        DateToTimeBucketStart deserialisedDateToTimeBucketStart = JSONSerialiser.deserialise(json, DateToTimeBucketStart.class);
        // Then
        assertEquals(dateToTimeBucketStart, deserialisedDateToTimeBucketStart);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.time.function.DateToTimeBucketStart\"}", json);
    }

    @Override
    protected DateToTimeBucketStart getInstance() {
        return new DateToTimeBucketStart();
    }

    @Override
    protected Iterable<DateToTimeBucketStart> getDifferentInstancesOrNull() {
        return null;
    }
}
