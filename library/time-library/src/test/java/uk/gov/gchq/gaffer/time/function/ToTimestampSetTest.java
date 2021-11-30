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

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.time.BoundedTimestampSet;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.gaffer.time.TimestampSet;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;


class ToTimestampSetTest extends FunctionTest<ToTimestampSet> {
    private static final Long TEST_TIMESTAMP = Instant.now().toEpochMilli();

    @Test
    public void shouldCreateEmptySetWhenNull() {
        // Given
        final ToTimestampSet toTimestampSet =
                new ToTimestampSet(CommonTimeUtil.TimeBucket.DAY, false);
        // When
        TimestampSet result = toTimestampSet.apply(null);

        // Then
        TimestampSet expected = new RBMBackedTimestampSet.Builder()
                .timeBucket(CommonTimeUtil.TimeBucket.DAY)
                .build();

        assertEquals(expected, result);
    }

    @Test
    public void shouldCreateBoundedSet() {
        // Given
        final ToTimestampSet toTimestampSet =
                new ToTimestampSet(CommonTimeUtil.TimeBucket.DAY, 10);
        // When
        TimestampSet result = toTimestampSet.apply(TEST_TIMESTAMP);

        // Then
        TimestampSet expected = new BoundedTimestampSet.Builder()
                .timeBucket(CommonTimeUtil.TimeBucket.DAY)
                .maxSize(10)
                .build();
        expected.add(Instant.ofEpochMilli(TEST_TIMESTAMP));

        assertEquals(expected, result);
    }

    @Test
    public void shouldCreateNonBoundedSet() {
        // Given
        final ToTimestampSet toTimestampSet =
                new ToTimestampSet(CommonTimeUtil.TimeBucket.DAY, false);
        // When
        TimestampSet result = toTimestampSet.apply(TEST_TIMESTAMP);

        // Then
        TimestampSet expected = new RBMBackedTimestampSet.Builder()
                .timeBucket(CommonTimeUtil.TimeBucket.DAY)
                .build();
        expected.add(Instant.ofEpochMilli(TEST_TIMESTAMP));

        assertEquals(expected, result);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Long.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{TimestampSet.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final ToTimestampSet toTimestampSet =
                new ToTimestampSet(CommonTimeUtil.TimeBucket.DAY, false);
        // When
        final String json = new String(JSONSerialiser.serialise(toTimestampSet));
        ToTimestampSet deserialisedToTimestampSet = JSONSerialiser.deserialise(json, ToTimestampSet.class);
        // Then
        assertEquals(toTimestampSet, deserialisedToTimestampSet);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.time.function.ToTimestampSet\",\"bucket\":\"DAY\",\"millisCorrection\":1}", json);
    }

    @Override
    protected ToTimestampSet getInstance() {
        return new ToTimestampSet();
    }

    @Override
    protected Iterable<ToTimestampSet> getDifferentInstancesOrNull() {
        return null;
    }
}
