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
package uk.gov.gchq.gaffer.time.serialisation;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RBMBackedTimestampSetSerialiserTest extends ToBytesSerialisationTest<RBMBackedTimestampSet> {

    @Test
    public void testSerialiser() throws SerialisationException {
        // Given
        final RBMBackedTimestampSet rbmBackedTimestampSet = getExampleValue();

        // When
        final byte[] serialised = serialiser.serialise(rbmBackedTimestampSet);
        final RBMBackedTimestampSet deserialised = serialiser.deserialise(serialised);

        // Then
        assertEquals(rbmBackedTimestampSet, deserialised);
    }

    private RBMBackedTimestampSet getExampleValue() {
        final RBMBackedTimestampSet rbmBackedTimestampSet = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.SECOND);
        rbmBackedTimestampSet.add(Instant.ofEpochMilli(1000L));
        rbmBackedTimestampSet.add(Instant.ofEpochMilli(1000000L));
        return rbmBackedTimestampSet;
    }

    @Test
    public void testCanHandle() throws SerialisationException {
        assertTrue(serialiser.canHandle(RBMBackedTimestampSet.class));
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void testSerialisationSizesQuotedInJavadoc() throws SerialisationException {
        // Given
        final Random random = new Random(123456789L);
        final Instant instant = ZonedDateTime.of(2017, 1, 1, 1, 1, 1, 0, ZoneId.of("UTC")).toInstant();
        // Set of 100 minutes in a day and the time bucket is a minute
        final RBMBackedTimestampSet rbmBackedTimestampSet1 = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.MINUTE);
        IntStream.range(0, 100)
                .forEach(i -> rbmBackedTimestampSet1.add(instant.plusSeconds(random.nextInt(24 * 60) * 60)));
        // Set of every minute in a year and the time bucket is a minute
        final RBMBackedTimestampSet rbmBackedTimestampSet2 = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.MINUTE);
        IntStream.range(0, 365 * 24 * 60)
                .forEach(i -> rbmBackedTimestampSet2.add(instant.plusSeconds(i * 60)));
        // Set of every second in a year is set and the time bucket is a second
        final RBMBackedTimestampSet rbmBackedTimestampSet3 = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.SECOND);
        IntStream.range(0, 365 * 24 * 60 * 60)
                .forEach(i -> rbmBackedTimestampSet3.add(instant.plusSeconds(i)));

        // When
        final int lengthSet1 = serialiser.serialise(rbmBackedTimestampSet1).length;
        final int lengthSet2 = serialiser.serialise(rbmBackedTimestampSet2).length;
        final int lengthSet3 = serialiser.serialise(rbmBackedTimestampSet3).length;

        // Then
        assertTrue(200 < lengthSet1 && lengthSet1 < 220);
        assertTrue(72000 < lengthSet2 && lengthSet2 < 74000);
        assertTrue(3900000 < lengthSet3 && lengthSet3 < 4100000);
    }

    @Override
    public Serialiser<RBMBackedTimestampSet, byte[]> getSerialisation() {
        return new RBMBackedTimestampSetSerialiser();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Pair<RBMBackedTimestampSet, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{new Pair(getExampleValue(), new byte[]{0, 58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 16, 0, 0, 0, 1, 0, -24, 3})};

    }
}
