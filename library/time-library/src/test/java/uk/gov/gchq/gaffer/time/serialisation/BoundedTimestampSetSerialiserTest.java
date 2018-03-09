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
import uk.gov.gchq.gaffer.time.BoundedTimestampSet;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BoundedTimestampSetSerialiserTest extends ToBytesSerialisationTest<BoundedTimestampSet> {

    @Test
    public void testSerialiserWhenNotFull() throws SerialisationException {
        // Given
        final BoundedTimestampSet boundedTimestampSet = getExampleValue();

        // When
        final byte[] serialised = serialiser.serialise(boundedTimestampSet);
        final BoundedTimestampSet deserialised = serialiser.deserialise(serialised);

        // Then
        assertEquals(boundedTimestampSet.getState(), deserialised.getState());
        assertEquals(boundedTimestampSet.getTimeBucket(), deserialised.getTimeBucket());
        assertEquals(boundedTimestampSet.getMaxSize(), deserialised.getMaxSize());
        assertEquals(boundedTimestampSet.getNumberOfTimestamps(), deserialised.getNumberOfTimestamps());
        assertEquals(boundedTimestampSet.getTimestamps(), deserialised.getTimestamps());
    }

    private BoundedTimestampSet getExampleValue() {
        final BoundedTimestampSet boundedTimestampSet = new BoundedTimestampSet(CommonTimeUtil.TimeBucket.SECOND, 10);
        boundedTimestampSet.add(Instant.ofEpochMilli(1000L));
        boundedTimestampSet.add(Instant.ofEpochMilli(1000000L));
        return boundedTimestampSet;
    }

    @Test
    public void testSerialiserWhenSampling() throws SerialisationException {
        // Given
        final Set<Instant> instants = new HashSet<>();
        IntStream.range(0, 1000)
                .forEach(i -> instants.add(Instant.ofEpochMilli(i * 1000L)));
        final BoundedTimestampSet boundedTimestampSet = new BoundedTimestampSet(CommonTimeUtil.TimeBucket.SECOND, 10);
        instants.forEach(boundedTimestampSet::add);

        // When
        final byte[] serialised = serialiser.serialise(boundedTimestampSet);
        final BoundedTimestampSet deserialised = serialiser.deserialise(serialised);

        // Then
        assertEquals(boundedTimestampSet.getState(), deserialised.getState());
        assertEquals(boundedTimestampSet.getTimeBucket(), deserialised.getTimeBucket());
        assertEquals(boundedTimestampSet.getMaxSize(), deserialised.getMaxSize());
        assertEquals(boundedTimestampSet.getNumberOfTimestamps(), deserialised.getNumberOfTimestamps());
        assertEquals(boundedTimestampSet.getTimestamps(), deserialised.getTimestamps());
    }

    @Test
    public void testCanHandle() throws SerialisationException {
        assertTrue(serialiser.canHandle(BoundedTimestampSet.class));
        assertFalse(serialiser.canHandle(String.class));
    }

    @Override
    public Serialiser<BoundedTimestampSet, byte[]> getSerialisation() {
        return new BoundedTimestampSetSerialiser();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Pair<BoundedTimestampSet, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{new Pair(getExampleValue(), new byte[]{0, 10, 0, 58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 16, 0, 0, 0, 1, 0, -24, 3})};
    }
}
