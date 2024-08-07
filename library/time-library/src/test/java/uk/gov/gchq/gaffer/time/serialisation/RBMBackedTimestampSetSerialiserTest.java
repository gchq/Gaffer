/*
 * Copyright 2017-2024 Crown Copyright
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

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.bitmap.serialisation.json.BitmapJsonModules;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.serialisation.CustomMapSerialiser;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedFloatSerialiser;
import uk.gov.gchq.gaffer.time.CommonTimeUtil.TimeBucket;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.gaffer.types.CustomMap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Random;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class RBMBackedTimestampSetSerialiserTest extends ToBytesSerialisationTest<RBMBackedTimestampSet> {

    @Test
    void testSerialiser() throws SerialisationException {
        // Given
        final RBMBackedTimestampSet rbmBackedTimestampSet = getExampleValue();

        // When
        final byte[] serialised = serialiser.serialise(rbmBackedTimestampSet);
        final RBMBackedTimestampSet deserialised = serialiser.deserialise(serialised);

        // Then
        assertThat(deserialised).isEqualTo(rbmBackedTimestampSet);
    }

    private RBMBackedTimestampSet getExampleValue() {
        final RBMBackedTimestampSet rbmBackedTimestampSet = new RBMBackedTimestampSet(TimeBucket.SECOND);
        rbmBackedTimestampSet.add(Instant.ofEpochMilli(1000L));
        rbmBackedTimestampSet.add(Instant.ofEpochMilli(1000000L));
        return rbmBackedTimestampSet;
    }

    @Test
    void testCanHandle() {
        assertThat(serialiser.canHandle(RBMBackedTimestampSet.class)).isTrue();
        assertThat(serialiser.canHandle(String.class)).isFalse();
    }

    @Test
    void testSerialisationSizesQuotedInJavadoc() throws SerialisationException {
        // Given
        final Random random = new Random(123456789L);
        final Instant instant = ZonedDateTime.of(2017, 1, 1, 1, 1, 1, 0, ZoneId.of("UTC")).toInstant();
        // Set of 100 minutes in a day and the time bucket is a minute
        final RBMBackedTimestampSet rbmBackedTimestampSet1 = new RBMBackedTimestampSet(TimeBucket.MINUTE);
        IntStream.range(0, 100)
                .forEach(i -> rbmBackedTimestampSet1.add(instant.plusSeconds(random.nextInt(24 * 60) * 60)));
        // Set of every minute in a year and the time bucket is a minute
        final RBMBackedTimestampSet rbmBackedTimestampSet2 = new RBMBackedTimestampSet(TimeBucket.MINUTE);
        IntStream.range(0, 365 * 24 * 60)
                .forEach(i -> rbmBackedTimestampSet2.add(instant.plusSeconds(i * 60)));
        // Set of every second in a year is set and the time bucket is a second
        final RBMBackedTimestampSet rbmBackedTimestampSet3 = new RBMBackedTimestampSet(TimeBucket.SECOND);
        IntStream.range(0, 365 * 24 * 60 * 60)
                .forEach(i -> rbmBackedTimestampSet3.add(instant.plusSeconds(i)));

        // When
        final int lengthSet1 = serialiser.serialise(rbmBackedTimestampSet1).length;
        final int lengthSet2 = serialiser.serialise(rbmBackedTimestampSet2).length;
        final int lengthSet3 = serialiser.serialise(rbmBackedTimestampSet3).length;

        // Then
        assertThat(lengthSet1).isBetween(200, 220);
        assertThat(lengthSet2).isBetween(72000, 74000);
        assertThat(lengthSet3).isBetween(3900000, 4100000);
    }

    @Test
    void shouldSerialiserStringRBMBackedTimestampSet() throws SerialisationException {
        // Given
        final Serialiser<CustomMap, byte[]> customMapSerialiser = new CustomMapSerialiser();
        final RBMBackedTimestampSet timestampSet1 = new RBMBackedTimestampSet.Builder()
                .timeBucket(TimeBucket.MINUTE)
                .timestamps(Lists.newArrayList(Instant.ofEpochSecond(10)))
                .timestamps(Lists.newArrayList(Instant.ofEpochSecond(20)))
                .build();

        final RBMBackedTimestampSet timestampSet2 = new RBMBackedTimestampSet.Builder()
                .timeBucket(TimeBucket.MINUTE)
                .timestamps(Lists.newArrayList(Instant.ofEpochSecond(111)))
                .timestamps(Lists.newArrayList(Instant.ofEpochSecond(222)))
                .build();

        final CustomMap<String, RBMBackedTimestampSet> expected = new CustomMap<String, RBMBackedTimestampSet>(new StringSerialiser(), new RBMBackedTimestampSetSerialiser());
        expected.put("OneTimeStamp", timestampSet1);
        expected.put("TwoTimeStamp", timestampSet2);

        // When
        final CustomMap deserialise = customMapSerialiser.deserialise(customMapSerialiser.serialise(expected));

        // Then
        detailedEquals(expected, deserialise, String.class, RBMBackedTimestampSet.class, new StringSerialiser(), new RBMBackedTimestampSetSerialiser());
    }

    private void detailedEquals(final CustomMap expected, final CustomMap actual, final Class expectedKClass, final Class expectedVClass, final ToBytesSerialiser kS, final ToBytesSerialiser vS) {
        Throwable thrown = catchThrowable(() -> assertThat(actual).isEqualTo(expected));

        if (thrown != null) {
            // Serialiser
            assertThat(expected.getKeySerialiser()).isEqualTo(kS);
            assertThat(actual.getKeySerialiser()).isEqualTo(kS);
            assertThat(expected.getValueSerialiser()).isEqualTo(vS);
            assertThat(actual.getValueSerialiser()).isEqualTo(vS);
            assertThat(actual.getKeySerialiser()).isEqualTo(expected.getKeySerialiser());

            // Key element
            assertThat(expected.keySet().iterator().next().getClass()).isEqualTo(expectedKClass);
            assertThat(actual.keySet().iterator().next().getClass()).isEqualTo(expectedKClass);

            // Value element
            assertThat(expected.values().iterator().next().getClass()).isEqualTo(expectedVClass);
            assertThat(actual.values().iterator().next().getClass()).isEqualTo(expectedVClass);

            // keySets
            assertThat(actual.keySet()).isEqualTo(expected.keySet());

            //values
            for (Object k : expected.keySet()) {
                final Object expectedV = expected.get(k);
                final Object actualV = actual.get(k);
                assertThat(actualV.getClass()).isEqualTo(expectedV.getClass());
                assertThat(actualV.getClass()).isEqualTo(expectedVClass);
                assertThat(expectedV.getClass()).isEqualTo(expectedVClass);
                assertThat(actualV).isEqualTo(expectedV);
            }
            assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    void shouldJSONSerialiseFloatRDM() throws IOException {
        //given
        System.setProperty(JSONSerialiser.JSON_SERIALISER_MODULES, BitmapJsonModules.class.getCanonicalName());

        final RBMBackedTimestampSet timestampSet1 = new RBMBackedTimestampSet.Builder()
                .timeBucket(TimeBucket.MINUTE)
                .timestamps(Lists.newArrayList(Instant.ofEpochSecond(11)))
                .build();

        final RBMBackedTimestampSet timestampSet2 = new RBMBackedTimestampSet.Builder()
                .timeBucket(TimeBucket.HOUR)
                .timestamps(Lists.newArrayList(Instant.ofEpochSecond(222222)))
                .build();

        final CustomMap<Float, RBMBackedTimestampSet> expectedMap = new CustomMap<>(new OrderedFloatSerialiser(), new RBMBackedTimestampSetSerialiser());
        expectedMap.put(123.3f, timestampSet1);
        expectedMap.put(345.6f, timestampSet2);

        final String expectedJson = jsonFromFile("custom-map04.json");

        //when
        final byte[] serialise = JSONSerialiser.serialise(expectedMap, true);
        final CustomMap jsonMap = JSONSerialiser.deserialise(expectedJson, CustomMap.class);
        final CustomMap deserialiseMap = JSONSerialiser.deserialise(serialise, CustomMap.class);

        //then
        assertThat(jsonMap).isEqualTo(expectedMap);
        assertThat(deserialiseMap).isEqualTo(expectedMap);
    }

    protected String jsonFromFile(final String path) throws IOException {
        return String.join("\n", IOUtils.readLines(StreamUtil.openStream(getClass(), path), StandardCharsets.UTF_8));
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
