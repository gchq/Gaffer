/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.types;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import uk.gov.gchq.gaffer.bitmap.serialisation.json.BitmapJsonModules;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.serialisation.DoubleSerialiser;
import uk.gov.gchq.gaffer.serialisation.IntegerSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.MapSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.TreeSetStringSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.RawFloatSerialiser;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.gaffer.time.serialisation.RBMBackedTimestampSetSerialiser;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class CustomMapTest {

    @Test
    public void shouldJSONSerialiseStringInteger() throws IOException {
        //given
        final CustomMap<String, Integer> expectedMap = new CustomMap<>(new StringSerialiser(), new IntegerSerialiser());
        expectedMap.put("one", 1111);
        expectedMap.put("two", 2222);
        final String expectedJson = jsonFromFile("custom-map01.json");

        //when
        final byte[] serialise = JSONSerialiser.serialise(expectedMap, true);
        final CustomMap jsonMap = JSONSerialiser.deserialise(expectedJson, CustomMap.class);
        final CustomMap deserialiseMap = JSONSerialiser.deserialise(serialise, CustomMap.class);

        //then
        assertEquals("The expected map from Json doesn't match", jsonMap, deserialiseMap);
        assertEquals("The expected map doesn't match", expectedMap, deserialiseMap);
    }

    @Test
    public void shouldJSONSerialiseBigIntString() throws IOException {
        //given
        final CustomMap<TreeSet<String>, Double> expectedMap = new CustomMap<>(new TreeSetStringSerialiser(), new DoubleSerialiser());
        final TreeSet<String> key1 = new TreeSet<>();
        key1.add("k1");
        key1.add("k2");
        expectedMap.put(key1, 11.11);
        final TreeSet<String> key2 = new TreeSet<>();
        key2.add("k3");
        key2.add("k4");
        expectedMap.put(key2, 22.22);

        final String expectedJson = jsonFromFile("custom-map02.json");

        //when
        final byte[] serialise = JSONSerialiser.serialise(expectedMap, true);
        final CustomMap jsonMap = JSONSerialiser.deserialise(expectedJson, CustomMap.class);
        final CustomMap deserialiseMap = JSONSerialiser.deserialise(serialise, CustomMap.class);

        //then
        assertEquals("The expected map from Json doesn't match", jsonMap, deserialiseMap);
        assertEquals("The expected map doesn't match", expectedMap, deserialiseMap);
    }

    @Test
    public void shouldJSONSerialiseStringMap() throws IOException {
        //given
        final MapSerialiser mapSerialiser = new MapSerialiser();
        mapSerialiser.setValueSerialiser(new StringSerialiser());
        mapSerialiser.setKeySerialiser(new StringSerialiser());
        final CustomMap<String, HashMap> expectedMap = new CustomMap<>(new StringSerialiser(), mapSerialiser);
        final HashMap<String, String> innerMap1 = new HashMap<>();
        innerMap1.put("innerKeyOne", "innerValue1");
        final HashMap<String, String> innerMap2 = new HashMap<>();
        innerMap2.put("innerKeyTwo", "innerValue2");
        expectedMap.put("innerOne", innerMap1);
        expectedMap.put("innerTwo", innerMap2);
        final String expectedJson = jsonFromFile("custom-map03.json");

        //when
        final byte[] serialise = JSONSerialiser.serialise(expectedMap, true);
        final CustomMap jsonMap = JSONSerialiser.deserialise(expectedJson, CustomMap.class);
        final CustomMap deserialiseMap = JSONSerialiser.deserialise(serialise, CustomMap.class);

        //then
        assertEquals("The expected map from Json doesn't match", jsonMap, deserialiseMap);
        assertEquals("The expected map doesn't match", expectedMap, deserialiseMap);
    }

    @Test
    public void shouldJSONSerialiseFloatRDM() throws IOException {
        //given
        System.setProperty(JSONSerialiser.JSON_SERIALISER_MODULES, BitmapJsonModules.class.getCanonicalName());

        final RBMBackedTimestampSet timestampSet1 = new RBMBackedTimestampSet.Builder()
                .timeBucket(CommonTimeUtil.TimeBucket.MINUTE)
                .timestamps(Lists.newArrayList(Instant.ofEpochSecond(11)))
                .build();

        final RBMBackedTimestampSet timestampSet2 = new RBMBackedTimestampSet.Builder()
                .timeBucket(CommonTimeUtil.TimeBucket.HOUR)
                .timestamps(Lists.newArrayList(Instant.ofEpochSecond(222222)))
                .build();

        final CustomMap<Float, RBMBackedTimestampSet> expectedMap = new CustomMap<>(new RawFloatSerialiser(), new RBMBackedTimestampSetSerialiser());
        expectedMap.put(123.3f, timestampSet1);
        expectedMap.put(345.6f, timestampSet2);

        final String expectedJson = jsonFromFile("custom-map04.json");

        //when
        final byte[] serialise = JSONSerialiser.serialise(expectedMap, true);
        final CustomMap jsonMap = JSONSerialiser.deserialise(expectedJson, CustomMap.class);
        final CustomMap deserialiseMap = JSONSerialiser.deserialise(serialise, CustomMap.class);

        //then
        assertEquals("The expected map from Json doesn't match", jsonMap, deserialiseMap);
        assertEquals("The expected map doesn't match", expectedMap, deserialiseMap);
    }

    protected String jsonFromFile(final String path) throws IOException {
        return IOUtils.readLines(StreamUtil.openStream(getClass(), path))
                .stream()
                .collect(Collectors.joining("\n"));
    }
}
