/*
 * Copyright 2017-2021 Crown Copyright
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
package uk.gov.gchq.gaffer.serialisation.implementation;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;
import uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedIntegerSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedLongSerialiser;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MapSerialiserTest extends ToBytesSerialisationTest<Map> {

    @Test
    public void shouldSerialiseAndDeSerialiseOverlappingMapValuesWithDifferentKeys() throws SerialisationException {

        Map<String, Long> map = getExampleValue();

        byte[] b = serialiser.serialise(map);
        Map o = serialiser.deserialise(b);

        assertEquals(HashMap.class, o.getClass());
        assertThat(o).hasSize(6);
        assertEquals(map, o);
        assertEquals((Long) 123298333L, o.get("one"));
        assertEquals((Long) 342903339L, o.get("two"));
        assertEquals((Long) 123298333L, o.get("three"));
        assertEquals((Long) 345353439L, o.get("four"));
        assertEquals((Long) 123338333L, o.get("five"));
        assertEquals((Long) 345353439L, o.get("six"));
    }

    private Map<String, Long> getExampleValue() {
        Map<String, Long> map = new HashMap<>();
        map.put("one", 123298333L);
        map.put("two", 342903339L);
        map.put("three", 123298333L);
        map.put("four", 345353439L);
        map.put("five", 123338333L);
        map.put("six", 345353439L);
        return map;
    }

    @Test
    public void mapSerialiserTest() throws SerialisationException {

        Map<Integer, Integer> map = new LinkedHashMap<>();
        map.put(1, 3);
        map.put(2, 7);
        map.put(3, 11);

        ((MapSerialiser) serialiser).setKeySerialiser(new OrderedIntegerSerialiser());
        ((MapSerialiser) serialiser).setValueSerialiser(new OrderedIntegerSerialiser());
        ((MapSerialiser) serialiser).setMapClass(LinkedHashMap.class);

        byte[] b = serialiser.serialise(map);
        Map o = serialiser.deserialise(b);


        assertEquals(LinkedHashMap.class, o.getClass());
        assertThat(o).hasSize(3);
        assertEquals(3, o.get(1));
        assertEquals(7, o.get(2));
        assertEquals(11, o.get(3));
    }

    @Override
    public Serialiser<Map, byte[]> getSerialisation() {
        MapSerialiser serialiser = new MapSerialiser();
        serialiser.setKeySerialiser(new StringSerialiser());
        serialiser.setValueSerialiser(new OrderedLongSerialiser());
        return serialiser;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Pair<Map, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{new Pair(getExampleValue(), new byte[]{3, 115, 105, 120, 9, 8, -128, 0, 0, 0, 20, -107, -84, -33, 4, 102, 111, 117, 114, 9, 8, -128, 0, 0, 0, 20, -107, -84, -33, 3, 111, 110, 101, 9, 8, -128, 0, 0, 0, 7, 89, 98, 29, 3, 116, 119, 111, 9, 8, -128, 0, 0, 0, 20, 112, 74, 43, 5, 116, 104, 114, 101, 101, 9, 8, -128, 0, 0, 0, 7, 89, 98, 29, 4, 102, 105, 118, 101, 9, 8, -128, 0, 0, 0, 7, 89, -2, 93})};
    }
}
