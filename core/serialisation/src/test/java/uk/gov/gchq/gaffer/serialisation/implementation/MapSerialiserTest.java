/*
 * Copyright 2017 Crown Copyright
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

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.IntegerSerialiser;
import uk.gov.gchq.gaffer.serialisation.LongSerialiser;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToByteSerialisationTest;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MapSerialiserTest extends ToByteSerialisationTest<Map<? extends Object, ? extends Object>> {

    @Test
    public void shouldSerialiseAndDeSerialiseOverlappingMapValuesWithDifferentKeys() throws SerialisationException {

        Map<String, Long> map = new HashMap<>();
        map.put("one", 123298333L);
        map.put("two", 342903339L);
        map.put("three", 123298333L);
        map.put("four", 345353439L);
        map.put("five", 123338333L);
        map.put("six", 345353439L);

        MapSerialiser s = new MapSerialiser();
        s.setKeySerialiser(new StringSerialiser());
        s.setValueSerialiser(new LongSerialiser());

        byte[] b = s.serialise(map);
        Map o = s.deserialise(b);

        assertEquals(HashMap.class, o.getClass());
        assertEquals(6, o.size());
        assertEquals(map, o);
        assertEquals((Long)123298333L, o.get("one"));
        assertEquals((Long)342903339L, o.get("two"));
        assertEquals((Long)123298333L, o.get("three"));
        assertEquals((Long)345353439L, o.get("four"));
        assertEquals((Long)123338333L, o.get("five"));
        assertEquals((Long)345353439L, o.get("six"));
    }

    @Test
    public void mapSerialiserTest() throws SerialisationException {

        Map<Integer, Integer> map = new LinkedHashMap<>();
        map.put(1, 3);
        map.put(2, 7);
        map.put(3, 11);

        MapSerialiser s = new MapSerialiser();
        s.setKeySerialiser(new IntegerSerialiser());
        s.setValueSerialiser(new IntegerSerialiser());
        s.setMapClass(LinkedHashMap.class);

        byte[] b =s.serialise(map);
        Map o = s.deserialise(b);


        assertEquals(LinkedHashMap.class, o.getClass());
        assertEquals(3, o.size());
        assertEquals(3, o.get(1));
        assertEquals(7, o.get(2));
        assertEquals(11, o.get(3));
    }

    @Override
    public Serialiser<Map<? extends Object, ? extends Object>, byte[]> getSerialisation() {
        return new MapSerialiser();
    }
}
