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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SetSerialiserTest extends ToByteSerialisationTest<Set<? extends Object>> {

    @Test
    public void shouldSerialiseAndDeSerialiseSet() throws SerialisationException {

        Set<String> set = new HashSet<>();
        set.add("one");
        set.add("two");
        set.add("three");
        set.add("four");
        set.add("five");
        set.add("six");

        SetSerialiser s = new SetSerialiser();
        s.setObjectSerialiser(new StringSerialiser());

        byte[] b = s.serialise(set);
        Set o = s.deserialise(b);

        assertEquals(HashSet.class, o.getClass());
        assertEquals(6, o.size());
        assertEquals(set, o);
        assertTrue(o.contains("one"));
        assertTrue(o.contains("two"));
        assertTrue(o.contains("three"));
        assertTrue(o.contains("four"));
        assertTrue(o.contains("five"));
        assertTrue(o.contains("six"));
    }

    @Test
    public void setSerialiserWithOverlappingValuesTest() throws SerialisationException {

        Set<Integer> set = new LinkedHashSet<>();
        set.add(1);
        set.add(3);
        set.add(2);
        set.add(7);
        set.add(3);
        set.add(11);

        SetSerialiser s = new SetSerialiser();
        s.setObjectSerialiser(new IntegerSerialiser());
        s.setSetClass(LinkedHashSet.class);

        byte[] b = s.serialise(set);
        Set o = s.deserialise(b);

        assertEquals(LinkedHashSet.class, o.getClass());
        assertEquals(6, o.size());
        assertTrue(o.contains(1));
        assertTrue(o.contains(3));
        assertTrue(o.contains(2));
        assertTrue(o.contains(7));
        assertTrue(o.contains(11));
    }

    @Override
    public Serialiser<Set<? extends Object>, byte[]> getSerialisation() {
        return new SetSerialiser();
    }
}
