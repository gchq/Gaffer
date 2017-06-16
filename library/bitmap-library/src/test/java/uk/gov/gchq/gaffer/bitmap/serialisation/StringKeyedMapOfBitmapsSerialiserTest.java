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
package uk.gov.gchq.gaffer.bitmap.serialisation;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.roaringbitmap.RoaringBitmap;
import uk.gov.gchq.gaffer.bitmap.types.MapOfBitmaps;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToByteSerialisationTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class StringKeyedMapOfBitmapsSerialiserTest extends ToByteSerialisationTest<MapOfBitmaps> {

    @Test
    public void shouldSerialiseAndDeSerialiseOverlappingBitmapsWithDifferentKeys() throws SerialisationException {

        MapOfBitmaps mapOfBitmaps = new MapOfBitmaps();
        RoaringBitmap inputBitmap = new RoaringBitmap();
        int input1 = 123298333;
        int input2 = 342903339;
        inputBitmap.add(input1);
        inputBitmap.add(input2);

        int input3 = 123298333;
        int input4 = 345353439;
        inputBitmap.add(input3);
        inputBitmap.add(input4);

        int input5 = 123338333;
        int input6 = 345353439;
        inputBitmap.add(input5);
        inputBitmap.add(input6);

        mapOfBitmaps.put("bitMapA", inputBitmap);


        RoaringBitmap inputBitmap2 = new RoaringBitmap();
        int input7 = 123338333;
        int input8 = 343429339;
        inputBitmap2.add(input7);
        inputBitmap2.add(input8);
        mapOfBitmaps.put("bitMapB", inputBitmap2);

        byte[] b = getSerialisation().serialise(mapOfBitmaps);
        MapOfBitmaps o = getSerialisation().deserialise(b);

        assertEquals(MapOfBitmaps.class, o.getClass());
        assertEquals(2, o.size());
        assertEquals(inputBitmap2, o.get("bitMapB"));
        RoaringBitmap resultBitmap2 = o.get("bitMapB");
        assertEquals(2, resultBitmap2.getCardinality());

        RoaringBitmap resultBitmap1 = o.get("bitMapA");
        assertEquals(4, resultBitmap1.getCardinality());
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void shouldThrowErrorWhenAskedToSerialiseNonStringKeyedMapEntry() throws SerialisationException {
        expectedEx.expect(SerialisationException.class);
        expectedEx.expectMessage("Key in MapOfBitmaps was not of expected type, expected: class java.lang.String but was class java.lang.Integer");
        MapOfBitmaps mapOfBitmaps = new MapOfBitmaps();
        RoaringBitmap inputBitmap = new RoaringBitmap();
        int input1 = 123298333;
        int input2 = 342903339;
        inputBitmap.add(input1);
        inputBitmap.add(input2);
        mapOfBitmaps.put(1, inputBitmap);
        getSerialisation().serialise(mapOfBitmaps);
        fail();
    }

    @Override
    public Serialiser<MapOfBitmaps, byte[]> getSerialisation() {
        return new StringKeyedMapOfBitmapsSerialiser();
    }
}
