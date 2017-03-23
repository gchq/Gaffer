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

import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;
import uk.gov.gchq.gaffer.serialisation.SerialisationTest;

import static org.junit.Assert.assertEquals;

public class RoaringBitmapSerialiserTest extends SerialisationTest<RoaringBitmap> {

    private static final RoaringBitmapSerialiser SERIALISER = new RoaringBitmapSerialiser();

    @Test
    public void testCanSerialiseAndDeserialise() throws SerialisationException {
        RoaringBitmap testBitmap = new RoaringBitmap();
        testBitmap.add(2);
        testBitmap.add(3000);
        testBitmap.add(300000);

        for (int i=400000; i<500000; i+=2) {
            testBitmap.add(i);
        }

        byte[] b = SERIALISER.serialise(testBitmap);
        Object o = SERIALISER.deserialise(b);
        assertEquals(RoaringBitmap.class, o.getClass());
        assertEquals(testBitmap, o);
    }

    @Override
    public void shouldDeserialiseEmptyBytes() throws SerialisationException {
        // When
        final RoaringBitmap value = serialiser.deserialiseEmptyBytes();

        // Then
        assertEquals(new RoaringBitmap(), value);
    }

    @Override
    public Serialisation<RoaringBitmap> getSerialisation() {
        return new RoaringBitmapSerialiser();
    }
}
