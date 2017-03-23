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
package uk.gov.gchq.gaffer.sketches.datasketches.quantiles.serialisation;

import com.yahoo.sketches.quantiles.DoublesUnion;
import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DoublesUnionSerialiserTest {
    private static final double DELTA = 0.01D;
    private static final DoublesUnionSerialiser SERIALISER = new DoublesUnionSerialiser();

    @Test
    public void testSerialiseAndDeserialise() {
        final DoublesUnion union = DoublesUnion.builder().build();
        union.update(1.0D);
        union.update(2.0D);
        union.update(3.0D);
        testSerialiser(union);

        final DoublesUnion emptyUnion = DoublesUnion.builder().build();
        testSerialiser(emptyUnion);
    }

    private void testSerialiser(final DoublesUnion union) {
        final double quantile1 = union.getResult().getQuantile(0.5D);
        final byte[] unionSerialised;
        try {
            unionSerialised = SERIALISER.serialise(union);
        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
            return;
        }

        final DoublesUnion unionDeserialised;
        try {
            unionDeserialised = SERIALISER.deserialise(unionSerialised);
        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
            return;
        }
        assertEquals(quantile1, unionDeserialised.getResult().getQuantile(0.5D), DELTA);
    }

    @Test
    public void testCanHandleDoublesUnion() {
        assertTrue(SERIALISER.canHandle(DoublesUnion.class));
        assertFalse(SERIALISER.canHandle(String.class));
    }
}
