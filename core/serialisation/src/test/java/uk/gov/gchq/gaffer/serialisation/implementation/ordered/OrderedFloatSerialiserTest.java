/*
 * Copyright 2017-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.serialisation.implementation.ordered;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OrderedFloatSerialiserTest extends ToBytesSerialisationTest<Float> {

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        // Given
        for (float i = 0; i < 1000; i += 1.1f) {
            // When
            byte[] b = serialiser.serialise(i);
            Object o = serialiser.deserialise(b);

            // Then
            assertEquals(Float.class, o.getClass());
            assertEquals(i, o);
        }
    }

    @Test
    public void canSerialiseFloatMinValue() throws SerialisationException {
        // Given When
        byte[] b = serialiser.serialise(Float.MIN_VALUE);
        Object o = serialiser.deserialise(b);

        // Then
        assertEquals(Float.class, o.getClass());
        assertEquals(Float.MIN_VALUE, o);
    }

    @Test
    public void canSerialiseFloatMaxValue() throws SerialisationException {
        // Given When
        byte[] b = serialiser.serialise(Float.MAX_VALUE);
        Object o = serialiser.deserialise(b);

        // Then
        assertEquals(Float.class, o.getClass());
        assertEquals(Float.MAX_VALUE, o);
    }

    @Test
    public void checkOrderPreserved() throws SerialisationException {
        // Given
        byte[] startBytes = serialiser.serialise(0.0f);
        for (float test = 1.0f; test >= 5; test += 0.1f) {
            // When
            byte[] newTestBytes = serialiser.serialise(test);

            // Then
            assertTrue(compare(newTestBytes, startBytes) < 0);
            startBytes = newTestBytes;
        }
    }

    @Test
    public void cantSerialiseStringClass() {
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void canSerialiseFloatClass() {
        assertTrue(serialiser.canHandle(Float.class));
    }

    private static int compare(final byte[] first, final byte[] second) {
        for (int i = 0; i < first.length; i++) {
            if (first[i] < second[i]) {
                return -1;
            } else if (first[i] > second[i]) {
                return 1;
            }
        }
        return 0;
    }

    @Override
    public Serialiser<Float, byte[]> getSerialisation() {
        return new OrderedFloatSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<Float, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[] {
                new Pair<>(Float.MAX_VALUE, new byte[] {4, 127, 127, -1, -1}),
                new Pair<>(Float.MIN_VALUE, new byte[] {1, 1}),
                new Pair<>(0f, new byte[] {0}),
                new Pair<>(1f, new byte[] {4, 63, -128, 0, 0})
        };
    }
}
