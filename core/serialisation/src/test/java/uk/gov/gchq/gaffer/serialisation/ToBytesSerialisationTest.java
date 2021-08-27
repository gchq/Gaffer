/*
 * Copyright 2016-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.serialisation;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

public abstract class ToBytesSerialisationTest<T> extends SerialisationTest<T, byte[]> {

    @Test
    @Override
    public void shouldSerialiseNull() throws SerialisationException {
        // When
        final byte[] bytes = serialiser.serialiseNull();

        // Then
        assertArrayEquals(new byte[0], bytes);
    }

    @Test
    @Override
    public void shouldDeserialiseEmpty() throws SerialisationException {
        assertNull(serialiser.deserialiseEmpty());
    }

    @Override
    protected void serialiseFirst(final Pair<T, byte[]> pair) throws SerialisationException {
        byte[] serialise = serialiser.serialise(pair.getFirst());
        assertArrayEquals(pair.getSecond(), serialise, Arrays.toString(serialise));
    }

    @Test
    public void shouldHaveValidEqualsMethodForToByteSerialiser() {
        final Serialiser<T, byte[]> serialiser2 = getSerialisation();
        assertNotSame(this.serialiser, serialiser2,
                "The getSerialisation() shouldn't return the same instance each time it's called, required for this test.");
        assertEquals(this.serialiser, serialiser2, "different instances that are the same should be equal");
    }
}
