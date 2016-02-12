/*
 * Copyright 2016 Crown Copyright
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
package gaffer.serialisation.simple;

import gaffer.exception.SerialisationException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StringSerialiserTest {

    private static final StringSerialiser SERIALISER = new StringSerialiser();

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            builder.append(i);
            byte[] b = SERIALISER.serialise(builder.toString());
            Object o = SERIALISER.deserialise(b);
            assertEquals(String.class, o.getClass());
            assertEquals(builder.toString(), o);
        }
    }

    @Test
    public void cantSerialiseLongClass() throws SerialisationException {
        assertFalse(SERIALISER.canHandle(Long.class));
    }

    @Test
    public void canSerialiseStringClass() throws SerialisationException {
        assertTrue(SERIALISER.canHandle(String.class));
    }
}