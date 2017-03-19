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
package uk.gov.gchq.gaffer.serialisation;

import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AvroSerialiserTest {

    private AvroSerialiser serialiser = null;

    @Before
    public void setupTest() throws SerialisationException {
        serialiser = new AvroSerialiser();
    }

    @Test
    public void testCanHandleObjectClass() {
        assertTrue(serialiser.canHandle(Object.class));
    }

    @Test
    public void testPrimitiveSerialisation() throws SerialisationException {
        byte[] b = serialiser.serialise(2);
        Object o = serialiser.deserialise(b);
        assertEquals(Integer.class, o.getClass());
        assertEquals(2, o);
    }

    @Test
    public void testParameterisedDeserialisationOfSimpleObject() throws SerialisationException {
        byte[] b = serialiser.serialise(2);
        Integer o = serialiser.deserialise(b, Integer.class);
        assertEquals(Integer.class, o.getClass());
        assertEquals(0, o.compareTo(2));
    }


}
