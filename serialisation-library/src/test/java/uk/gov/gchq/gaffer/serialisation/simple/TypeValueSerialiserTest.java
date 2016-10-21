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
package uk.gov.gchq.gaffer.serialisation.simple;

import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.types.simple.TypeValue;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

public class TypeValueSerialiserTest {

    private static final TypeValueSerialiser SERIALISER = new TypeValueSerialiser();

    @Test
    public void testCanSerialiseDeSerialiseCorrectly() throws SerialisationException {
        TypeValue typeValue = new TypeValue("testType", "testValue");
        byte[] bytes = SERIALISER.serialise(typeValue);
        String serialisedForm = new String(bytes);
        assertEquals("testType\0testValue", serialisedForm);
        TypeValue deSerialisedTypeValue = (TypeValue) SERIALISER.deserialise(bytes);
        assertEquals(typeValue.getType(), deSerialisedTypeValue.getType());
        assertEquals(typeValue.getValue(), deSerialisedTypeValue.getValue());
        assertEquals(typeValue, deSerialisedTypeValue);
    }

    @Test
    public void testCanSerialiseDeSerialiseCorrectlyValueOnly() throws SerialisationException {
        TypeValue typeValue = new TypeValue();
        typeValue.setValue("testValue");
        byte[] bytes = SERIALISER.serialise(typeValue);
        String serialisedForm = new String(bytes);
        assertEquals("\0testValue", serialisedForm);
        TypeValue deSerialisedTypeValue = (TypeValue) SERIALISER.deserialise(bytes);
        assertNull(deSerialisedTypeValue.getType());
        assertEquals(typeValue.getValue(), deSerialisedTypeValue.getValue());
        assertEquals(typeValue, deSerialisedTypeValue);
    }

    @Test
    public void testCanSerialiseDeSerialiseCorrectlyTypeOnly() throws SerialisationException {
        TypeValue typeValue = new TypeValue();
        typeValue.setType("testType");
        byte[] bytes = SERIALISER.serialise(typeValue);
        String serialisedForm = new String(bytes);
        assertEquals("testType\0", serialisedForm);
        TypeValue deSerialisedTypeValue = (TypeValue) SERIALISER.deserialise(bytes);
        assertEquals(typeValue.getType(), deSerialisedTypeValue.getType());
        assertNull(typeValue.getValue(), deSerialisedTypeValue.getValue());
        assertEquals(typeValue, deSerialisedTypeValue);
    }

    @Test
    public void testCanSerialiseDeserialiseCorrectlyAndBeEscaped() throws SerialisationException {
        TypeValue typeValue = new TypeValue("testType", "testValue");
        byte[] bytes = ByteArrayEscapeUtils.escape(SERIALISER.serialise(typeValue));
        String serialisedForm = new String(bytes);
        assertEquals("testType\1\1testValue", serialisedForm);
        TypeValue deSerialisedTypeValue = (TypeValue) SERIALISER.deserialise(ByteArrayEscapeUtils.unEscape(bytes));
        assertEquals(typeValue.getType(), deSerialisedTypeValue.getType());
        assertEquals(typeValue.getValue(), deSerialisedTypeValue.getValue());
        assertEquals(typeValue, deSerialisedTypeValue);
    }
}
