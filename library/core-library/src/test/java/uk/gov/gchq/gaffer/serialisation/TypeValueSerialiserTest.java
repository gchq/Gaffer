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

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.types.TypeValue;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNull;

public class TypeValueSerialiserTest extends SerialisationTest<TypeValue> {

    @Test
    public void testCanSerialiseDeSerialiseCorrectly() throws SerialisationException {
        TypeValue typeValue = new TypeValue("testType", "testValue");
        byte[] bytes = serialiser.serialise(typeValue);
        String serialisedForm = new String(bytes);
        assertEquals("testType\0testValue", serialisedForm);
        TypeValue deSerialisedTypeValue = (TypeValue) serialiser.deserialise(bytes);
        assertEquals(typeValue.getType(), deSerialisedTypeValue.getType());
        assertEquals(typeValue.getValue(), deSerialisedTypeValue.getValue());
        assertEquals(typeValue, deSerialisedTypeValue);
    }

    @Test
    public void testCanSerialiseDeSerialiseCorrectlyValueOnly() throws SerialisationException {
        TypeValue typeValue = new TypeValue();
        typeValue.setValue("testValue");
        byte[] bytes = serialiser.serialise(typeValue);
        String serialisedForm = new String(bytes);
        assertEquals("\0testValue", serialisedForm);
        TypeValue deSerialisedTypeValue = (TypeValue) serialiser.deserialise(bytes);
        assertNull(deSerialisedTypeValue.getType());
        assertEquals(typeValue.getValue(), deSerialisedTypeValue.getValue());
        assertEquals(typeValue, deSerialisedTypeValue);
    }

    @Test
    public void testCanSerialiseDeSerialiseCorrectlyTypeOnly() throws SerialisationException {
        TypeValue typeValue = new TypeValue();
        typeValue.setType("testType");
        byte[] bytes = serialiser.serialise(typeValue);
        String serialisedForm = new String(bytes);
        assertEquals("testType\0", serialisedForm);
        TypeValue deSerialisedTypeValue = (TypeValue) serialiser.deserialise(bytes);
        assertEquals(typeValue.getType(), deSerialisedTypeValue.getType());
        assertNull(typeValue.getValue(), deSerialisedTypeValue.getValue());
        assertEquals(typeValue, deSerialisedTypeValue);
    }

    @Test
    public void testCanSerialiseDeserialiseCorrectlyAndBeEscaped() throws SerialisationException {
        TypeValue typeValue = new TypeValue("testType", "testValue");
        byte[] bytes = ByteArrayEscapeUtils.escape(serialiser.serialise(typeValue));
        String serialisedForm = new String(bytes);
        assertEquals("testType\1\1testValue", serialisedForm);
        TypeValue deSerialisedTypeValue = (TypeValue) serialiser.deserialise(ByteArrayEscapeUtils
                .unEscape(bytes));
        assertEquals(typeValue.getType(), deSerialisedTypeValue.getType());
        assertEquals(typeValue.getValue(), deSerialisedTypeValue.getValue());
        assertEquals(typeValue, deSerialisedTypeValue);
    }

    @Override
    public void shouldDeserialiseEmptyBytes() throws SerialisationException {
        // When
        final TypeValue value = serialiser.deserialiseEmptyBytes();

        // Then
        assertNull(value);
    }

    @Override
    public Serialisation<TypeValue> getSerialisation() {
        return new TypeValueSerialiser();
    }
}
