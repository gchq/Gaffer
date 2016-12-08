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
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNull;

public class TypeSubTypeValueSerialiserTest extends SerialisationTest<TypeSubTypeValue> {

    private static final TypeSubTypeValueSerialiser serialiser = new TypeSubTypeValueSerialiser();

    @Test
    public void testCanSerialiseDeSerialiseCorrectly() throws SerialisationException {
        TypeSubTypeValue typeSubTypeValue = new TypeSubTypeValue("testType", "testSubType", "testValue");
        byte[] bytes = serialiser.serialise(typeSubTypeValue);
        String serialisedForm = new String(bytes);
        assertEquals("testType\0testSubType\0testValue", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = (TypeSubTypeValue) serialiser
                .deserialise(bytes);
        assertEquals(typeSubTypeValue.getType(), deSerialisedTypeSubTypeValue.getType());
        assertEquals(typeSubTypeValue.getSubType(), deSerialisedTypeSubTypeValue.getSubType());
        assertEquals(typeSubTypeValue.getValue(), deSerialisedTypeSubTypeValue.getValue());
        assertEquals(typeSubTypeValue, deSerialisedTypeSubTypeValue);
    }

    @Test
    public void testCanSerialiseDeSerialiseCorrectlyValueOnly() throws SerialisationException {
        TypeSubTypeValue typeSubTypeValue = new TypeSubTypeValue();
        typeSubTypeValue.setValue("testValue");
        byte[] bytes = serialiser.serialise(typeSubTypeValue);
        String serialisedForm = new String(bytes);
        assertEquals("\0\0testValue", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = (TypeSubTypeValue) serialiser
                .deserialise(bytes);
        assertNull(deSerialisedTypeSubTypeValue.getType());
        assertNull(deSerialisedTypeSubTypeValue.getSubType());
        assertEquals(typeSubTypeValue.getValue(), deSerialisedTypeSubTypeValue.getValue());
        assertEquals(typeSubTypeValue, deSerialisedTypeSubTypeValue);
    }

    @Test
    public void testCanSerialiseDeSerialiseCorrectlyTypeValueOnly() throws SerialisationException {
        TypeSubTypeValue typeSubTypeValue = new TypeSubTypeValue();
        typeSubTypeValue.setValue("testValue");
        typeSubTypeValue.setType("testType");
        byte[] bytes = serialiser.serialise(typeSubTypeValue);
        String serialisedForm = new String(bytes);
        assertEquals("testType\0\0testValue", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = (TypeSubTypeValue) serialiser
                .deserialise(bytes);
        assertEquals(typeSubTypeValue.getType(), deSerialisedTypeSubTypeValue.getType());
        assertNull(deSerialisedTypeSubTypeValue.getSubType());
        assertEquals(typeSubTypeValue.getValue(), deSerialisedTypeSubTypeValue.getValue());
        assertEquals(typeSubTypeValue, deSerialisedTypeSubTypeValue);
    }

    @Test
    public void testCanSerialiseDeSerialiseCorrectlySubTypeValueOnly() throws SerialisationException {
        TypeSubTypeValue typeSubTypeValue = new TypeSubTypeValue();
        typeSubTypeValue.setValue("testValue");
        typeSubTypeValue.setSubType("testSubType");
        byte[] bytes = serialiser.serialise(typeSubTypeValue);
        String serialisedForm = new String(bytes);
        assertEquals("\0testSubType\0testValue", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = (TypeSubTypeValue) serialiser
                .deserialise(bytes);
        assertNull(deSerialisedTypeSubTypeValue.getType());
        assertEquals(typeSubTypeValue.getSubType(), deSerialisedTypeSubTypeValue.getSubType());
        assertEquals(typeSubTypeValue.getValue(), deSerialisedTypeSubTypeValue.getValue());
        assertEquals(typeSubTypeValue, deSerialisedTypeSubTypeValue);
    }

    @Test
    public void testCanSerialiseDeSerialiseCorrectlyTypeOnly() throws SerialisationException {
        TypeSubTypeValue typeSubTypeValue = new TypeSubTypeValue();
        typeSubTypeValue.setType("testType");
        byte[] bytes = serialiser.serialise(typeSubTypeValue);
        String serialisedForm = new String(bytes);
        assertEquals("testType\0\0", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = (TypeSubTypeValue) serialiser
                .deserialise(bytes);
        assertEquals(typeSubTypeValue.getType(), deSerialisedTypeSubTypeValue.getType());
        assertNull(deSerialisedTypeSubTypeValue.getSubType());
        assertNull(typeSubTypeValue.getValue(), deSerialisedTypeSubTypeValue.getValue());
        assertEquals(typeSubTypeValue, deSerialisedTypeSubTypeValue);
    }

    @Test
    public void testCanSerialiseDeSerialiseCorrectlySubTypeOnly() throws SerialisationException {
        TypeSubTypeValue typeSubTypeValue = new TypeSubTypeValue();
        typeSubTypeValue.setSubType("testSubType");
        byte[] bytes = serialiser.serialise(typeSubTypeValue);
        String serialisedForm = new String(bytes);
        assertEquals("\0testSubType\0", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = (TypeSubTypeValue) serialiser
                .deserialise(bytes);
        assertNull(deSerialisedTypeSubTypeValue.getType());
        assertEquals(typeSubTypeValue.getSubType(), deSerialisedTypeSubTypeValue.getSubType());
        assertNull(typeSubTypeValue.getValue(), deSerialisedTypeSubTypeValue.getValue());
        assertEquals(typeSubTypeValue, deSerialisedTypeSubTypeValue);
    }

    @Test
    public void testCanSerialiseDeSerialiseCorrectlyTypeSubTypeOnly() throws SerialisationException {
        TypeSubTypeValue typeSubTypeValue = new TypeSubTypeValue();
        typeSubTypeValue.setType("testType");
        typeSubTypeValue.setSubType("testSubType");
        byte[] bytes = serialiser.serialise(typeSubTypeValue);
        String serialisedForm = new String(bytes);
        assertEquals("testType\0testSubType\0", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = (TypeSubTypeValue) serialiser
                .deserialise(bytes);
        assertEquals(typeSubTypeValue.getType(), deSerialisedTypeSubTypeValue.getType());
        assertEquals(typeSubTypeValue.getSubType(), deSerialisedTypeSubTypeValue.getSubType());
        assertNull(typeSubTypeValue.getValue(), deSerialisedTypeSubTypeValue.getValue());
        assertEquals(typeSubTypeValue, deSerialisedTypeSubTypeValue);
    }

    @Test
    public void testCanSerialiseDeserialiseCorrectlyAndBeEscaped() throws SerialisationException {
        TypeSubTypeValue typeSubTypeValue = new TypeSubTypeValue("testType", "testSubType", "testValue");
        byte[] bytes = ByteArrayEscapeUtils.escape(serialiser.serialise(typeSubTypeValue));
        String serialisedForm = new String(bytes);
        assertEquals("testType\1\1testSubType\1\1testValue", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = (TypeSubTypeValue) serialiser
                .deserialise(ByteArrayEscapeUtils.unEscape(bytes));
        assertEquals(typeSubTypeValue.getType(), deSerialisedTypeSubTypeValue.getType());
        assertEquals(typeSubTypeValue.getSubType(), deSerialisedTypeSubTypeValue.getSubType());
        assertEquals(typeSubTypeValue.getValue(), deSerialisedTypeSubTypeValue.getValue());
        assertEquals(typeSubTypeValue, deSerialisedTypeSubTypeValue);
    }

    @Override
    public void shouldDeserialiseEmptyBytes() throws SerialisationException {
        // When
        final TypeSubTypeValue value = serialiser.deserialiseEmptyBytes();

        // Then
        assertEquals(new TypeSubTypeValue(), value);
    }

    @Override
    public Serialisation<TypeSubTypeValue> getSerialisation() {
        return new TypeSubTypeValueSerialiser();
    }
}
