/*
 * Copyright 2016-2019 Crown Copyright
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
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TypeSubTypeValueSerialiserTest extends ToBytesSerialisationTest<TypeSubTypeValue> {

    private static final TypeSubTypeValueSerialiser SERIALISER = new TypeSubTypeValueSerialiser();

    @Test
    public void testCanSerialiseDeSerialiseCorrectly() throws SerialisationException {
        TypeSubTypeValue typeSubTypeValue = new TypeSubTypeValue("testType", "testSubType", "testValue");
        byte[] bytes = SERIALISER.serialise(typeSubTypeValue);
        String serialisedForm = new String(bytes);
        assertEquals("testType\0testSubType\0testValue", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = SERIALISER
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
        byte[] bytes = SERIALISER.serialise(typeSubTypeValue);
        String serialisedForm = new String(bytes);
        assertEquals("\0\0testValue", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = SERIALISER
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
        byte[] bytes = SERIALISER.serialise(typeSubTypeValue);
        String serialisedForm = new String(bytes);
        assertEquals("testType\0\0testValue", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = SERIALISER
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
        byte[] bytes = SERIALISER.serialise(typeSubTypeValue);
        String serialisedForm = new String(bytes);
        assertEquals("\0testSubType\0testValue", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = SERIALISER
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
        byte[] bytes = SERIALISER.serialise(typeSubTypeValue);
        String serialisedForm = new String(bytes);
        assertEquals("testType\0\0", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = SERIALISER
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
        byte[] bytes = SERIALISER.serialise(typeSubTypeValue);
        String serialisedForm = new String(bytes);
        assertEquals("\0testSubType\0", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = SERIALISER
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
        byte[] bytes = SERIALISER.serialise(typeSubTypeValue);
        String serialisedForm = new String(bytes);
        assertEquals("testType\0testSubType\0", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = SERIALISER
                .deserialise(bytes);
        assertEquals(typeSubTypeValue.getType(), deSerialisedTypeSubTypeValue.getType());
        assertEquals(typeSubTypeValue.getSubType(), deSerialisedTypeSubTypeValue.getSubType());
        assertNull(typeSubTypeValue.getValue(), deSerialisedTypeSubTypeValue.getValue());
        assertEquals(typeSubTypeValue, deSerialisedTypeSubTypeValue);
    }

    @Test
    public void testCanSerialiseDeserialiseCorrectlyAndBeEscaped() throws SerialisationException {
        TypeSubTypeValue typeSubTypeValue = new TypeSubTypeValue("testType", "testSubType", "testValue");
        byte[] bytes = ByteArrayEscapeUtils.escape(SERIALISER.serialise(typeSubTypeValue));
        String serialisedForm = new String(bytes);
        assertEquals("testType\1\1testSubType\1\1testValue", serialisedForm);
        TypeSubTypeValue deSerialisedTypeSubTypeValue = SERIALISER
                .deserialise(ByteArrayEscapeUtils.unEscape(bytes));
        assertEquals(typeSubTypeValue.getType(), deSerialisedTypeSubTypeValue.getType());
        assertEquals(typeSubTypeValue.getSubType(), deSerialisedTypeSubTypeValue.getSubType());
        assertEquals(typeSubTypeValue.getValue(), deSerialisedTypeSubTypeValue.getValue());
        assertEquals(typeSubTypeValue, deSerialisedTypeSubTypeValue);
    }

    @Override
    public void shouldDeserialiseEmpty() throws SerialisationException {
        // When
        final TypeSubTypeValue value = SERIALISER.deserialiseEmpty();

        // Then
        assertEquals(new TypeSubTypeValue(), value);
    }

    @Override
    public Serialiser<TypeSubTypeValue, byte[]> getSerialisation() {
        return new TypeSubTypeValueSerialiser();
    }

    @Override
    public Pair<TypeSubTypeValue, byte[]>[] getHistoricSerialisationPairs() {
        TypeSubTypeValue typeSubTypeValue = new TypeSubTypeValue();
        typeSubTypeValue.setType("testType");
        typeSubTypeValue.setSubType("testSubType");
        return new Pair[]{
                new Pair(typeSubTypeValue, new byte[]{116, 101, 115, 116, 84, 121, 112, 101, 0, 116, 101, 115, 116, 83, 117, 98, 84, 121, 112, 101, 0})
        };
    }
}
