/*
 * Copyright 2016-2020 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.types.TypeValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TypeValueSerialiserTest extends ToBytesSerialisationTest<TypeValue> {

    @Test
    public void testCanSerialiseDeSerialiseCorrectly() throws SerialisationException {
        // Given
        final TypeValue typeValue = new TypeValue("testType", "testValue");

        // When
        final byte[] bytes = serialiser.serialise(typeValue);
        final String serialisedForm = new String(bytes);

        // Then
        assertEquals("testType\0testValue", serialisedForm);

        // When
        final TypeValue deSerialisedTypeValue = serialiser.deserialise(bytes);

        // Then
        assertEquals(typeValue.getType(), deSerialisedTypeValue.getType());
        assertEquals(typeValue.getValue(), deSerialisedTypeValue.getValue());
        assertEquals(typeValue, deSerialisedTypeValue);
    }

    @Test
    public void testCanSerialiseDeSerialiseCorrectlyValueOnly() throws SerialisationException {
        // Given
        final TypeValue typeValue = new TypeValue();
        typeValue.setValue("testValue");

        // When
        final byte[] bytes = serialiser.serialise(typeValue);
        final String serialisedForm = new String(bytes);

        // Then
        assertEquals("\0testValue", serialisedForm);

        // When
        final TypeValue deSerialisedTypeValue = serialiser.deserialise(bytes);

        // Then
        assertNull(deSerialisedTypeValue.getType());
        assertEquals(typeValue.getValue(), deSerialisedTypeValue.getValue());
        assertEquals(typeValue, deSerialisedTypeValue);
    }

    @Test
    public void testCanSerialiseDeSerialiseCorrectlyTypeOnly() throws SerialisationException {
        // Given
        final TypeValue typeValue = new TypeValue();
        typeValue.setType("testType");

        // When
        final byte[] bytes = serialiser.serialise(typeValue);
        final String serialisedForm = new String(bytes);

        // Then
        assertEquals("testType\0", serialisedForm);

        // When
        final TypeValue deSerialisedTypeValue = serialiser.deserialise(bytes);

        // When
        assertEquals(typeValue.getType(), deSerialisedTypeValue.getType());
        assertNull(typeValue.getValue(), deSerialisedTypeValue.getValue());
        assertEquals(typeValue, deSerialisedTypeValue);
    }

    @Test
    public void testCanSerialiseDeserialiseCorrectlyAndBeEscaped() throws SerialisationException {
        // Given
        final TypeValue typeValue = new TypeValue("testType", "testValue");

        // When
        final byte[] bytes = ByteArrayEscapeUtils.escape(serialiser.serialise(typeValue));
        final String serialisedForm = new String(bytes);

        // Then
        assertEquals("testType\1\1testValue", serialisedForm);

        // When
        final TypeValue deSerialisedTypeValue = serialiser.deserialise(ByteArrayEscapeUtils
                .unEscape(bytes));

        // Then
        assertEquals(typeValue.getType(), deSerialisedTypeValue.getType());
        assertEquals(typeValue.getValue(), deSerialisedTypeValue.getValue());
        assertEquals(typeValue, deSerialisedTypeValue);
    }

    @Test
    @Override
    public void shouldDeserialiseEmpty() throws SerialisationException {
        final TypeValue value = serialiser.deserialiseEmpty();

        assertNull(value);
    }

    @Override
    public Serialiser<TypeValue, byte[]> getSerialisation() {
        return new TypeValueSerialiser();
    }

    @Override
    public Pair<TypeValue, byte[]>[] getHistoricSerialisationPairs() {
        TypeValue typeValue = new TypeValue("testType", "testValue");
        return new Pair[] {
                new Pair(typeValue, new byte[] {116, 101, 115, 116, 84, 121, 112, 101, 0, 116, 101, 115, 116, 86, 97, 108, 117, 101})
        };

    }
}
