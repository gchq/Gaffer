/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.serialisation.util;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class LengthValueBytesSerialiserUtilTest {

    @Test
    public void shouldSerialiseAndDeserialiseValue() throws IOException {
        // Given
        final byte[] bytes = StringUtil.toBytes("Some value");

        // When
        final byte[] serialisedBytes = LengthValueBytesSerialiserUtil.serialise(bytes);
        final byte[] deserialisedBytes = LengthValueBytesSerialiserUtil.deserialise(serialisedBytes);

        // Then
        assertArrayEquals(bytes, deserialisedBytes);
    }

    @Test
    public void shouldSerialiseAndDeserialiseValueWithSerialiser() throws IOException {
        // Given
        final String string = "Some value";
        final StringSerialiser serialiser = new StringSerialiser();

        // When
        final byte[] serialisedBytes = LengthValueBytesSerialiserUtil.serialise(serialiser, string);
        final String deserialisedString = LengthValueBytesSerialiserUtil.deserialise(serialiser, serialisedBytes);

        // Then
        assertEquals(string, deserialisedString);
    }

    @Test
    public void shouldSerialiseAndDeserialiseValues() throws IOException {
        // Given
        final byte[] bytes1 = StringUtil.toBytes("Some value 1");
        final byte[] bytes2 = StringUtil.toBytes("Some value 2");
        final byte[] bytes3 = StringUtil.toBytes("Some value 3");

        // When - serialise
        byte[] serialisedBytes;
        try (final ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
            LengthValueBytesSerialiserUtil.serialise(bytes1, byteStream);
            LengthValueBytesSerialiserUtil.serialise(bytes2, byteStream);
            LengthValueBytesSerialiserUtil.serialise(bytes3, byteStream);
            serialisedBytes = byteStream.toByteArray();
        }

        // When - deserialise
        int lastDelimiter = 0;
        final byte[] deserialisedBytes1 = LengthValueBytesSerialiserUtil.deserialise(serialisedBytes, lastDelimiter);
        lastDelimiter = LengthValueBytesSerialiserUtil.getNextDelimiter(serialisedBytes, deserialisedBytes1, lastDelimiter);
        final byte[] deserialisedBytes2 = LengthValueBytesSerialiserUtil.deserialise(serialisedBytes, lastDelimiter);
        lastDelimiter = LengthValueBytesSerialiserUtil.getNextDelimiter(serialisedBytes, deserialisedBytes2, lastDelimiter);
        final byte[] deserialisedBytes3 = LengthValueBytesSerialiserUtil.deserialise(serialisedBytes, lastDelimiter);

        // Then
        assertArrayEquals(bytes1, deserialisedBytes1);
        assertArrayEquals(bytes2, deserialisedBytes2);
        assertArrayEquals(bytes3, deserialisedBytes3);
    }

    @Test
    public void shouldSerialiseAndDeserialiseValuesWithSerialiser() throws IOException {
        // Given
        final ToBytesSerialiser<String> stringSerialiser = new StringSerialiser();
        final String string1 = "Some value 1";
        final String string2 = "Some value 2";
        final String string3 = "Some value 3";

        // When - serialise
        byte[] serialisedBytes;
        try (final ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
            LengthValueBytesSerialiserUtil.serialise(stringSerialiser, string1, byteStream);
            LengthValueBytesSerialiserUtil.serialise(stringSerialiser, string2, byteStream);
            LengthValueBytesSerialiserUtil.serialise(stringSerialiser, string3, byteStream);
            serialisedBytes = byteStream.toByteArray();
        }

        // When - deserialise
        int[] delimiter = {0};
        final String deserialisedString1 = LengthValueBytesSerialiserUtil.deserialise(stringSerialiser, serialisedBytes, delimiter);
        final String deserialisedString2 = LengthValueBytesSerialiserUtil.deserialise(stringSerialiser, serialisedBytes, delimiter);
        final String deserialisedString3 = LengthValueBytesSerialiserUtil.deserialise(stringSerialiser, serialisedBytes, delimiter);

        // Then
        assertEquals(string1, deserialisedString1);
        assertEquals(string2, deserialisedString2);
        assertEquals(string3, deserialisedString3);
    }

    @Test
    public void shouldSerialiseAndDeserialiseNullValue() throws IOException {
        // Given
        final byte[] bytes = null;

        // When
        final byte[] serialisedBytes = LengthValueBytesSerialiserUtil.serialise(bytes);
        final byte[] deserialisedBytes = LengthValueBytesSerialiserUtil.deserialise(serialisedBytes);

        // Then
        assertArrayEquals(new byte[0], deserialisedBytes);
    }

    @Test
    public void shouldSerialiseAndDeserialiseEmptyValue() throws IOException {
        // Given
        final byte[] bytes = new byte[0];

        // When
        final byte[] serialisedBytes = LengthValueBytesSerialiserUtil.serialise(bytes);
        final byte[] deserialisedBytes = LengthValueBytesSerialiserUtil.deserialise(serialisedBytes);

        // Then
        assertArrayEquals(bytes, deserialisedBytes);
    }

    @Test
    public void shouldDeserialiseEmptyValue() throws IOException {
        // Given
        final byte[] bytes = new byte[0];

        // When
        final byte[] deserialisedBytes = LengthValueBytesSerialiserUtil.deserialise(bytes);

        // Then
        assertArrayEquals(bytes, deserialisedBytes);
    }

    @Test
    public void shouldDeserialiseNullValue() throws IOException {
        // Given
        final byte[] bytes = null;

        // When
        final byte[] deserialisedBytes = LengthValueBytesSerialiserUtil.deserialise(bytes);

        // Then
        assertArrayEquals(new byte[0], deserialisedBytes);
    }
}
