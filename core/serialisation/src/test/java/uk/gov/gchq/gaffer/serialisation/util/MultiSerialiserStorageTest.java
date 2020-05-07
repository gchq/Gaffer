/*
 * Copyright 2018-2020 Crown Copyright
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.serialisation.IntegerSerialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.MultiSerialiserStorage;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.RawIntegerSerialiser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class MultiSerialiserStorageTest {

    public static final byte BYTE = (byte) 0;
    public static final ToBytesSerialiser SERIALISER_CLASS = new IntegerSerialiser();
    public static final int VALUE = 1;
    public static final ToBytesSerialiser SERIALISER_CLASS2 = new RawIntegerSerialiser();
    public static final Class SUPPORTED_CLASS = Integer.class;

    private MultiSerialiserStorage multiSerialiserStorage;

    @BeforeEach
    public void setUp() throws Exception {
        multiSerialiserStorage = new MultiSerialiserStorage();
    }

    @Test
    public void shouldPutAndGet() throws Exception {
        multiSerialiserStorage.put(BYTE, SERIALISER_CLASS, SUPPORTED_CLASS);

        assertPutInvoked();
    }

    @Test
    public void shouldNotRetainOldSerialiserWhenKeyIsOverWritten() throws Exception {
        // When
        multiSerialiserStorage.put(BYTE, SERIALISER_CLASS, SUPPORTED_CLASS);
        multiSerialiserStorage.put(BYTE, SERIALISER_CLASS2, SUPPORTED_CLASS);

        // Then
        assertNotNull(multiSerialiserStorage.getKeyFromValue(VALUE));
        assertEquals((Object) BYTE, multiSerialiserStorage.getKeyFromValue(VALUE), "Wrong key for value");

        ToBytesSerialiser actualClassFromByte = multiSerialiserStorage.getSerialiserFromKey(BYTE);
        assertNotNull(actualClassFromByte, "Byte key not found");
        assertEquals(SERIALISER_CLASS2, actualClassFromByte, "Wrong new SerialiserClass returned for key");

        ToBytesSerialiser actualClassFromValue = multiSerialiserStorage.getSerialiserFromValue(Integer.MAX_VALUE);
        assertNotNull(actualClassFromValue, "Value class not found");
        assertEquals(SERIALISER_CLASS2, actualClassFromValue, "Wrong new SerialiserClass returned for value class");
    }

    @Test
    public void shouldUpdateToNewerValueToSerialiser() throws Exception {
        final byte serialiserEncoding = BYTE + 1;

        multiSerialiserStorage.put(serialiserEncoding, SERIALISER_CLASS2, SUPPORTED_CLASS);
        multiSerialiserStorage.put(BYTE, SERIALISER_CLASS, SUPPORTED_CLASS);

        // Then
        assertPutInvoked();
        assertEquals(BYTE, (byte) multiSerialiserStorage.getKeyFromValue(VALUE));

        ToBytesSerialiser actualClassFromByte2 = multiSerialiserStorage.getSerialiserFromKey(serialiserEncoding);
        assertNotNull(actualClassFromByte2, "Byte key not found");
        assertEquals(SERIALISER_CLASS2, actualClassFromByte2, "Wrong SerialiserClass returned for key");

        ToBytesSerialiser actualClassFromValue2 = multiSerialiserStorage.getSerialiserFromValue(Integer.MAX_VALUE);
        assertNotNull(actualClassFromValue2, "Value class not found");
        assertEquals(SERIALISER_CLASS, actualClassFromValue2, "Wrong SerialiserClass, should have updated to newer SerialiserClass");
    }

    private void assertPutInvoked() {
        assertEquals((Object) BYTE, multiSerialiserStorage.getKeyFromValue(VALUE));

        ToBytesSerialiser actualClassFromByte = multiSerialiserStorage.getSerialiserFromKey(BYTE);
        assertNotNull(actualClassFromByte, "Byte key not found");
        assertEquals(SERIALISER_CLASS, actualClassFromByte, "Wrong SerialiserClass returned for key");

        ToBytesSerialiser actualClassFromValue = multiSerialiserStorage.getSerialiserFromValue(Integer.MAX_VALUE);
        assertNotNull(actualClassFromValue, "Value class not found");
        assertEquals(SERIALISER_CLASS, actualClassFromValue, "Wrong SerialiserClass returned for value class");
    }

}
