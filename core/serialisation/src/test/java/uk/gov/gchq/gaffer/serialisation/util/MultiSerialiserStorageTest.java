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

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
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
    private MultiSerialiserStorage mss;


    @BeforeEach
    public void setUp() throws Exception {
        mss = new MultiSerialiserStorage();
    }

    @Test
    public void shouldPutAndGet() throws Exception {
        //when
        mss.put(BYTE, SERIALISER_CLASS, SUPPORTED_CLASS);
        //then
        checkBasicPut();
    }

    @Test
    public void shouldNotRetainOldSerialiserWhenKeyIsOverWritten() throws Exception {
        //when
        mss.put(BYTE, SERIALISER_CLASS, SUPPORTED_CLASS);
        mss.put(BYTE, SERIALISER_CLASS2, SUPPORTED_CLASS);
        //then
        assertNotNull(mss.getKeyFromValue(VALUE));
        assertEquals((Object) BYTE, mss.getKeyFromValue(VALUE), "Wrong key for value");
        ToBytesSerialiser actualClassFromByte = mss.getSerialiserFromKey(BYTE);
        assertNotNull(actualClassFromByte, "Byte key not found");
        assertEquals(SERIALISER_CLASS2, actualClassFromByte, "Wrong new SerialiserClass returned for key");
        ToBytesSerialiser actualClassFromValue = mss.getSerialiserFromValue(Integer.MAX_VALUE);
        assertNotNull(actualClassFromValue, "Value class not found");
        assertEquals(SERIALISER_CLASS2, actualClassFromValue, "Wrong new SerialiserClass returned for value class");
    }


    @Test
    public void shouldUpdateToNewerValueToSerialiser() throws Exception {
        //give
        byte serialiserEncoding = BYTE + 1;
        //when
        mss.put(serialiserEncoding, SERIALISER_CLASS2, SUPPORTED_CLASS);
        mss.put(BYTE, SERIALISER_CLASS, SUPPORTED_CLASS);
        //then
        checkBasicPut();

        assertEquals(BYTE, (byte) mss.getKeyFromValue(VALUE));
        ToBytesSerialiser actualClassFromByte2 = mss.getSerialiserFromKey(serialiserEncoding);
        assertNotNull(actualClassFromByte2, "Byte key not found");
        assertEquals(SERIALISER_CLASS2, actualClassFromByte2, "Wrong SerialiserClass returned for key");

        ToBytesSerialiser actualClassFromValue2 = mss.getSerialiserFromValue(Integer.MAX_VALUE);
        assertNotNull(actualClassFromValue2, "Value class not found");
        assertEquals(SERIALISER_CLASS, actualClassFromValue2,
                "Wrong SerialiserClass, should have updated to newer SerialiserClass");
    }

    private void checkBasicPut() throws GafferCheckedException {
        assertEquals((Object) BYTE, mss.getKeyFromValue(VALUE));
        ToBytesSerialiser actualClassFromByte = mss.getSerialiserFromKey(BYTE);
        assertNotNull(actualClassFromByte, "Byte key not found");
        assertEquals(SERIALISER_CLASS, actualClassFromByte, "Wrong SerialiserClass returned for key");
        ToBytesSerialiser actualClassFromValue = mss.getSerialiserFromValue(Integer.MAX_VALUE);
        assertNotNull(actualClassFromValue, "Value class not found");
        assertEquals(SERIALISER_CLASS, actualClassFromValue, "Wrong SerialiserClass returned for value class");
    }

}
