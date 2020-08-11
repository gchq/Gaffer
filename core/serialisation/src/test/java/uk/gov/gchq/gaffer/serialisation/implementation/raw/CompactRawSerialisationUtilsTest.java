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
package uk.gov.gchq.gaffer.serialisation.implementation.raw;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import static uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawSerialisationUtils.decodeVIntSize;
import static uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawSerialisationUtils.writeLong;

public class CompactRawSerialisationUtilsTest {

    private static final String LONG_VALUE_IS_LEGAL_TO_NOT_REQUIRE_A_LENGTH_BYTE = "long value is legal to not require a length byte";
    private static final String LONG_VALUE_REQUIRES_A_LENGTH_BYTE = "long value requires a length byte";
    private static final String LENGTH_SHOULD_BE_1_AS_IT_DOES_NOT_REQUIRE_A_LENGTH_BYTE = "Length should be 1, as it does not require a length byte";

    @Test
    public void shouldHaveExtraByteForLengthInFieldVariables() throws Exception {
        final byte[] bytesWithLength = new byte[]{-120, 17, 34, 16, -12, 125, -23, -127, 21};
        assertEquals(Long.BYTES + 1, bytesWithLength.length);
    }

    @Test
    public void shouldNotHaveExtraByteForLength() throws Exception {
        //if (value >= -112 && value <= 127) {
        long lLowerLimit = -112;
        long lUpperLimit = 127;


        byte[] bytesLower = writeLong(lLowerLimit);
        assertEquals(1, bytesLower.length, LONG_VALUE_IS_LEGAL_TO_NOT_REQUIRE_A_LENGTH_BYTE);
        assertEquals(bytesLower.length, decodeVIntSize(bytesLower[0]), LONG_VALUE_IS_LEGAL_TO_NOT_REQUIRE_A_LENGTH_BYTE);

        byte[] bytesUpper = writeLong(lUpperLimit);
        assertEquals(1, bytesUpper.length, LENGTH_SHOULD_BE_1_AS_IT_DOES_NOT_REQUIRE_A_LENGTH_BYTE);
        assertEquals(bytesUpper.length, decodeVIntSize(bytesUpper[0]), LENGTH_SHOULD_BE_1_AS_IT_DOES_NOT_REQUIRE_A_LENGTH_BYTE);
    }

    @Test
    public void shouldHaveExtraByteForLength() throws Exception {
        long lExceedLowerLimit = -113;
        long lExceedUpperLimit = 128;

        byte[] bytesLower = writeLong(lExceedLowerLimit);
        assertEquals(2, bytesLower.length, LONG_VALUE_REQUIRES_A_LENGTH_BYTE);
        assertEquals(bytesLower.length, decodeVIntSize(bytesLower[0]), LONG_VALUE_REQUIRES_A_LENGTH_BYTE);

        byte[] bytesUpper = writeLong(lExceedUpperLimit);
        assertNotEquals(1, bytesUpper.length, "Length should not be 1, as it does require a length byte");
        assertEquals(bytesUpper.length, decodeVIntSize(bytesUpper[0]));
    }


}
