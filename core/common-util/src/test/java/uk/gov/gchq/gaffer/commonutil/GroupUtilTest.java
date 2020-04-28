/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GroupUtilTest {
    private static final String INVALID_STRING = "inv@l1dStr|ng&^";
    private static final String VALID_STRING = "vAl1d-Str|ng";

    @Test
    public void shouldThrowExceptionWithInvalidStringName() {
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> GroupUtil.validateName(INVALID_STRING));

        final String expected = "Group is invalid: inv@l1dStr|ng&^, it must match regex: [a-zA-Z0-9|-]*";
        assertEquals(expected, exception.getMessage());
    }

    @Test
    public void shouldPassValidationWithValidStringName() {
        assertDoesNotThrow(() -> GroupUtil.validateName(VALID_STRING));
    }
}
