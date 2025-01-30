/*
 * Copyright 2017-2024 Crown Copyright
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;

class PropertiesUtilTest {
    private static final String INVALID_STRING = "inv@l1dStr|ng&^";
    private static final String VALID_STRING = "vAl1d-Str|ng";

    @Test
    void shouldThrowExceptionWithInvalidStringName() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> PropertiesUtil.validateName(INVALID_STRING))
                .withMessage("Property is invalid: inv@l1dStr|ng&^, it must match regex: [a-zA-Z0-9|\\-]+");
    }

    @Test
    void shouldPassValidationWithValidStringName() {
        assertThatNoException().isThrownBy(() -> PropertiesUtil.validateName(VALID_STRING));
    }

    @Test
    void shouldBeFalseWithInvalidStringName() {
        assertThat(PropertiesUtil.isValidName(INVALID_STRING)).isFalse();
    }

    @Test
    void shouldBeTrueWithValidStringName() {
        assertThat(PropertiesUtil.isValidName(VALID_STRING)).isTrue();
    }

    @Test
    void shouldStripInvalidCharacters() {
        final String expectedString = "invl1dStr|ng";
        assertThat(PropertiesUtil.stripInvalidCharacters(INVALID_STRING)).isEqualTo(expectedString);
    }
}
