/*
 * Copyright 2016-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil.iterable;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import uk.gov.gchq.koryphe.ValidationResult;

import static org.assertj.core.api.Assertions.assertThat;

class AlwaysValidTrueTest {

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"test"})
    void shouldReturnTrueForNullAndString(final String input) {
        final AlwaysValid<String> validator = new AlwaysValid<>();

        final boolean result = validator.validate(input);

        assertThat(result).isTrue();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"test"})
    void shouldReturnValidationResultForNullAndString(final String input) {
        final AlwaysValid<String> validator = new AlwaysValid<>();

        final ValidationResult result = validator.validateWithValidationResult(input);

        assertThat(result.isValid()).isTrue();
    }
}
