/*
 * Copyright 2016-2018 Crown Copyright
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

import org.junit.Test;

import uk.gov.gchq.koryphe.ValidationResult;

import static org.junit.Assert.assertTrue;

public class AlwaysValidTrue {

    @Test
    public void shouldReturnTrueForNull() {
        // Given
        final AlwaysValid<?> validator = new AlwaysValid<>();

        // When
        final boolean result = validator.validate(null);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldReturnTrueForString() {
        // Given
        final AlwaysValid<String> validator = new AlwaysValid<>();

        // When
        final boolean result = validator.validate("test");

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldReturnValidationResultForNull() {
        // Given
        final AlwaysValid<?> validator = new AlwaysValid<>();

        // When
        final ValidationResult result = validator.validateWithValidationResult(null);

        // Then
        assertTrue(result.isValid());
    }

    @Test
    public void shouldReturnValidationResultForString() {
        // Given
        final AlwaysValid<String> validator = new AlwaysValid<>();

        // When
        final ValidationResult result = validator.validateWithValidationResult("test");

        // Then
        assertTrue(result.isValid());
    }
}
