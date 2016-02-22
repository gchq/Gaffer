/*
 * Copyright 2016 Crown Copyright
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

package gaffer.function;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class FilterFunctionTest extends ConsumerFunctionTest {
    @Test
    public void shouldSetAndGetIsNot() {
        // Given
        final FilterFunction function = getInstance();

        // When 1
        function.setNot(true);
        final boolean isNot1 = function.isNot();

        // Then 2
        assertTrue(isNot1);

        // When 2
        function.setNot(false);
        final boolean isNot2 = function.isNot();

        // Then 3
        assertFalse(isNot2);
    }

    @Test
    public void shouldReturnFilterValueWhenIsNotIsFalse() {
        // Given
        final FilterFunction function = getInstance();
        function.setNot(false);

        // When
        final boolean result = function.isValid(getSomeAcceptedInput());

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldReturnOppositeFilterValueWhenIsNotIsTrue() {
        // Given
        final FilterFunction function = getInstance();
        function.setNot(true);

        // When
        final boolean result = function.isValid(getSomeAcceptedInput());

        // Then
        assertFalse(result);
    }

    protected abstract FilterFunction getInstance();

    protected abstract Object[] getSomeAcceptedInput();
}
