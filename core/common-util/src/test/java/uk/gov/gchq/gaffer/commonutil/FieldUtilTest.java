/*
 * Copyright 2019-2024 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.tuple.n.Tuple3;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class FieldUtilTest {

    @Test
    void testNullFieldPair() {
        final Pair<String, Object> nullPair = new Pair<String, Object>("Test", null);

        final ValidationResult validationResult = FieldUtil.validateRequiredFields(nullPair);

        final Set<String> expected = new LinkedHashSet<>();
        expected.add("Test is required.");
        assertThat(validationResult.getErrors()).isEqualTo(expected);
    }

    @Test
    void testNotNullFieldPair() {
        final Pair<String, Object> nonNullPair = new Pair<String, Object>("Test", "Test");

        final ValidationResult validationResult = FieldUtil.validateRequiredFields(nonNullPair);

        final Set<String> expected = new LinkedHashSet<>();
        assertThat(validationResult.getErrors()).isEqualTo(expected);
    }

     @Test
    void testNullFieldTuple3() {
        final Tuple3 nullTuple3 = new Tuple3<>("Test", null, mock(Predicate.class));

        final ValidationResult validationResult = FieldUtil.validateRequiredFields(nullTuple3);

        final Set<String> expected = new LinkedHashSet<>();
        expected.add("Test");
        assertThat(validationResult.getErrors()).isEqualTo(expected);
    }
}
