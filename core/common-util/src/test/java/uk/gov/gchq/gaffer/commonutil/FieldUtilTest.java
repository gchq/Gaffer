/*
 * Copyright 2019-2020 Crown Copyright
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

import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FieldUtilTest {

    @Test
    public void testNullField() {
        final Pair nullPair = new Pair("Test", null);

        final ValidationResult validationResult = FieldUtil.validateRequiredFields(nullPair);

        final Set<String> expected = new LinkedHashSet<>();
        expected.add("Test is required.");
        assertEquals(expected, validationResult.getErrors());
    }

    @Test
    public void testNotNullField() {
        final Pair nonNullPair = new Pair("Test", "Test");

        final ValidationResult validationResult = FieldUtil.validateRequiredFields(nonNullPair);

        final Set<String> expected = new LinkedHashSet<>();
        assertEquals(expected, validationResult.getErrors());
    }
}
