/*
 * Copyright 2019 Crown Copyright
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

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class FieldUtilTest {

    /**
     * Compares the error set returned by the ValidationResult
     */
    @Test
    public void testNullField() {

        //Given
        Pair nullPair = new Pair("Test", null);
        Set<String> testErrorSet = new LinkedHashSet<>();
        testErrorSet.add("Test is required.");

        //When
        ValidationResult validationResult = FieldUtil.validateRequiredFields(nullPair);

        //Then
        assertEquals(validationResult.getErrors(), testErrorSet);
    }

    /**
     * Compares the empty error set returned by the ValidationResult
     */
    @Test
    public void testNotNullField() {

        //Given
        Pair nonNullPair = new Pair("Test", "Test");
        Set<String> testNoErrorSet = new LinkedHashSet<>();

        //When
        ValidationResult validationResult = FieldUtil.validateRequiredFields(nonNullPair);

        //Then
        assertEquals(validationResult.getErrors(), testNoErrorSet);

    }
}
