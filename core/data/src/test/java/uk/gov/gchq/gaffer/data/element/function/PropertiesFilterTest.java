/*
 * Copyright 2021 Crown Copyright
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

package uk.gov.gchq.gaffer.data.element.function;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.tuple.predicate.KoryphePredicate2;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PropertiesFilterTest extends JSONSerialisationTest<PropertiesFilter> {

    @Test
    public void shouldTestPropertiesOnPredicate2() {
        // Given
        final PropertiesFilter propertiesFilter = new PropertiesFilter.Builder()
                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                .execute(new KoryphePredicate2<String, String>() {
                    @Override
                    public boolean test(final String o, final String o2) {
                        return "value".equals(o) && "value2".equals(o2);
                    }
                })
                .build();

        final Properties correctProperties = new Properties();
        correctProperties.put(TestPropertyNames.PROP_1, "value");
        correctProperties.put(TestPropertyNames.PROP_2, "value2");

        final Properties incorrectProperties = new Properties();
        incorrectProperties.put(TestPropertyNames.PROP_1, "value_incorrect");
        incorrectProperties.put(TestPropertyNames.PROP_2, "value2_incorrect");

        // When
        final boolean correctResult = propertiesFilter.test(correctProperties);
        final boolean incorrectResult = propertiesFilter.test(incorrectProperties);

        // Then
        assertTrue(correctResult);
        assertFalse(incorrectResult);
    }

    @Test
    public void shouldTestPropertiesOnPredicate2WithValidationResult() {
        // Given
        final PropertiesFilter propertiesFilter = new PropertiesFilter.Builder()
                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                .execute(new KoryphePredicate2<String, String>() {
                    @Override
                    public boolean test(final String o, final String o2) {
                        return "value".equals(o) && "value2".equals(o2);
                    }
                })
                .build();


        final Properties correctProperties = new Properties();
        correctProperties.put(TestPropertyNames.PROP_1, "value");
        correctProperties.put(TestPropertyNames.PROP_2, "value2");

        final Properties incorrectProperties = new Properties();
        incorrectProperties.put(TestPropertyNames.PROP_1, "value_incorrect");
        incorrectProperties.put(TestPropertyNames.PROP_2, "value2_incorrect");

        // When
        final ValidationResult correctResult = propertiesFilter.testWithValidationResult(correctProperties);
        final ValidationResult incorrectResult = propertiesFilter.testWithValidationResult(incorrectProperties);

        // Then
        assertTrue(correctResult.isValid());
        assertFalse(incorrectResult.isValid());
        assertTrue(incorrectResult.getErrorString().contains("{property1: <java.lang.String>value_incorrect, property2: <java.lang.String>value2_incorrect}"), "Result was: " + incorrectResult.getErrorString());
    }

    @Override
    protected PropertiesFilter getTestObject() {
        return new PropertiesFilter();
    }
}
