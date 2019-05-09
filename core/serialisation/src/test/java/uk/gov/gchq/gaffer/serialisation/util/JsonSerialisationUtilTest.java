/*
 * Copyright 2017-2019 Crown Copyright
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
package uk.gov.gchq.gaffer.serialisation.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.junit.Test;

import uk.gov.gchq.koryphe.impl.predicate.IsIn;
import uk.gov.gchq.koryphe.impl.predicate.range.InDateRange;
import uk.gov.gchq.koryphe.impl.predicate.range.InRange;
import uk.gov.gchq.koryphe.impl.predicate.range.InTimeRange;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JsonSerialisationUtilTest {
    @Test
    public void testClassWithNoFields() {
        // Given

        final String className = ClassWithNoFields.class.getName();

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then
        assertTrue(result.entrySet().isEmpty());
    }

    @Test
    public void testClassWithJsonAnnotations() {
        // Given
        final String className = ClassWithAnnotations.class.getName();
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("field1", String.class.getName());

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then]
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }

    @Test
    public void testClassWithCreator() {
        // Given
        final String className = ClassWithCreator.class.getName();
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("field1", String.class.getName());

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then]
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }

    @Test
    public void testClassWithBuilder() {
        // Given
        final String className = ClassWithBuilder.class.getName();
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("field1", String.class.getName());

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then]
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }

    @Test
    public void testIsIn() {
        // Given
        final String className = IsIn.class.getName();
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("values", "java.lang.Object[]");

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }

    @Test
    public void testInRange() {
        // Given
        final String className = InRange.class.getName();
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("start", "java.lang.Comparable<T>");
        expectedValues.put("end", "java.lang.Comparable<T>");
        expectedValues.put("startInclusive", Boolean.class.getName());
        expectedValues.put("endInclusive", Boolean.class.getName());

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }

    @Test
    public void testInDateRangeAndInTimeRange() {
        // Given
        final String classNameIDR = InDateRange.class.getName();
        final String classNameITR = InTimeRange.class.getName();
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("timeUnit", String.class.getName());
        expectedValues.put("offsetUnit", String.class.getName());
        expectedValues.put("start", String.class.getName());
        expectedValues.put("startOffset", Long.class.getName());
        expectedValues.put("startInclusive", Boolean.class.getName());
        expectedValues.put("end", String.class.getName());
        expectedValues.put("endOffset", Long.class.getName());
        expectedValues.put("endInclusive", Boolean.class.getName());
        expectedValues.put("timeZone", String.class.getName());

        // When
        final Map<String, String> resultIDR = JsonSerialisationUtil.getSerialisedFieldClasses(classNameIDR);
        final Map<String, String> resultITR = JsonSerialisationUtil.getSerialisedFieldClasses(classNameITR);

        // Then
        assertEquals(expectedValues.entrySet(), resultIDR.entrySet());
        assertEquals(resultIDR.entrySet(), resultITR.entrySet());
    }

    @Test
    public void testClassWithTypeParamAndOtherField() {
        // Given
        final String classWithTypeParamAndOtherFieldName = ClassWithTypeParamAndOtherField.class.getName();
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("test", String.class.getName());
        expectedValues.put("t", Object.class.getName());

        // When
        final Map<String, String> result =
                JsonSerialisationUtil.getSerialisedFieldClasses(classWithTypeParamAndOtherFieldName);

        // Then
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }

    @Test
    public void testClassWithJustTypeParam() {
        // Given
        final String classWithTypeParamName = ClassWithTypeParam.class.getName();
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("t", Object.class.getName());

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(classWithTypeParamName);

        // Then
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }

    @Test
    public void testClassWithTypeParamExtendingComparable() {
        // Given
        final String classWithTypeParamExtendingComparableName =
                ClassWithTypeParamExtendingComparable.class.getName();
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("t", Comparable.class.getName());

        // When
        final Map<String, String> result =
                JsonSerialisationUtil.getSerialisedFieldClasses(classWithTypeParamExtendingComparableName);

        // Then
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }

    private static final class ClassWithNoFields {
    }

    private static final class ClassWithAnnotations {
        private String field1;
        private String field2;

        public String getField1() {
            return field1;
        }

        public void setField1(final String field1) {
            this.field1 = field1;
        }

        @JsonIgnore
        public String getField2() {
            return field2;
        }

        public void setField2(final String field2) {
            this.field2 = field2;
        }

        @JsonGetter("field1")
        String getField1Json() {
            return field1;
        }

        @JsonSetter("field1")
        void setField1Json(final String field1) {
            this.field1 = field1;
        }
    }

    private static final class ClassWithCreator {
        private String field1;

        @JsonCreator
        ClassWithCreator(@JsonProperty("field1") final String field1) {
            this.field1 = field1;
        }

        public String getField1() {
            return field1;
        }
    }

    @JsonDeserialize(builder = ClassWithBuilder.Builder.class)
    private static final class ClassWithBuilder {
        private String field1;

        public String getField1() {
            return field1;
        }

        @JsonPOJOBuilder(withPrefix = "")
        static class Builder {
            private String field1;

            public Builder field1(final String field1) {
                this.field1 = field1;
                return this;
            }

            public ClassWithBuilder build() {
                final ClassWithBuilder result = new ClassWithBuilder();
                result.field1 = this.field1;
                return result;
            }
        }
    }

    private static final class ClassWithTypeParamAndOtherField<T> {
        private T t;
        private String test;

        public T getT() {
            return t;
        }

        public void setT(final T t) {
            this.t = t;
        }

        public String getTest() {
            return test;
        }

        public void setTest(final String test) {
            this.test = test;
        }
    }

    private static final class ClassWithTypeParam<T> {
        private T t;

        public T getT() {
            return t;
        }

        public void setT(final T t) {
            this.t = t;
        }
    }


    private static final class ClassWithTypeParamExtendingComparable<T extends Comparable> {
        private T t;

        public T getT() {
            return t;
        }

        public void setT(final T t) {
            this.t = t;
        }
    }
}
