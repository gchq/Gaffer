/*
 * Copyright 2023 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.serialisation.json;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.datasketches.hll.HllSketch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.BooleanSerialiser;
import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;
import uk.gov.gchq.gaffer.types.CustomMap;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.gaffer.types.TypeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class HllSketchJsonDeserialiserTest {

    @BeforeEach
    void before() {
        System.setProperty(JSONSerialiser.JSON_SERIALISER_MODULES, SketchesJsonModules.class.getName());
        JSONSerialiser.update();
    }

    @AfterEach
    void after() {
        System.clearProperty(JSONSerialiser.JSON_SERIALISER_MODULES);
        JSONSerialiser.update();
    }

    @Test
    void shouldFailDeserialisingHllSketchNull() throws SerialisationException {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> JSONSerialiser.deserialise((String) null, HllSketch.class))
                .withMessage("argument \"content\" is null");
    }

    @Test
    void shouldFailDeserialisingHllSketchEmptyString() throws SerialisationException {
        assertThatExceptionOfType(SerialisationException.class)
                .isThrownBy(() -> JSONSerialiser.deserialise("  ", HllSketch.class))
                .withMessageContaining("No content to map due to end-of-input");
    }

    @Test
    void shouldDeserialiseHllSketchAsEmptySketch() throws IOException {
        // Given
        final String json = "{}";

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(json, HllSketch.class);

        // Then
        assertThat(hllSketch.getEstimate()).isEqualTo(0);
    }

    @Test
    void shouldFailDeserialisingHllSketchEmptyInvalidBytes() {
        // Given
        final String json = String.format("{\"%s\": \"fail\"}",
                HllSketchJsonConstants.BYTES);

        // When / Then
        assertThatExceptionOfType(SerialisationException.class)
                .isThrownBy(() -> JSONSerialiser.deserialise(json, HllSketch.class))
                .withMessageContaining("Unexpected IOException (of type uk.gov.gchq.gaffer.exception.SerialisationException): " +
                        "Error deserialising JSON object: Bounds Violation");
    }

    @Test
    void shouldDeserialiseHllSketchWithLogKValuesAndValueStoredAsBytes() throws IOException {
        // Given
        final HllSketch expected = new HllSketch(10);
        expected.update("test");

        final Map<String, Object> stringMap = Collections.singletonMap(HllSketchJsonConstants.BYTES,
                expected.toCompactByteArray());

        final String s = JSONSerialiser.getMapper().writeValueAsString(stringMap);

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(s, HllSketch.class);

        // Then
        assertThat(hllSketch.toCompactByteArray()).isEqualTo(expected.toCompactByteArray());
    }

    @Test
    void shouldDeserialiseHllSketchWithLogKValue() throws IOException {
        // Given
        final String json = "{\"logK\": 5}";

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(json, HllSketch.class);

        // Then
        assertThat(hllSketch.toCompactByteArray()).isEqualTo(new HllSketch(5).toCompactByteArray());
    }

    @Test
    void shouldDeserialiseHllSketchWithLogKValueAndValuesEmpty() throws IOException {
        // Given
        final String json = "{\"p\": 5, \"sp\": 10, \"values\": []}";

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(json, HllSketch.class);

        // Then
        assertThat(hllSketch.toCompactByteArray()).isEqualTo(new HllSketch(10).toCompactByteArray());
    }

    @Test
    void shouldFailDeserialisingHllSketchWithValuesInvalidObject() throws IOException {
        // Given
        final String json = "{\"logK\": 10, " +
                "\"values\": [" +
                "[{\"class\": \"fail\", \"fail\": \"fail\"}]" +
                "]}";

        // When
        // Then
        assertThatExceptionOfType(SerialisationException.class)
                .isThrownBy(() -> JSONSerialiser.deserialise(json, HllSketch.class))
                .withMessageContaining("Error deserialising JSON object: Unexpected token");
    }

    @Test
    void shouldDeserialiseHllSketchWithValues() throws IOException {
        // Given
        final String json = "{\"logK\": 10, \"values\": [\"value1\", \"value2\", \"value2\", \"value2\", \"value3\"]}";

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(json, HllSketch.class);

        // Then
        final HllSketch expected = new HllSketch(10);
        expected.update("value1");
        expected.update("value2");
        expected.update("value2");
        expected.update("value2");
        expected.update("value3");
        assertArrayEquals(expected.toCompactByteArray(), hllSketch.toCompactByteArray());
    }

    @Test
    void shouldDeserialiseHllSketchWithValuesTypeSubTypeValue() throws IOException {
        // Given
        final String json = "{\"logK\": 10, " +
                "\"values\": [" +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeSubTypeValue\", \"type\": \"type1\", \"subType\": \"subType1\", \"value\": \"value1\"}, " +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeSubTypeValue\", \"type\": \"type2\", \"subType\": \"subType2\", \"value\": \"value2\"}, " +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeSubTypeValue\", \"type\": \"type3\", \"subType\": \"subType3\", \"value\": \"value3\"}" +
                "]}";

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(json, HllSketch.class);

        // Then
        final HllSketch expected = new HllSketch(10);
        expected.update(new TypeSubTypeValue("type1", "subType1", "value1").toString());
        expected.update(new TypeSubTypeValue("type2", "subType2", "value2").toString());
        expected.update(new TypeSubTypeValue("type3", "subType3", "value3").toString());
        assertThat(hllSketch.toCompactByteArray()).isEqualTo(expected.toCompactByteArray());
    }

    @Test
    void shouldDeserialiseHllSketchWithValuesTypeValue() throws IOException {
        // Given
        final String json = "{\"logK\": 10, " +
                "\"values\": [" +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeValue\", \"type\": \"type1\", \"value\": \"value1\"}, " +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeValue\", \"type\": \"type2\", \"value\": \"value2\"}, " +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeValue\", \"type\": \"type3\", \"value\": \"value3\"}" +
                "]}";

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(json, HllSketch.class);

        // Then
        final HllSketch expected = new HllSketch(10);
        expected.update(new TypeValue("type1", "value1").toString());
        expected.update(new TypeValue("type2", "value2").toString());
        expected.update(new TypeValue("type3", "value3").toString());
        assertThat(hllSketch.toCompactByteArray()).isEqualTo(expected.toCompactByteArray());
    }

    @Test
    void shouldDeserialiseHllSketchWithValuesTypeFreqMap() throws IOException {
        // Given
        final String json = "{\"logK\": 10, " +
                "\"values\": [" +
                "{\"class\": \"" + FreqMap.class.getName() + "\", \"a\": 1, \"b\": 1234567891011121314}" +
                "]}";

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(json, HllSketch.class);

        // Then
        final FreqMap freqMap = new FreqMap();
        freqMap.put("a", 1L);
        freqMap.put("b", 1234567891011121314L);

        final HllSketch expected = new HllSketch(10);
        expected.update(freqMap.toString());
        assertThat(hllSketch.toCompactByteArray()).isEqualTo(expected.toCompactByteArray());
    }

    @Test
    void shouldDeserialiseHllSketchWithValuesInteger() throws IOException {
        // Given
        final String json = "{\"logK\": 10, " +
                "\"values\": [1, 2, 3]}";

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(json, HllSketch.class);

        // Then
        final HllSketch expected = new HllSketch(10);
        expected.update(1);
        expected.update(2);
        expected.update(3);
        assertThat(hllSketch.toCompactByteArray()).isEqualTo(expected.toCompactByteArray());
    }

    @Test
    void shouldDeserialiseHllSketchWithValuesString() throws IOException {
        // Given
        final String json = "{\"logK\": 10, " +
                "\"values\": [\"one\", \"two\", \"three\"]}";

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(json, HllSketch.class);

        // Then
        final HllSketch expected = new HllSketch(10);
        expected.update("one");
        expected.update("two");
        expected.update("three");
        assertThat(hllSketch.toCompactByteArray()).isEqualTo(expected.toCompactByteArray());
    }

    @Test
    void shouldDeserialiseHllSketchWithValuesFloatAsDoubles() throws IOException {
        // Given
        final String json = "{\"logK\": 10, " +
                "\"values\": [1.2, 2.12345, 3.123456789]}";

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(json, HllSketch.class);

        // Then
        final HllSketch expected = new HllSketch(10);
        // values are serialised as Doubles
        expected.update(1.2D);
        expected.update(2.12345D);
        expected.update(3.123456789D);
        assertThat(hllSketch.toCompactByteArray()).isEqualTo(expected.toCompactByteArray());
    }

    @Test
    void shouldDeserialiseHllSketchWithValuesLong() throws IOException {
        // Given
        final String json = "{\"logK\": 10, " +
                "\"values\": [1234567891011121314]}";

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(json, HllSketch.class);

        // Then
        final HllSketch expected = new HllSketch(10);
        expected.update(1234567891011121314L);
        assertThat(hllSketch.toCompactByteArray()).isEqualTo(expected.toCompactByteArray());
    }

    @Test
    void shouldDeserialiseHllSketchWithValuesDouble() throws IOException {
        // Given
        final String json = "{\"logK\": 10, " +
                "\"values\": [1.2, 2.12345, 3.123456789]}";

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(json, HllSketch.class);

        // Then
        final HllSketch expected = new HllSketch(10);
        expected.update(1.2D);
        expected.update(2.12345D);
        expected.update(3.123456789D);
        assertThat(hllSketch.toCompactByteArray()).isEqualTo(expected.toCompactByteArray());
    }

    @Test
    void shouldDeserialiseHllSketchWithValuesTestObjectNoAnnotations() throws IOException {
        // Given
        final String json = "{\"logK\": 10, " +
                "\"values\": [{\"class\": \"" + TestObject.class.getName() + "\", \"testString\": \"test\"}]}";

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(json, HllSketch.class);

        // Then

        final HllSketch expected = new HllSketch(10);
        expected.update(new TestObject("test").toString());
        assertThat(hllSketch.toCompactByteArray()).isEqualTo(expected.toCompactByteArray());
    }

    @Test
    void shouldDeserialiseHllSketchWithValuesTestJavaToJson() throws IOException {
        // Given
        final HllSketchWithValues HllSketchWithValues = new HllSketchWithValues();
        HllSketchWithValues.setLogK(10);
        final ArrayList<Object> values = new ArrayList<>();
        values.add(new TypeSubTypeValue("type", "subType", "value"));
        values.add(new TypeValue("type", "value"));
        values.add(new CustomMap<>(new BooleanSerialiser(), new BooleanSerialiser()));
        values.add(new FreqMap("test"));
        HllSketchWithValues.setValues(values);
        final byte[] serialised = JSONSerialiser.serialise(HllSketchWithValues, true);

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(serialised, HllSketch.class);

        // Then
        final HllSketch expected = new HllSketch(10);
        expected.update(new TypeSubTypeValue("type", "subType", "value").toString());
        expected.update(new TypeValue("type", "value").toString());
        expected.update(new CustomMap<>(new BooleanSerialiser(), new BooleanSerialiser()).toString());
        expected.update(new FreqMap("test").toString());
        assertThat(hllSketch.toCompactByteArray()).isEqualTo(expected.toCompactByteArray());
    }

    @Test
    void shouldDeserialiseHllSketchWithValuesAll() throws IOException {
        // Given
        final String json = "{\"logK\": 10, " +
                "\"values\": [" +
                "1, " +
                "\"string\",  " +
                "1.12345, " +
                "1.123456789, " +
                "1234567891011121314, " +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeValue\", \"type\": \"type\", \"value\": \"value\"}, " +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeSubTypeValue\", \"type\": \"type\", \"subType\": \"subType\", \"value\": \"value\"}" +
                "]}";

        // When
        final HllSketch hllSketch = JSONSerialiser.deserialise(json, HllSketch.class);

        // Then
        final HllSketch expected = new HllSketch(10);
        expected.update(1);
        expected.update("string");
        expected.update(1.12345D);
        expected.update(1.123456789D);
        expected.update(1234567891011121314L);
        expected.update(new TypeValue("type", "value").toString());
        expected.update(new TypeSubTypeValue("type", "subType", "value").toString());
        assertThat(hllSketch.toCompactByteArray()).isEqualTo(expected.toCompactByteArray());
    }

    public static class TestObject {

        private String testString = null;

        public TestObject() {
        }

        public TestObject(final String testString) {
            this.testString = testString;
        }

        public String getTestString() {
            return testString;
        }

        public void setTestString(final String testString) {
            this.testString = testString;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TestObject that = (TestObject) o;
            return new EqualsBuilder()
                    .append(getTestString(), that.getTestString())
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(47, 85)
                    .append(getTestString())
                    .hashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("testString", getTestString())
                    .toString();
        }
    }
}
