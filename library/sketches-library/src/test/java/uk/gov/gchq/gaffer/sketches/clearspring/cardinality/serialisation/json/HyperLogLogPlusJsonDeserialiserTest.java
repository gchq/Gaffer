/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.json;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
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

public class HyperLogLogPlusJsonDeserialiserTest {

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
    void shouldFailDeserialisingHllpNull() throws SerialisationException {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> JSONSerialiser.deserialise((String) null, HyperLogLogPlus.class))
                .withMessage("argument \"content\" is null");
    }

    @Test
    void shouldFailDeserialisingHllpEmptyString() throws SerialisationException {
        assertThatExceptionOfType(SerialisationException.class)
                .isThrownBy(() -> JSONSerialiser.deserialise("  ", HyperLogLogPlus.class))
                .withMessageContaining("No content to map due to end-of-input");
    }

    @Test
    void shouldDeserialiseHllpAsEmptySketch() throws IOException {
        // Given
        final String json = "{}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then
        assertThat(hllp.cardinality()).isEqualTo(0);
    }

    @Test
    void shouldFailDeserialisingHllpEmptyInvalidBytes() {
        // Given
        final String json = String.format("{\"%s\": \"fail\"}",
                HyperLogLogPlusJsonConstants.HYPER_LOG_LOG_PLUS_SKETCH_BYTES_FIELD);

        assertThatExceptionOfType(SerialisationException.class)
                .isThrownBy(() -> JSONSerialiser.deserialise(json, HyperLogLogPlus.class))
                .withMessageContaining("Unexpected IOException (of type uk.gov.gchq.gaffer.exception.SerialisationException): " +
                        "Error deserialising JSON object: null");
    }

    @Test
    void shouldDeserialiseHllpWithSAndSpValuesAndOfferStoredAsBytes() throws IOException {
        // Given
        final HyperLogLogPlus expected = new HyperLogLogPlus(5, 10);
        expected.offer("test");

        final Map<String, Object> stringMap = Collections.singletonMap(HyperLogLogPlusJsonConstants.HYPER_LOG_LOG_PLUS_SKETCH_BYTES_FIELD,
                expected.getBytes());

        final String s = JSONSerialiser.getMapper().writeValueAsString(stringMap);

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(s, HyperLogLogPlus.class);

        // Then
        assertThat(hllp.getBytes()).isEqualTo(expected.getBytes());
    }

    @Test
    void shouldDeserialiseHllpWithSAndSpValues() throws IOException {
        // Given
        final String json = "{\"p\": 5, \"sp\": 10}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then
        assertThat(hllp.getBytes()).isEqualTo(new HyperLogLogPlus(5, 10).getBytes());
    }

    @Test
    void shouldDeserialiseNestedHllpWithSAndSpValues() throws IOException {
        // Given
        final String json = "{\"hyperLogLogPlus\": {\"p\": 5, \"sp\": 10}}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then
        assertThat(hllp.getBytes()).isEqualTo(new HyperLogLogPlus(5, 10).getBytes());
    }

    @Test
    void shouldDeserialiseNestedHllpWithSAndSpValuesOffersEmpty() throws IOException {
        // Given
        final String json = "{\"hyperLogLogPlus\": {\"p\": 5, \"sp\": 10, \"offers\": []}}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then
        assertThat(hllp.getBytes()).isEqualTo(new HyperLogLogPlus(5, 10).getBytes());
    }

    @Test
    void shouldFailDeserialisingHllpWithOffersInvalidObject() throws IOException {
        // Given
        final String json = "{\"p\": 5, \"sp\": 5, " +
                "\"offers\": [" +
                "[{\"class\": \"fail\", \"fail\": \"fail\"}]" +
                "]}";

        // When
        // Then
        assertThatExceptionOfType(SerialisationException.class)
                .isThrownBy(() -> JSONSerialiser.deserialise(json, HyperLogLogPlus.class))
                .withMessageContaining("Error deserialising JSON object: Unexpected token");
    }

    @Test
    void shouldDeserialiseHllpWithOffers() throws IOException {
        // Given
        final String json = "{\"p\": 5, \"sp\": 5, \"offers\": [\"value1\", \"value2\", \"value2\", \"value2\", \"value3\"]}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then
        final HyperLogLogPlus expected = new HyperLogLogPlus(5, 5);
        expected.offer("value1");
        expected.offer("value2");
        expected.offer("value2");
        expected.offer("value2");
        expected.offer("value3");
        assertArrayEquals(expected.getBytes(), hllp.getBytes());
    }

    @Test
    void shouldDeserialiseHllpWithOffersTypeSubTypeValue() throws IOException {
        // Given
        final String json = "{\"p\": 5, \"sp\": 5, " +
                "\"offers\": [" +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeSubTypeValue\", \"type\": \"type1\", \"subType\": \"subType1\", \"value\": \"value1\"}, " +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeSubTypeValue\", \"type\": \"type2\", \"subType\": \"subType2\", \"value\": \"value2\"}, " +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeSubTypeValue\", \"type\": \"type3\", \"subType\": \"subType3\", \"value\": \"value3\"}" +
                "]}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then
        final HyperLogLogPlus expected = new HyperLogLogPlus(5, 5);
        expected.offer(new TypeSubTypeValue("type1", "subType1", "value1"));
        expected.offer(new TypeSubTypeValue("type2", "subType2", "value2"));
        expected.offer(new TypeSubTypeValue("type3", "subType3", "value3"));
        assertThat(hllp.getBytes()).isEqualTo(expected.getBytes());
    }

    @Test
    void shouldDeserialiseHllpWithOffersTypeValue() throws IOException {
        // Given
        final String json = "{\"p\": 5, \"sp\": 5, " +
                "\"offers\": [" +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeValue\", \"type\": \"type1\", \"value\": \"value1\"}, " +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeValue\", \"type\": \"type2\", \"value\": \"value2\"}, " +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeValue\", \"type\": \"type3\", \"value\": \"value3\"}" +
                "]}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then
        final HyperLogLogPlus expected = new HyperLogLogPlus(5, 5);
        expected.offer(new TypeValue("type1", "value1"));
        expected.offer(new TypeValue("type2", "value2"));
        expected.offer(new TypeValue("type3", "value3"));
        assertThat(hllp.getBytes()).isEqualTo(expected.getBytes());
    }

    @Test
    void shouldDeserialiseHllpWithOffersTypeFreqMap() throws IOException {
        // Given
        final String json = "{\"p\": 5, \"sp\": 5, " +
                "\"offers\": [" +
                "{\"class\": \"" + FreqMap.class.getName() + "\", \"a\": 1, \"b\": 1234567891011121314}" +
                "]}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then
        final FreqMap freqMap = new FreqMap();
        freqMap.put("a", 1L);
        freqMap.put("b", 1234567891011121314L);

        final HyperLogLogPlus expected = new HyperLogLogPlus(5, 5);
        expected.offer(freqMap);
        assertThat(hllp.getBytes()).isEqualTo(expected.getBytes());
    }

    @Test
    void shouldDeserialiseHllpWithOffersInteger() throws IOException {
        // Given
        final String json = "{\"p\": 5, \"sp\": 5, " +
                "\"offers\": [1, 2, 3]}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then
        final HyperLogLogPlus expected = new HyperLogLogPlus(5, 5);
        expected.offer(1);
        expected.offer(2);
        expected.offer(3);
        assertThat(hllp.getBytes()).isEqualTo(expected.getBytes());
    }

    @Test
    void shouldDeserialiseHllpWithOffersString() throws IOException {
        // Given
        final String json = "{\"p\": 5, \"sp\": 5, " +
                "\"offers\": [\"one\", \"two\", \"three\"]}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then
        final HyperLogLogPlus expected = new HyperLogLogPlus(5, 5);
        expected.offer("one");
        expected.offer("two");
        expected.offer("three");
        assertThat(hllp.getBytes()).isEqualTo(expected.getBytes());
    }

    @Test
    void shouldDeserialiseHllpWithOffersFloat() throws IOException {
        // Given
        final String json = "{\"p\": 5, \"sp\": 5, " +
                "\"offers\": [1.2, 2.12345, 3.123456789]}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then
        final HyperLogLogPlus expected = new HyperLogLogPlus(5, 5);
        expected.offer(1.2F);
        expected.offer(2.12345F);
        expected.offer(3.123456789);
        assertThat(hllp.getBytes()).isEqualTo(expected.getBytes());
    }

    @Test
    void shouldDeserialiseHllpWithOffersLong() throws IOException {
        // Given
        final String json = "{\"p\": 5, \"sp\": 5, " +
                "\"offers\": [1234567891011121314]}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then
        final HyperLogLogPlus expected = new HyperLogLogPlus(5, 5);
        expected.offer(1234567891011121314L);
        assertThat(hllp.getBytes()).isEqualTo(expected.getBytes());
    }

    @Test
    void shouldDeserialiseHllpWithOffersDouble() throws IOException {
        // Given
        final String json = "{\"p\": 5, \"sp\": 5, " +
                "\"offers\": [1.2, 2.12345, 3.123456789]}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then
        final HyperLogLogPlus expected = new HyperLogLogPlus(5, 5);
        expected.offer(1.2D);
        expected.offer(2.12345D);
        expected.offer(3.123456789D);
        assertThat(hllp.getBytes()).isEqualTo(expected.getBytes());
    }

    @Test
    void shouldDeserialiseHllpWithOffersBoolean() throws IOException {
        // Given
        final String json = "{\"p\": 5, \"sp\": 5, " +
                "\"offers\": [true, false]}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then
        final HyperLogLogPlus expected = new HyperLogLogPlus(5, 5);
        expected.offer(true);
        expected.offer(false);
        assertThat(hllp.getBytes()).isEqualTo(expected.getBytes());
    }

    @Test
    void shouldDeserialiseHllpWithOffersTestObjectNoAnnotations() throws IOException {
        // Given
        final String json = "{\"p\": 5, \"sp\": 5, " +
                "\"offers\": [{\"class\": \"" + TestObject.class.getName() + "\", \"testString\": \"test\"}]}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then

        final HyperLogLogPlus expected = new HyperLogLogPlus(5, 5);
        expected.offer(new TestObject("test"));
        assertThat(hllp.getBytes()).isEqualTo(expected.getBytes());
    }

    @Test
    void shouldDeserialiseHllpWithOffersTestJavaToJson() throws IOException {
        // Given
        final HyperLogLogPlusWithOffers hyperLogLogPlusWithOffers = new HyperLogLogPlusWithOffers();
        hyperLogLogPlusWithOffers.setP(5);
        hyperLogLogPlusWithOffers.setSp(5);
        final ArrayList<Object> offers = new ArrayList<>();
        offers.add(new TypeSubTypeValue("type", "subType", "value"));
        offers.add(new TypeValue("type", "value"));
        offers.add(new CustomMap<>(new BooleanSerialiser(), new BooleanSerialiser()));
        offers.add(new FreqMap("test"));
        hyperLogLogPlusWithOffers.setOffers(offers);
        final byte[] serialised = JSONSerialiser.serialise(hyperLogLogPlusWithOffers, true);

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(serialised, HyperLogLogPlus.class);

        // Then
        final HyperLogLogPlus expected = new HyperLogLogPlus(5, 5);
        expected.offer(new TypeSubTypeValue("type", "subType", "value"));
        expected.offer(new TypeValue("type", "value"));
        expected.offer(new CustomMap<>(new BooleanSerialiser(), new BooleanSerialiser()));
        expected.offer(new FreqMap("test"));
        assertThat(hllp.getBytes()).isEqualTo(expected.getBytes());
    }

    @Test
    void shouldDeserialiseHllpWithOffersAll() throws IOException {
        // Given
        final String json = "{\"p\": 5, \"sp\": 5, " +
                "\"offers\": [" +
                "1, " +
                "\"string\",  " +
                "true, " +
                "1.12345, " +
                "1.123456789, " +
                "1234567891011121314, " +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeSubTypeValue\", \"type\": \"type\", \"subType\": \"subType\", \"value\": \"value\"}, " +
                "{\"class\": \"uk.gov.gchq.gaffer.types.TypeValue\", \"type\": \"type\", \"value\": \"value\"}" +
                "]}";

        // When
        final HyperLogLogPlus hllp = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then
        final HyperLogLogPlus expected = new HyperLogLogPlus(5, 5);
        expected.offer(1);
        expected.offer("string");
        expected.offer(true);
        expected.offer(1.12345F);
        expected.offer(1.123456789D);
        expected.offer(1234567891011121314L);
        expected.offer(new TypeValue("type", "value"));
        expected.offer(new TypeSubTypeValue("type", "subType", "value"));
        assertThat(hllp.getBytes()).isEqualTo(expected.getBytes());
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
