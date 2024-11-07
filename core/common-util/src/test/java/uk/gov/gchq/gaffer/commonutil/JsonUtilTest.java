/*
 * Copyright 2017-2024 Crown Copyright
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;

class JsonUtilTest {

    @Test
    void shouldReturnTrueWhenJsonObjectsAreEqualButInADifferentOrder() {
        final String json1 = "{\"a\": 1, \"b\": 2}";
        final String json2 = "{\"b\": 2, \"a\": 1}";

        assertThat(JsonUtil.equals(json1, json2)).isTrue();
        assertThat(JsonUtil.equals(json1.getBytes(), json2.getBytes())).isTrue();

        JsonAssert.assertEquals(json1, json2);
        JsonAssert.assertEquals(json1.getBytes(), json2.getBytes());
    }

    @Test
    void shouldReturnTrueWhenJsonArraysAreEqual() {
        final String json1 = "[1,2,3]";
        final String json2 = "[1,2,3]";

        assertThat(JsonUtil.equals(json1, json2)).isTrue();
        assertThat(JsonUtil.equals(json1.getBytes(), json2.getBytes())).isTrue();

        JsonAssert.assertEquals(json1, json2);
        JsonAssert.assertEquals(json1.getBytes(), json2.getBytes());
    }

    @ParameterizedTest
    @MethodSource("provideJSONStrings")
    void shouldReturnFalse(String json1, String json2) {
        assertThat(JsonUtil.equals(json1, json2)).isFalse();
        assertThat(JsonUtil.equals(json1.getBytes(), json2.getBytes())).isFalse();

        JsonAssert.assertNotEqual(json1, json2);
        JsonAssert.assertNotEqual(json1.getBytes(), json2.getBytes());
    }

    private static Stream<Arguments> provideJSONStrings() {
        return Stream.of(
            Arguments.of("{\"a\": 1, \"b\": 2}", "{\"a\": 1, \"b\": 2, \"c\": 3}"),
            Arguments.of("{\"a\": 1, \"b\": 2}", "{\"a\": 1, \"b\": 3}"),
            Arguments.of("[1,2,3]", "[1,2,4]"),
            Arguments.of("[1,2,3]", "[1,2,3,4]")
        );
    }

    @Test
    void shouldReturnFalseWhenActualObjectIsNull() {
        final String json1 = "{\"a\": 1, \"b\": 2}";

        assertThat(JsonUtil.equals(json1, null)).isFalse();
        assertThat(JsonUtil.equals(json1.getBytes(), null)).isFalse();

        JsonAssert.assertNotEqual(json1, null);
        JsonAssert.assertNotEqual(json1.getBytes(), null);
    }
}
