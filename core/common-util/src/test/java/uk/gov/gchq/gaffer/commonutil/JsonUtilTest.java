/*
 * Copyright 2017-2020 Crown Copyright
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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonUtilTest {

    @Test
    public void shouldReturnTrueWhenJsonObjectsAreEqualButInADifferentOrder() {
        final String json1 = "{\"a\": 1, \"b\": 2}";
        final String json2 = "{\"b\": 2, \"a\": 1}";

        assertTrue(JsonUtil.equals(json1, json2));
        assertTrue(JsonUtil.equals(json1.getBytes(), json2.getBytes()));

        JsonAssert.assertEquals(json1, json2);
        JsonAssert.assertEquals(json1.getBytes(), json2.getBytes());
    }

    @Test
    public void shouldReturnFalseWhenJsonObjectsAreDifferentSizes() {
        final String json1 = "{\"a\": 1, \"b\": 2}";
        final String json2 = "{\"a\": 1, \"b\": 2, \"c\": 3}";

        assertFalse(JsonUtil.equals(json1, json2));
        assertFalse(JsonUtil.equals(json1.getBytes(), json2.getBytes()));

        JsonAssert.assertNotEqual(json1, json2);
        JsonAssert.assertNotEqual(json1.getBytes(), json2.getBytes());
    }

    @Test
    public void shouldReturnFalseWhenJsonObjectsAreNotEqual() {
        final String json1 = "{\"a\": 1, \"b\": 2}";
        final String json2 = "{\"a\": 1, \"b\": 3}";

        assertFalse(JsonUtil.equals(json1, json2));
        assertFalse(JsonUtil.equals(json1.getBytes(), json2.getBytes()));

        JsonAssert.assertNotEqual(json1, json2);
        JsonAssert.assertNotEqual(json1.getBytes(), json2.getBytes());
    }

    @Test
    public void shouldReturnTrueWhenJsonArraysAreEqual() {
        final String json1 = "[1,2,3]";
        final String json2 = "[1,2,3]";

        assertTrue(JsonUtil.equals(json1, json2));
        assertTrue(JsonUtil.equals(json1.getBytes(), json2.getBytes()));

        JsonAssert.assertEquals(json1, json2);
        JsonAssert.assertEquals(json1.getBytes(), json2.getBytes());
    }

    @Test
    public void shouldReturnFalseWhenJsonArraysAreNotEqual() {
        // Given
        final String json1 = "[1,2,3]";
        final String json2 = "[1,2,4]";

        assertFalse(JsonUtil.equals(json1, json2));
        assertFalse(JsonUtil.equals(json1.getBytes(), json2.getBytes()));

        JsonAssert.assertNotEqual(json1, json2);
        JsonAssert.assertNotEqual(json1.getBytes(), json2.getBytes());
    }

    @Test
    public void shouldReturnFalseWhenJsonArraysAreDifferentSizes() {
        final String json1 = "[1,2,3]";
        final String json2 = "[1,2,3,4]";

        assertFalse(JsonUtil.equals(json1, json2));
        assertFalse(JsonUtil.equals(json1.getBytes(), json2.getBytes()));

        JsonAssert.assertNotEqual(json1, json2);
        JsonAssert.assertNotEqual(json1.getBytes(), json2.getBytes());
    }

    @Test
    public void shouldReturnFalseWhenActualObjectIsNull() {
        final String json1 = "{\"a\": 1, \"b\": 2}";

        assertFalse(JsonUtil.equals(json1, null));
        assertFalse(JsonUtil.equals(json1.getBytes(), null));

        JsonAssert.assertNotEqual(json1, null);
        JsonAssert.assertNotEqual(json1.getBytes(), null);
    }
}
