/*
 * Copyright 2016-2024 Crown Copyright
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

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class JsonAssert {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsonAssert() {
        //Private to prevent instantiation
    }

    public static void assertEquals(final String expectedJson, final String actualJson) {
        try {
            final Map expectedSchemaMap = expectedJson != null ? OBJECT_MAPPER.readValue(expectedJson, Map.class) : Collections.emptyMap();
            final Map actualSchemaMap = actualJson != null ? OBJECT_MAPPER.readValue(actualJson, Map.class) : Collections.emptyMap();
            assertThat(actualSchemaMap).isEqualTo(expectedSchemaMap);
            return;
        } catch (final IOException e) {
            // ignore the error and try using lists instead
        }

        try {
            final List expectedSchemaMap = expectedJson != null ? OBJECT_MAPPER.readValue(expectedJson, List.class) : Collections.emptyList();
            final List actualSchemaMap = actualJson != null ? OBJECT_MAPPER.readValue(actualJson, List.class) : Collections.emptyList();
            assertThat(actualSchemaMap).isEqualTo(expectedSchemaMap);
        } catch (final IOException e) {
            throw new AssertionError(expectedJson + " is not equal to " + actualJson, e);
        }
    }

    public static void assertEquals(final byte[] expectedJson, final byte[] actualJson) {
        assertEquals(null != expectedJson ? new String(expectedJson) : null, null != actualJson ? new String(actualJson) : null);
    }

    public static void assertNotEqual(final String firstJson, final String secondJson) {
        try {
            final Map firstSchemaMap = firstJson != null ? OBJECT_MAPPER.readValue(firstJson, Map.class) : Collections.emptyMap();
            final Map secondSchemaMap = secondJson != null ? OBJECT_MAPPER.readValue(secondJson, Map.class) : Collections.emptyMap();
            assertThat(firstSchemaMap).isNotEqualTo(secondSchemaMap);
        } catch (final IOException e) {
            // ignore
        }
    }

    public static void assertNotEqual(final byte[] firstJson, final byte[] secondJson) {
        assertNotEqual(null != firstJson ? new String(firstJson) : null, null != secondJson ? new String(secondJson) : null);
    }
}
