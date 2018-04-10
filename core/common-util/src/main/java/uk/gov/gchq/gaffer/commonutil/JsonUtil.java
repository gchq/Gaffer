/*
 * Copyright 2017-2018 Crown Copyright
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Utility methods for various JSON representations in Gaffer.
 */
public final class JsonUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonUtil.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsonUtil() {
        // Private constructor to prevent instantiation.
    }

    /**
     * Test to see whether two JSON {@link String} representations are equal.
     *
     * @param expectedJson the expected JSON string
     * @param actualJson the actual JSON string
     * @return true if both strings are equal, otherwise false
     */
    public static boolean equals(final String expectedJson, final String actualJson) {
        try {
            final Map expectedSchemaMap = null != expectedJson ? OBJECT_MAPPER.readValue(expectedJson, Map.class) : Collections.emptyMap();
            final Map actualSchemaMap = null != actualJson ? OBJECT_MAPPER.readValue(actualJson, Map.class) : Collections.emptyMap();
            return Objects.equals(expectedSchemaMap, actualSchemaMap);
        } catch (final IOException e) {
            // ignore the error and try using lists instead
        }

        try {
            final List expectedSchemaMap = null != expectedJson ? OBJECT_MAPPER.readValue(expectedJson, List.class) : Collections.emptyList();
            final List actualSchemaMap = null != actualJson ? OBJECT_MAPPER.readValue(actualJson, List.class) : Collections.emptyList();
            return Objects.equals(expectedSchemaMap, actualSchemaMap);
        } catch (final IOException e) {
            logError(expectedJson, actualJson, e);
            return false;
        }
    }

    /**
     * Test to see whether two JSON byte array representations are equal.
     *
     * @param expectedJson the expected JSON byte array
     * @param actualJson the actual JSON byte array
     * @return true if both strings are equal, otherwise false
     */
    public static boolean equals(final byte[] expectedJson, final byte[] actualJson) {
        try {
            final Map expectedSchemaMap = null != expectedJson ? OBJECT_MAPPER.readValue(expectedJson, Map.class) : Collections.emptyMap();
            final Map actualSchemaMap = null != actualJson ? OBJECT_MAPPER.readValue(actualJson, Map.class) : Collections.emptyMap();
            return Objects.equals(expectedSchemaMap, actualSchemaMap);
        } catch (final IOException e) {
            logError(expectedJson, actualJson, e);
            // ignore the error and try using arrays instead
        }

        try {
            final List expectedSchemaMap = null != expectedJson ? OBJECT_MAPPER.readValue(expectedJson, List.class) : Collections.emptyList();
            final List actualSchemaMap = null != actualJson ? OBJECT_MAPPER.readValue(actualJson, List.class) : Collections.emptyList();
            return Objects.equals(expectedSchemaMap, actualSchemaMap);
        } catch (final IOException e) {
            logError(expectedJson, actualJson, e);
            return false;
        }
    }

    private static void logError(final Object expectedJson, final Object actualJson, final IOException e) {
        LOGGER.debug("Error comparing json.\nexpectedJson:\n {}\n actual json:\n", expectedJson, actualJson, e);
    }
}
