/*
 * Copyright 2016 Crown Copyright
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
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class JsonUtil {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static boolean equals(final String expectedJson, final String actualJson) {
        Map expectedSchemaMap = null;
        Map actualSchemaMap = null;
        try {
            if (null != expectedJson) {
                expectedSchemaMap = OBJECT_MAPPER.readValue(expectedJson, Map.class);
            }
            if (null != actualJson) {
                actualSchemaMap = OBJECT_MAPPER.readValue(actualJson, Map.class);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid json", e);
        }
        return Objects.equals(expectedSchemaMap, actualSchemaMap);

    }

    public static boolean equals(final byte[] expectedJson, final byte[] actualJson) {
        return equals(new String(expectedJson), new String(actualJson));
    }
}
