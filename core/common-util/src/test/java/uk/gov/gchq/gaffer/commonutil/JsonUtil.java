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
import org.junit.Assert;
import java.io.IOException;
import java.util.Map;

public class JsonUtil {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void assertEquals(final String expectedJson, final String actualJson) {
        try {
            final Map expectedSchemaMap = OBJECT_MAPPER.readValue(expectedJson, Map.class);
            final Map actualSchemaMap = OBJECT_MAPPER.readValue(actualJson, Map.class);
            Assert.assertEquals(expectedSchemaMap, actualSchemaMap);
        } catch (final IOException e) {
            throw new AssertionError(expectedJson + "is not equal to " + actualJson, e);
        }
    }

    public static void assertEquals(final byte[] expectedJson, final byte[] actualJson) {
        assertEquals(new String(expectedJson), new String(actualJson));
    }
}
