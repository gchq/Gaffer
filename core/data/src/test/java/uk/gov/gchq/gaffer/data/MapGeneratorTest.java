/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.data;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.MapGenerator;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MapGeneratorTest {
    @Test
    public void shouldGenerateMapFromElementFieldsAndConstants() {
        // Given
        final Object vertex = "source vertex";
        final String prop1 = "property 1";
        final int count = 10;
        final Element element = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex(vertex)
                .property(TestPropertyNames.PROP_1, prop1)
                .property(TestPropertyNames.COUNT, count)
                .build();

        final MapGenerator generator = new MapGenerator.Builder()
                .vertex("field1")
                .group("field2")
                .property(TestPropertyNames.COUNT, "field3")
                .property(TestPropertyNames.PROP_1, "field4")
                .source("field5") // the entity does not have a source so this should be skipped
                .property("unknown property", "field6")
                .constant("constant1", "constantValue1")
                .build();

        // When
        final Map<String, Object> map = generator.getObject(element);

        // Then

        final Map<String, Object> expectedMap = new LinkedHashMap<>();
        expectedMap.put("field1", vertex);
        expectedMap.put("field2", TestGroups.ENTITY);
        expectedMap.put("field3", count);
        expectedMap.put("field4", prop1);
        expectedMap.put("constant1", "constantValue1");
        assertEquals(expectedMap, map);
    }

    @Test
    public void shouldGenerateMapFromASingleProperty() {
        // Given
        final Object vertex = "source vertex";
        final String prop1 = "property 1";
        final int count = 10;
        final Element element = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex(vertex)
                .property(TestPropertyNames.PROP_1, prop1)
                .property(TestPropertyNames.COUNT, count)
                .build();

        final MapGenerator generator = new MapGenerator.Builder()
                .property(TestPropertyNames.PROP_1, "field1")
                .build();

        // When
        final Map<String, Object> map = generator.getObject(element);

        // Then

        final Map<String, Object> expectedMap = new LinkedHashMap<>();
        expectedMap.put("field1", prop1);
        assertEquals(expectedMap, map);
    }

    @Test
    public void shouldGenerateAnEmptyMap() {
        // Given
        final Object vertex = "source vertex";
        final String prop1 = "property 1";
        final int count = 10;
        final Element element = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex(vertex)
                .property(TestPropertyNames.PROP_1, prop1)
                .property(TestPropertyNames.COUNT, count)
                .build();

        final MapGenerator generator = new MapGenerator();

        // When
        final Map<String, Object> map = generator.getObject(element);

        // Then

        final Map<String, Object> expectedMap = new LinkedHashMap<>();
        assertEquals(expectedMap, map);
    }
}
