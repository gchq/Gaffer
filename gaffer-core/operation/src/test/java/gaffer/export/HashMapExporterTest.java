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

package gaffer.export;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import gaffer.commonutil.iterable.ChainedIterable;
import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.user.User;
import org.junit.Test;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class HashMapExporterTest {

    @Test
    public void shouldSetAndGetExportMap() {
        // Given
        final HashMapExporter exporter = new HashMapExporter();
        final Map<String, Set<Object>> exportMap = new HashMap<>();

        // When
        exporter.setExportMap(exportMap);

        // Then
        assertSame(exportMap, exporter.getExportMap());
    }

    @Test
    public void shouldNotBeAbleToSetANullExportMap() {
        // Given
        final HashMapExporter exporter = new HashMapExporter();
        final Map<String, Set<Object>> exportMap = exporter.getExportMap();

        // When
        exporter.setExportMap(null);

        // Then
        assertNotNull(exporter.getExportMap());
        assertNotSame(exportMap, exporter.getExportMap());
    }

    @Test
    public void shouldCreateNewHashMapWhenInitialising() {
        // Given
        final HashMapExporter exporter = new HashMapExporter();
        final Map<String, Set<Object>> exportMap = exporter.getExportMap();

        // When
        exporter.initialise(null, new User());

        // Then
        assertNotSame(exportMap, exporter.getExportMap());
    }

    @Test
    public void shouldAddIterablesToMap() {
        // Given
        final String key1 = "key1";
        final String key2 = "key2";
        final List<Integer> values1 = Arrays.asList(1, 2, 3);
        final List<String> values2a = Arrays.asList("1", "2", "3");
        final List<String> values2b = Arrays.asList("4", "5", "6");
        final List<String> values2Combined = Lists.newArrayList(new ChainedIterable<String>(values2a, values2b));
        final HashMapExporter exporter = new HashMapExporter();

        // When
        exporter._add(key1, values1, new User());
        exporter._add(key2, values2a, new User());
        exporter._add(key2, values2b, new User());

        // Then
        final Map<String, Set<Object>> exportMap = exporter.getExportMap();
        assertNotSame(values1, exportMap.get(key1));
        assertEquals(Sets.newHashSet(values1), exportMap.get(key1));
        assertEquals(Sets.newHashSet(values2Combined), exportMap.get(key2));
    }

    @Test
    public void shouldGetSubsetOfValuesFromMap() {
        // Given
        final String key1 = "key1";
        final List<Integer> values1 = Arrays.asList(1, 2, 3, 4, 5);
        final HashMapExporter exporter = new HashMapExporter();
        final int start = 2;
        final int end = 3;
        exporter._add(key1, values1, new User());

        // When
        try (CloseableIterable<?> results = exporter._get(key1, new User(), start, end)) {

            // Then
            assertEquals(values1.subList(start, end), Lists.newArrayList(results));
        }
    }
}
