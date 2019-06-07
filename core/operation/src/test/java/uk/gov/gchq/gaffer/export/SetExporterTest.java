/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.export;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.operation.impl.export.set.SetExporter;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class SetExporterTest {

    @Test
    public void shouldAddIterablesToSet() {
        // Given
        final List<String> valuesA = Arrays.asList("1", "2", "3");
        final List<String> valuesB = Arrays.asList("4", "5", "6");
        final List<String> valuesCombined = Lists.newArrayList(new ChainedIterable<String>(valuesA, valuesB));
        final SetExporter exporter = new SetExporter();

        // When
        exporter.add("key", valuesA);
        exporter.add("key", valuesB);

        // Then
        final CloseableIterable<?> export = exporter.get("key");
        assertEquals(Sets.newHashSet(valuesCombined), Sets.newHashSet(export));
    }

    @Test
    public void shouldAddIterablesToDifferentSets() {
        // Given
        final List<String> valuesA = Arrays.asList("1", "2", "3");
        final List<String> valuesB = Arrays.asList("4", "5", "6");
        final SetExporter exporter = new SetExporter();

        // When
        exporter.add("key1", valuesA);
        exporter.add("key2", valuesB);

        // Then
        final CloseableIterable<?> export1 = exporter.get("key1");
        assertEquals(valuesA, Lists.newArrayList(export1));

        final CloseableIterable<?> export2 = exporter.get("key2");
        assertEquals(valuesB, Lists.newArrayList(export2));
    }

    @Test
    public void shouldGetSubsetOfValuesFromMap() {
        // Given
        final List<Integer> values1 = Arrays.asList(1, 2, 3, 4, 5);
        final SetExporter exporter = new SetExporter();
        final int start = 2;
        final int end = 3;
        exporter.add("key", values1);

        // When
        try (CloseableIterable<?> results = exporter.get("key", start, end)) {

            // Then
            assertEquals(values1.subList(start, end), Lists.newArrayList(results));
        }
    }
}
