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

package uk.gov.gchq.gaffer.export;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.user.User;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;


public class SetExporterTest {

    @Test
    public void shouldSetAndGetExport() {
        // Given
        final SetExporter exporter = new SetExporter();
        final Set<Object> export = new LinkedHashSet<>();

        // When
        exporter.setExport(export);

        // Then
        assertSame(export, exporter.getExport());
    }

    @Test
    public void shouldNotBeAbleToSetANullExport() {
        // Given
        final SetExporter exporter = new SetExporter();
        final Set<Object> export = exporter.getExport();

        // When
        exporter.setExport(null);

        // Then
        assertNotNull(exporter.getExport());
        assertNotSame(export, exporter.getExport());
    }

    @Test
    public void shouldCreateNewHashMapWhenInitialising() {
        // Given
        final SetExporter exporter = new SetExporter();
        final Set<Object> export = exporter.getExport();

        // When
        exporter.initialise("key", null, new User());

        // Then
        assertNotSame(export, exporter.getExport());
    }

    @Test
    public void shouldAddIterablesToMap() {
        // Given
        final List<String> valuesA = Arrays.asList("1", "2", "3");
        final List<String> valuesB = Arrays.asList("4", "5", "6");
        final List<String> valuesCombined = Lists.newArrayList(new ChainedIterable<String>(valuesA, valuesB));
        final SetExporter exporter = new SetExporter();

        // When
        exporter._add(valuesA, new User());
        exporter._add(valuesB, new User());

        // Then
        final Set<Object> export = exporter.getExport();
        assertEquals(Sets.newHashSet(valuesCombined), export);
    }

    @Test
    public void shouldGetSubsetOfValuesFromMap() {
        // Given
        final List<Integer> values1 = Arrays.asList(1, 2, 3, 4, 5);
        final SetExporter exporter = new SetExporter();
        final int start = 2;
        final int end = 3;
        exporter._add(values1, new User());

        // When
        try (CloseableIterable<?> results = exporter._get(new User(), start, end)) {

            // Then
            assertEquals(values1.subList(start, end), Lists.newArrayList(results));
        }
    }
}
