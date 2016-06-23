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
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import gaffer.commonutil.TestPropertyNames;
import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.user.User;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class ElementJsonFileExporterTest {
    private static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(ElementJsonFileExporter.PARENT_DIRECTORY));
    }

    @Test
    public void shouldCreateFileNameBasedOnUserAndTimestampAndKey() {
        // Given
        final ElementJsonFileExporter exporter = new ElementJsonFileExporter();
        final String key = "key1";
        final long timestamp = 1000;
        exporter.setTimestamp(timestamp);
        final String userId = "user01";
        exporter.initialise(key, null, new User(userId));

        // When
        final String fileName = exporter.getFileName();

        // Then
        assertEquals("json-exports/" + userId + "_" + timestamp + "_" + key + ".txt", fileName);
    }

    @Test
    public void shouldCreateDirectoryWhenInitialising() throws IOException {
        // Given
        final ElementJsonFileExporter exporter = new ElementJsonFileExporter();

        // When
        exporter.initialise("key", null, new User());

        // Then
        assertTrue(new File(ElementJsonFileExporter.PARENT_DIRECTORY).exists());
    }

    @Test
    public void shouldWriteToFile() throws IOException {
        // Given
        final User user = new User();
        final long timestamp = 1000;
        final String key = "key";
        final List<Element> elements = createElements();

        final ElementJsonFileExporter exporter = new ElementJsonFileExporter();
        exporter.setTimestamp(timestamp);
        exporter.initialise(key, null, user);

        // When
        exporter.addElements(elements, user);

        // Then
        final List<String> lines = FileUtils.readLines(new File(exporter.getFileName()));
        assertEquals(elements.size(), lines.size());
        for (int i = 0; i < elements.size(); i++) {
            assertEquals(new String(JSON_SERIALISER.serialise(elements.get(i))), lines.get(i));
        }
    }

    @Test
    public void shouldWriteAndReadFromFile() throws IOException {
        // Given
        final User user = new User();
        final long timestamp = 1000;
        final String key = "key";
        final List<Element> elements = createElements();
        final int start = 0;
        final int end = 2;

        final ElementJsonFileExporter exporter = new ElementJsonFileExporter();
        exporter.setTimestamp(timestamp);
        exporter.initialise(key, null, user);
        exporter.addElements(elements, user);

        // When
        try (final CloseableIterable<Element> exportedElements = exporter.getElements(user, start, end)) {
            // Then
            assertEquals(elements, Lists.newArrayList(exportedElements));
        }
    }

    @Test
    public void shouldWriteAndReadFromFileWithPagination() throws IOException {
        // Given
        final User user = new User();
        final long timestamp = 1000;
        final String key = "key";
        final List<Element> elements = createElements();
        final int start = 0;
        final int end = 1;

        final ElementJsonFileExporter exporter = new ElementJsonFileExporter();
        exporter.setTimestamp(timestamp);
        exporter.initialise(key, null, user);
        exporter.addElements(elements, user);

        // When
        try (final CloseableIterable<Element> exportedElements = exporter.getElements(user, start, end)) {
            // Then
            assertEquals(elements.subList(start, end), Lists.newArrayList(exportedElements));
        }
    }

    private List<Element> createElements() {
        return Arrays.asList(
                new Edge.Builder()
                        .source("source1").dest("dest1").directed(true)
                        .property(TestPropertyNames.COUNT, 1)
                        .build(),
                new Entity.Builder()
                        .vertex("vertex1")
                        .property(TestPropertyNames.STRING, "propValue")
                        .build());
    }
}
