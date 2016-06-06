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

package gaffer.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.jsonserialisation.JSONSerialiser;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class ElementJsonFileReaderIterableTest {
    private static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();
    public static final String FILENAME = "filename.txt";

    @After
    public void cleanUp() {
        FileUtils.deleteQuietly(new File(FILENAME));
    }

    @Test
    public void shouldReadAllElementsFromFile() throws IOException {
        // Given
        final List<Element> elements = createElements();

        final ElementJsonFileWriter writer = new ElementJsonFileWriter();
        writer.write(elements, FILENAME);

        // When
        final ElementJsonFileReaderIterable readerIterable = new ElementJsonFileReaderIterable(FILENAME);

        // Then
        final List<Element> readElements = Lists.newArrayList(readerIterable);
        assertEquals(elements, readElements);
    }

    @Test
    public void shouldReadAllElementsFromFileFromStartPosition() throws IOException {
        // Given
        final List<Element> elements = createElements();
        final int start = 1;

        final ElementJsonFileWriter writer = new ElementJsonFileWriter();
        writer.write(elements, FILENAME);

        // When
        final ElementJsonFileReaderIterable readerIterable = new ElementJsonFileReaderIterable(FILENAME, start);

        // Then
        final List<Element> readElements = Lists.newArrayList(readerIterable);
        assertEquals(elements.subList(start, elements.size()), readElements);
    }

    @Test
    public void shouldReadAllElementsFromFileFromStartToEndPositions() throws IOException {
        // Given
        final List<Element> elements = createElements();
        final int start = 1;
        final int end = 2;

        final ElementJsonFileWriter writer = new ElementJsonFileWriter();
        writer.write(elements, FILENAME);

        // When
        final ElementJsonFileReaderIterable readerIterable = new ElementJsonFileReaderIterable(FILENAME, start, end);

        // Then
        final List<Element> readElements = Lists.newArrayList(readerIterable);
        assertEquals(elements.subList(start, end), readElements);
    }

    @Test
    public void shouldReadNothingWhenStartIsGreaterThanLengthOfFile() throws IOException {
        // Given
        final List<Element> elements = createElements();
        final int start = 100;

        final ElementJsonFileWriter writer = new ElementJsonFileWriter();
        writer.write(elements, FILENAME);

        // When
        final ElementJsonFileReaderIterable readerIterable = new ElementJsonFileReaderIterable(FILENAME, start);

        // Then
        final List<Element> readElements = Lists.newArrayList(readerIterable);
        assertTrue(readElements.isEmpty());
    }

    private List<Element> createElements() {
        return Arrays.asList(
                new Edge.Builder()
                        .source("source1").dest("dest1").directed(true)
                        .property(TestPropertyNames.COUNT, 1)
                        .build(),
                new Entity.Builder()
                        .vertex("vertex1")
                        .property(TestPropertyNames.STRING, "propValue1")
                        .build(),
                new Entity.Builder()
                        .vertex("vertex2")
                        .property(TestPropertyNames.STRING, "propValue2")
                        .build());
    }
}
