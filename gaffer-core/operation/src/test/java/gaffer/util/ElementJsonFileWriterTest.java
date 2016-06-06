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

import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.jsonserialisation.JSONSerialiser;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class ElementJsonFileWriterTest {
    private static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();

    @Test
    public void shouldWriteToFile() throws IOException {
        // Given
        final String filename = "filename.txt";
        final List<Element> elements = createElements();

        final ElementJsonFileWriter writer = new ElementJsonFileWriter();

        // When
        try {
            writer.write(elements, filename);

            // Then
            final List<String> lines = FileUtils.readLines(new File(filename));
            assertEquals(elements.size(), lines.size());
            for (int i = 0; i < elements.size(); i++) {
                assertEquals(new String(JSON_SERIALISER.serialise(elements.get(i))), lines.get(i));
            }
        } finally {
            FileUtils.deleteQuietly(new File(filename));
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
