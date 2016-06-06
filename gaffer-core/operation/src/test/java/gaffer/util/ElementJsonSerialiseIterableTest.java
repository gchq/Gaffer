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

import com.google.common.collect.Lists;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.jsonserialisation.JSONSerialiser;
import org.junit.Test;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class ElementJsonSerialiseIterableTest {
    private static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();

    @Test
    public void shouldSerialiseElementsToJson() throws IOException {
        // Given
        final List<Element> elements = createElements();

        // When
        final ElementJsonSerialiserIterable serialiserIterable = new ElementJsonSerialiserIterable(elements);

        // Then
        final List<String> serialisedElements = Lists.newArrayList(serialiserIterable);
        assertEquals(elements.size(), serialisedElements.size());
        for (int i = 0; i < elements.size(); i++) {
            assertEquals(new String(JSON_SERIALISER.serialise(elements.get(i))), serialisedElements.get(i));
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
