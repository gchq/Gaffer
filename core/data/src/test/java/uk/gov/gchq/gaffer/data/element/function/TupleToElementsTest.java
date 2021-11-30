/*
 * Copyright 2021 Crown Copyright
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

package uk.gov.gchq.gaffer.data.element.function;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;
import uk.gov.gchq.koryphe.tuple.MapTuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TupleToElementsTest extends FunctionTest<TupleToElements> {

    @Test
    void shouldConvertBasicEntity() {
        // Given
        ElementTupleDefinition elementTupleDefinition = new ElementTupleDefinition(TestGroups.ENTITY);
        elementTupleDefinition.vertex("vertex");
        final TupleToElements tupleToElements = new TupleToElements();
        List<ElementTupleDefinition> elements = Stream.of(elementTupleDefinition).collect(Collectors.toList());
        tupleToElements.elements(elements);
        MapTuple<String> tuple = new MapTuple<>();
        tuple.put("vertex", "a");

        // When
        Iterable<Element> results = tupleToElements.apply(tuple);

        // Then
        List<Element> expected = new ArrayList<>();
        expected.add(new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("a")
                .build());
        assertThat(results).containsExactlyElementsOf(expected);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{String.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{Iterable.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final TupleToElements tupleToElements = new TupleToElements();
        // When
        final String json = new String(JSONSerialiser.serialise(tupleToElements));
        TupleToElements deserialisedTupleToElements = JSONSerialiser.deserialise(json, TupleToElements.class);
        // Then
        assertEquals(tupleToElements, deserialisedTupleToElements);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.data.element.function.TupleToElements\"}", json);

    }

    @Override
    protected TupleToElements getInstance() {
        return new TupleToElements();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}
