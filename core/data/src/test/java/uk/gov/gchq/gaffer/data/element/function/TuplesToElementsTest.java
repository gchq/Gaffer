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
import uk.gov.gchq.koryphe.tuple.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TuplesToElementsTest extends FunctionTest<TuplesToElements> {

    @Test
    public void shouldConvertBasicEntity() {
        // Given
        ElementTupleDefinition elementTupleDefinition = new ElementTupleDefinition(TestGroups.ENTITY);
        elementTupleDefinition.vertex("vertex");
        final TuplesToElements tuplesToElements = new TuplesToElements();
        List<ElementTupleDefinition> elements = Stream.of(elementTupleDefinition).collect(Collectors.toList());
        tuplesToElements.elements(elements);
        MapTuple<String> tuple = new MapTuple<>();
        tuple.put("vertex", "a");
        Iterable<Tuple<String>> tuples = Stream.of(tuple).collect(Collectors.toList());

        // When
        Iterable<Element> results = tuplesToElements.apply(tuples);

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
        return new Class[]{Iterable.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{Iterable.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final TuplesToElements tuplesToElements = new TuplesToElements();
        // When
        final String json = new String(JSONSerialiser.serialise(tuplesToElements));
        TuplesToElements deserialisedTuplesToElements = JSONSerialiser.deserialise(json, TuplesToElements.class);
        // Then
        assertEquals(tuplesToElements, deserialisedTuplesToElements);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.data.element.function.TuplesToElements\"}", json);

    }

    @Override
    protected TuplesToElements getInstance() {
        return new TuplesToElements();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}
