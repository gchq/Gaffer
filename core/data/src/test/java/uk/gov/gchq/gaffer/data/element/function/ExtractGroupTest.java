/*
 * Copyright 2017-2018 Crown Copyright
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

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ExtractGroupTest extends FunctionTest {

    @Test
    public void shouldReturnGroupFromEdge() {
        // Given
        final ExtractGroup function = new ExtractGroup();
        final String group = "testGroup";

        final Edge edge = new Edge.Builder()
                .source("src")
                .dest("dest")
                .directed(true)
                .group(group)
                .build();

        // When
        final String result = function.apply(edge);

        // Then
        assertEquals(group, result);
    }

    @Test
    public void shouldReturnGroupFromEntity() {
        // Given
        final ExtractGroup function = new ExtractGroup();

        final String group = "testGroup_2";

        final Entity entity = new Entity.Builder()
                .vertex("1")
                .group(group)
                .build();

        // When
        final String result = function.apply(entity);

        // Then
        assertEquals(group, result);
    }

    @Test
    public void shouldReturnNullForNullElement() {
        // Given
        final ExtractGroup function = new ExtractGroup();

        // When
        final String result = function.apply(null);

        // Then
        assertNull(result);
    }

    @Override
    protected ExtractGroup getInstance() {
        return new ExtractGroup();
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return ExtractGroup.class;
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final ExtractGroup function = getInstance();

        // When
        final byte[] json = JSONSerialiser.serialise(function);
        final ExtractGroup deserialised = JSONSerialiser.deserialise(json, ExtractGroup.class);

        // Then
        JsonAssert.assertEquals("{\"class\" : \"uk.gov.gchq.gaffer.data.element.function.ExtractGroup\"}",
                new String(json));
    }
}
