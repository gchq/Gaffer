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
package gaffer.function.simple.transform;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import gaffer.function.Function;
import gaffer.function.TransformFunctionTest;
import gaffer.jsonserialisation.JSONSerialiser;
import org.junit.Test;
import java.io.IOException;

public class RemoveTest extends TransformFunctionTest {
    @Test
    public void shouldReturnNullValueWhenExecutedWithMultipleValues() {
        // Given
        final Remove remove = new Remove();

        // When
        final Object[] output = remove.transform(new String[]{"1", "2", "3"});

        // Then
        assertArrayEquals(new Object[]{null}, output);
    }

    @Test
    public void shouldReturnNullValueWhenExecutedWithOneValue() {
        // Given
        final Remove remove = new Remove();

        // When
        final Object[] output = remove.transform(new Integer[]{1});

        // Then
        assertArrayEquals(new Object[]{null}, output);
    }

    @Test
    public void shouldReturnNullValueWhenExecutedWithNull() {
        // Given
        final Remove remove = new Remove();

        // When
        final Object[] output = remove.transform(null);

        // Then
        assertArrayEquals(new Object[]{null}, output);
    }

    @Override
    protected Remove getInstance() {
        return new Remove();
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final Remove remove = new Remove();

        // When
        final String json = new String(new JSONSerialiser().serialise(remove, true));

        // Then
        assertEquals("{\n" +
                "  \"class\" : \"gaffer.function.simple.transform.Remove\"\n" +
                "}", json);

        // When 2
        final Remove deserialisedRemove = new JSONSerialiser().deserialise(json.getBytes(), Remove.class);

        // Then 2
        assertNotNull(deserialisedRemove);
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return Remove.class;
    }
}