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

package gaffer.tuple.function.context;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import gaffer.function2.mock.MockTransform;
import gaffer.tuple.MapTuple;
import gaffer.tuple.view.Reference;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotSame;

public class FunctionContextTest {
    private static final ObjectMapper MAPPER = createObjectMapper();

    private static ObjectMapper createObjectMapper() {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
        return mapper;
    }

    protected String serialise(Object object) throws IOException {
        return MAPPER.writeValueAsString(object);
    }

    protected <T> T deserialise(String json, Class<T> type) throws IOException {
        return MAPPER.readValue(json, type);
    }

    @Test
    public void canSelectAndProject() {
        String outputValue = "O";
        String inputValue = "I";
        MockFunctionContext context = new MockFunctionContext();
        MockTransform mock = new MockTransform(outputValue);
        Reference<String> selection = new Reference("a");
        Reference<String> projection = new Reference("b", "c");

        context.setFunction(mock);
        context.setSelection(selection);
        context.setProjection(projection);

        MapTuple<String> tuple = new MapTuple<>();
        tuple.put("a", inputValue);

        context.project(tuple, context.getFunction().execute(context.select(tuple)));

        assertEquals("Unexpected value at reference a", inputValue, tuple.get("a"));
        assertEquals("Unexpected value at reference b", inputValue, tuple.get("b"));
        assertEquals("Unexpected value at reference c", outputValue, tuple.get("c"));
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        MockFunctionContext context = new MockFunctionContext();
        MockTransform mock = new MockTransform("a");
        Reference<String> selection = new Reference("a");
        Reference<String> projection = new Reference("b", "c");

        context.setFunction(mock);
        context.setSelection(selection);
        context.setProjection(projection);

        String json = serialise(context);
        MockFunctionContext deserialisedContext = deserialise(json, MockFunctionContext.class);

        // check deserialisation
        assertNotNull(deserialisedContext);
        Reference<String> deserialisedSelection = deserialisedContext.getSelection();
        Reference<String> deserialisedProjection = deserialisedContext.getProjection();
        assertNotNull(deserialisedContext.getFunction());
        assertNotSame(mock, deserialisedContext.getFunction());
        assertNotSame(context, deserialisedContext);
        assertNotSame(selection, deserialisedSelection);
        assertNotSame(projection, deserialisedProjection);
        assertTrue(deserialisedSelection.isFieldReference());
        assertEquals("a", deserialisedSelection.getField());
        assertTrue(deserialisedProjection.isTupleReference());
        Reference<String>[] deserialisedProjectionFields = deserialisedProjection.getTupleReferences();
        assertEquals(2, deserialisedProjectionFields.length);
        assertEquals("b", deserialisedProjectionFields[0].getField());
        assertEquals("c", deserialisedProjectionFields[1].getField());
    }
}
