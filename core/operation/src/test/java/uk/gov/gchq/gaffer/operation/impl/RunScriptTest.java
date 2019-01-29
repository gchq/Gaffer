/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl;

import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;


public class RunScriptTest extends OperationTest<RunScript> {

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("script", "type");
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final int input = 1;
        final String script = "input + 1";
        final RunScript operation = new RunScript.Builder<>()
                .input(input)
                .script(script)
                .type("javascript")
                .build();

        // Then
        assertEquals(input, operation.getInput());
        assertEquals(script, operation.getScript());
        assertEquals(input, operation.getInput());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final int input = 1;
        final String script = "input + 1";
        String type = "javascript";
        final RunScript operation = new RunScript.Builder<>()
                .input(input)
                .script(script)
                .type(type)
                .build();

        // When
        final RunScript clone = operation.shallowClone();

        // Then
        assertEquals(input, clone.getInput());
        assertEquals(script, clone.getScript());
        assertEquals(type, clone.getType());
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(Object.class, outputClass);
    }

    @Override
    protected RunScript getTestObject() {
        return new RunScript();
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() {
        // Given
        final int input = 1;
        final String script = "input + 1";
        final String type = "javascript";
        final RunScript operation = new RunScript.Builder<>()
                .input(input)
                .script(script)
                .type(type)
                .build();

        // When
        final byte[] json = toJson(operation);
        final RunScript deserialisedObj = fromJson(json);

        // Then
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.operation.impl.RunScript\",%n" +
                "  \"input\" : 1,%n" +
                "  \"script\" : \"input + 1\",%n" +
                "  \"type\" : \"javascript\"%n" +
                "}"), new String(json));
        assertNotNull(deserialisedObj);
        assertEquals(input, deserialisedObj.getInput());
        assertEquals(script, deserialisedObj.getScript());
        assertEquals(type, deserialisedObj.getType());
    }
}
