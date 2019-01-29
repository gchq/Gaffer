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

package uk.gov.gchq.gaffer.store.operation.handler;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.impl.RunScript;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclarations;
import uk.gov.gchq.gaffer.store.operation.handler.util.RunScriptUsingScriptEngine;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RunScriptHandlerTest extends JSONSerialisationTest<RunScriptHandler> {
    @Test
    public void shouldRunJavaScript() throws Exception {
        _shouldRunJavaScript("JavaScript");
    }

    @Test
    public void shouldRunJavaScriptLowerCase() throws Exception {
        // Given
        _shouldRunJavaScript("javascript");
    }

    @Test
    public void shouldRunJS() throws Exception {
        // Given
        _shouldRunJavaScript("js");
    }

    private void _shouldRunJavaScript(String type) throws uk.gov.gchq.gaffer.operation.OperationException {
        // Given
        final int input = 1;
        final RunScript<Integer, Double> operation = new RunScript.Builder<Integer, Double>()
                .input(input)
                .script("function apply(input) { return input + 1; };")
                .type(type)
                .build();

        final RunScriptHandler<Integer, Double> handler = new RunScriptHandler<>();

        // When
        final double result = handler.doOperation(operation, null, null);

        // Then
        assertEquals(2.0, result, 0.000001);
    }

    @Test
    public void shouldRunPython() throws Exception {
        // Given
        final int input = 1;
        final RunScript<Integer, Integer> operation = new RunScript.Builder<Integer, Integer>()
                .input(input)
                .script("def apply(input): return input + 1")
                .type("python")
                .build();

        final RunScriptHandler<Integer, Integer> handler = new RunScriptHandler<>();
        handler.addType("(?i)(python)", new RunScriptUsingScriptEngine("python"));

        // When
        final int result = handler.doOperation(operation, null, null);

        // Then
        assertEquals(2, result);
    }


    @Override
    protected RunScriptHandler getTestObject() {
        return new RunScriptHandler();
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() {
        // Given
        final RunScriptHandler obj = getTestObject();

        // When
        final byte[] json = toJson(obj);
        final RunScriptHandler deserialisedObj = fromJson(json);

        // Then
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"scriptTypes\" : [ {%n" +
                "    \"regex\" : \"(?i)(javascript)\",%n" +
                "    \"handler\" : {%n" +
                "      \"class\" : \"uk.gov.gchq.gaffer.store.operation.handler.util.RunScriptUsingScriptEngine\",%n" +
                "      \"type\" : \"JavaScript\"%n" +
                "    }%n" +
                "  }, {%n" +
                "    \"regex\" : \"(?i)(js)\",%n" +
                "    \"handler\" : {%n" +
                "      \"class\" : \"uk.gov.gchq.gaffer.store.operation.handler.util.RunScriptUsingScriptEngine\",%n" +
                "      \"type\" : \"JavaScript\"%n" +
                "    }%n" +
                "  } ]%n" +
                "}"), new String(json));
        assertNotNull(deserialisedObj);
    }

    @Test
    public void shouldLoadOperationDeclarations() throws IOException {
        // When
        InputStream stream = StreamUtil.openStream(RunScriptHandler.class, "RunScriptOperationDeclarations.json");

        // Given
        OperationDeclarations opDeclarations = JSONSerialiser.deserialise(IOUtils.toByteArray(stream), OperationDeclarations.class);

        // Then
        assertEquals(1, opDeclarations.getOperations().size());
        assertEquals(RunScript.class, opDeclarations.getOperations().get(0).getOperation());

        final RunScriptHandler handler = (RunScriptHandler) opDeclarations.getOperations().get(0).getHandler();
        final RunScriptHandler<Integer, Integer> expectedHandler = new RunScriptHandler<>();
        assertEquals(expectedHandler, handler);
    }

    @Test
    public void shouldLoadOperationDeclarationsWithPython() throws IOException {
        // When
        InputStream stream = StreamUtil.openStream(RunScriptHandler.class, "RunScriptWithPythonOperationDeclarations.json");

        // Given
        OperationDeclarations opDeclarations = JSONSerialiser.deserialise(IOUtils.toByteArray(stream), OperationDeclarations.class);

        // Then
        assertEquals(1, opDeclarations.getOperations().size());
        assertEquals(RunScript.class, opDeclarations.getOperations().get(0).getOperation());

        final RunScriptHandler handler = (RunScriptHandler) opDeclarations.getOperations().get(0).getHandler();
        final RunScriptHandler<Integer, Integer> expectedHandler = new RunScriptHandler<>();
        expectedHandler.addType("(?i)(python)", new RunScriptUsingScriptEngine("python"));
        assertEquals(expectedHandler, handler);
    }
}
