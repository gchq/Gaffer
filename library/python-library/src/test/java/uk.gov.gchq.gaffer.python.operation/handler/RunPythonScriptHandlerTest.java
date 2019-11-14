/*
 * Copyright 2019 Crown Copyright
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
package uk.gov.gchq.gaffer.python.operation.handler;

import org.junit.Assert;
import org.junit.Test;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.python.operation.RunPythonScript;
import uk.gov.gchq.gaffer.python.operation.ScriptInputType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RunPythonScriptHandlerTest {

    @Test
    public void shouldReturnDataInJSON() {
        // Given
        RunPythonScriptHandler rPSH = new RunPythonScriptHandler();
        final String scriptName = "script1";
        final Map<String, Object> scriptParameters = new HashMap<String, Object>() { {
            put("a", "b");
        } };

        final ScriptInputType scriptInputType = ScriptInputType.DATAFRAME;
        final ArrayList<String> inputData = new ArrayList<>();
        inputData.add("{\"Test Data\"}");

        final RunPythonScript<String, Iterable<? extends String>> runPythonScript =
                new RunPythonScript.Builder<String, Iterable<? extends String>>()
                        .scriptName(scriptName)
                        .scriptParameters(scriptParameters)
                        .scriptInputType(scriptInputType)
                        .build();

        runPythonScript.setInput(inputData);

        // When
        StringBuilder results = null;
        try {

            results = (StringBuilder) rPSH.doOperation(runPythonScript, null, null);
        } catch (OperationException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // Then
        assert results != null;
        Assert.assertEquals("[{\"0\":\"{\\\"Test Data\\\"}\"}]", results.toString());
    }
}
