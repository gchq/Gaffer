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
package uk.gov.gchq.gaffer.script.operation.handler;

import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.script.operation.RunScript;
import uk.gov.gchq.gaffer.script.operation.ScriptTestConstants;
import uk.gov.gchq.gaffer.script.operation.platform.LocalDockerPlatform;
import uk.gov.gchq.gaffer.script.operation.provider.GitScriptProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RunScriptHandlerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunScriptHandlerTest.class);

    @Test
    public void shouldReturnDataInJSON() {
        // Given
        RunScriptHandler rPSH = new RunScriptHandler();
        rPSH.setRepoName(ScriptTestConstants.REPO_NAME);
        rPSH.setRepoURI(ScriptTestConstants.REPO_URI);
        rPSH.setImagePlatform(new LocalDockerPlatform());
        rPSH.setScriptProvider(new GitScriptProvider());
        final Map<String, Object> scriptParameters = new HashMap<String, Object>() { {
            put("a", "b");
        } };

        final ArrayList<String> inputData = new ArrayList<>();
        inputData.add("{\"Test Data\"}");

        final RunScript<String, Iterable<? extends String>> runScript =
                new RunScript.Builder<String, Iterable<? extends String>>()
                        .scriptName(ScriptTestConstants.SCRIPT_NAME)
                        .scriptParameters(scriptParameters)
                        .build();

        runScript.setInput(inputData);

        // When
        StringBuilder results = null;
        try {
            rPSH.setRepoName("test");
            rPSH.setRepoURI("https://github.com/g609bmsma/test");
            results = (StringBuilder) rPSH.doOperation(runScript, null, null);
        } catch (OperationException e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }

        // Then
        assert results != null;
        Assert.assertEquals("[\"{\\\"Test Data\\\"}\"]", results.toString());
    }
}
