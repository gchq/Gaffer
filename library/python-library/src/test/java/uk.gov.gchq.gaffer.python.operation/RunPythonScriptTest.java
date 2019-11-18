/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.python.operation;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class RunPythonScriptTest {

    @Test
    public void shouldShallowClone() {
        // Given
        final String scriptName = "script3";
        final Map<String, Object> scriptParameters = new HashMap<String, Object>() { {
            put("animal", "dog");
        } };
        final ScriptInputType scriptInputType = ScriptInputType.DATAFRAME;

        final RunPythonScript runPythonScript = new RunPythonScript.Builder<>()
                .scriptName(scriptName)
                .scriptParameters(scriptParameters)
                .scriptInputType(scriptInputType)
                .build();

        // When
        RunPythonScript clone = (RunPythonScript) runPythonScript.shallowClone();

        // Then
        Assert.assertNotSame(runPythonScript, clone);
        Assert.assertEquals(scriptName, clone.getScriptName());
        Assert.assertEquals(scriptParameters, clone.getScriptParameters());
        Assert.assertEquals(scriptInputType, clone.getScriptInputType());
    }
}
