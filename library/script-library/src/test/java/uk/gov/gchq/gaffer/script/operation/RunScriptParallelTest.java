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

package uk.gov.gchq.gaffer.script.operation;

import org.junit.Test;

import uk.gov.gchq.gaffer.script.operation.handler.RunScriptHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.fail;

public class RunScriptParallelTest {

    @Test
    public void parallelTest() {
        //create a callable for each method
        Callable<Void> callable = () -> {
            try {
                runScript();
            } catch (Exception e) {
                e.printStackTrace();
                fail("Error running parallel");
            }
            return null;
        };

        List<Callable<Void>> taskList = new ArrayList<Callable<Void>>();
        for (int i = 0; i < 5; i++) {
            taskList.add(callable);
        }

        //create a pool executor with 3 threads
        ExecutorService executor = Executors.newFixedThreadPool(20);

        try {
            //start the threads and wait for them to finish
            executor.invokeAll(taskList);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            fail();
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (final InterruptedException e) {
            executor.shutdownNow();
            fail();
        }
    }

    private void runScript() throws Exception {

        RunScriptHandler rPSH = new RunScriptHandler();
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
        StringBuilder results = (StringBuilder) rPSH.doOperation(runScript, null, null);
    }
}
