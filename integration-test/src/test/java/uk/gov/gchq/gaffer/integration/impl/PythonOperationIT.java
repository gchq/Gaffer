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
/*

package uk.gov.gchq.gaffer.integration.impl;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.python.operation.RunPythonScript;
import uk.gov.gchq.gaffer.python.operation.ScriptInputType;
import uk.gov.gchq.gaffer.python.operation.ScriptOutputType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PythonOperationIT extends AbstractStoreIT {

    @Test
    public void shouldRunHTMLScripts() throws OperationException {

        final String scriptName = "script3";
        final Map<String, Object> scriptParameters = new HashMap<String, Object>() {
            private static final long serialVersionUID = 6396114418310642015L;
            {
            put("animal", "dog");
        } };
        final String repoName = "test";
        final String repoURI = "https://github.com/g609bmsma/test";
        final String ip = "127.0.0.1";
        final ScriptOutputType scriptOutputType = ScriptOutputType.JSON;
        final ScriptInputType scriptInputType = ScriptInputType.DATAFRAME;

        final GetAllElements getAllElements =
                new GetAllElements.Builder().build();

        final RunPythonScript<Element, String> runPythonScript =
                new RunPythonScript.Builder<Element, String>()
                        .scriptName(scriptName)
                        .scriptParameters(scriptParameters)
                        .repoName(repoName)
                        .repoURI(repoURI)
                        .ip(ip)
                        .scriptOutputType(scriptOutputType)
                        .scriptInputType(scriptInputType)
                        .build();

        OperationChain<CloseableIterable<? extends String>> opChain =
                new OperationChain.Builder()
                        .first(getAllElements)
                        .then(new Limit.Builder<Element>().resultLimit(2).truncate(true).build())
                        .then(runPythonScript)
                        .build();

        final Object results = graph.execute(opChain, user);

        System.out.println("results are: " + results);

    }

    @Test
    public void shouldRunElementScripts() throws OperationException {

        final String scriptName = "script1";
        final Map<String, Object> scriptParameters = new HashMap<String, Object>() {
            private static final long serialVersionUID = -4026415949101416137L;

            {
            put("a", "b");
        } };
        final String repoName = "test";
        final String repoURI = "https://github.com/g609bmsma/test";
        final ScriptOutputType scriptOutputType = ScriptOutputType.ELEMENTS;
        final ScriptInputType scriptInputType = ScriptInputType.DATAFRAME;

        final GetAllElements getAllElements =
                new GetAllElements.Builder().build();

        final RunPythonScript<Element, Element> runPythonScript =
                new RunPythonScript.Builder<Element, Element>()
                        .scriptName(scriptName)
                        .scriptParameters(scriptParameters)
                        .repoName(repoName)
                        .repoURI(repoURI)
                        .scriptOutputType(scriptOutputType)
                        .scriptInputType(scriptInputType)
                        .build();

        OperationChain<CloseableIterable<? extends Element>> opChain =
                new OperationChain.Builder()
                        .first(getAllElements)
                        .then(new Limit.Builder<Element>().resultLimit(100).build())
                        .then(runPythonScript)
                        .build();

        final CloseableIterable<? extends Element> results = graph.execute(opChain, user);

        System.out.println("results are: " + results);
    }

    @Test
    public void shouldHandleMultipleInstances() {
        //create a callable for each method
        Callable<Void> callable = () -> {
            shouldRunElementScripts();
            return null;
        };

        Callable<Void> callable2 = () -> {
            shouldRunHTMLScripts();
            return null;
        };

        List<Callable<Void>> taskList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            taskList.add(callable);
        }
        taskList.add(callable2);
        for (int i = 0; i < 5; i++) {
            taskList.add(callable);
        }

        //create a pool executor with 3 threads
        ExecutorService executor = Executors.newFixedThreadPool(20);

        try {
            //start the threads and wait for them to finish
            executor.invokeAll(taskList);
        } catch (final InterruptedException ie) {
            //do something if you care about interruption;
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (final InterruptedException e) {
            executor.shutdownNow();
        }
    }
}
*/
