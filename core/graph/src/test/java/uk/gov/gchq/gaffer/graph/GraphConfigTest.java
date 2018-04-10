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

package uk.gov.gchq.gaffer.graph;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.elementdefinition.view.GlobalViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.hook.AddOperationsToChain;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.graph.hook.Log4jLogger;
import uk.gov.gchq.gaffer.graph.hook.NamedOperationResolver;
import uk.gov.gchq.gaffer.graph.hook.OperationChainLimiter;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class GraphConfigTest extends JSONSerialisationTest<GraphConfig> {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Override
    public void shouldJsonSerialiseAndDeserialise() {
        // Given
        final GraphConfig obj = getTestObject();

        // When
        final byte[] json = toJson(obj);
        final GraphConfig deserialisedObj = fromJson(json);

        // Then
        assertNotNull(deserialisedObj);
        assertEquals(obj.getGraphId(), deserialisedObj.getGraphId());
        assertEquals(obj.getView(), deserialisedObj.getView());
        assertEquals(obj.getLibrary().getClass(), deserialisedObj.getLibrary().getClass());
        assertEquals(obj.getDescription(), deserialisedObj.getDescription());
        assertEquals((List) obj.getHooks().stream().map(GraphHook::getClass).collect(Collectors.toList()), (List) deserialisedObj.getHooks().stream().map(GraphHook::getClass).collect(Collectors.toList()));
    }

    @Test
    public void shouldJsonDeserialiseFromHookPaths() throws IOException {
        // Given
        final File hook1Path = folder.newFile();
        final File hook2Path = folder.newFile();
        FileUtils.write(hook1Path, "{\"class\": \"" + Log4jLogger.class.getName() + "\"}");
        FileUtils.write(hook2Path, "{\"class\": \"" + AddOperationsToChain.class.getName() + "\"}");
        final String json = "{" +
                "  \"graphId\": \"graphId1\"," +
                "  \"hooks\": [" +
                "    {" +
                "      \"class\": \"uk.gov.gchq.gaffer.graph.hook.GraphHookPath\"," +
                "      \"path\": \"" + hook1Path.getAbsolutePath() + "\"" +
                "    }, " +
                "    {" +
                "      \"class\": \"uk.gov.gchq.gaffer.graph.hook.GraphHookPath\"," +
                "      \"path\": \"" + hook2Path.getAbsolutePath() + "\"" +
                "    }," +
                "    {" +
                "      \"class\": \"uk.gov.gchq.gaffer.graph.hook.NamedOperationResolver\"" +
                "    }" +
                "  ]" +
                "}";

        // When
        final GraphConfig deserialisedObj = fromJson(json.getBytes());

        // Then
        assertNotNull(deserialisedObj);
        assertEquals(Arrays.asList(Log4jLogger.class, AddOperationsToChain.class, NamedOperationResolver.class), (List) deserialisedObj.getHooks().stream().map(GraphHook::getClass).collect(Collectors.toList()));
    }

    @Test
    public void shouldReturnClonedView() throws Exception {
        // Given
        final String graphId = "graphId";
        final View view = new View.Builder().entity(TestGroups.ENTITY).build();

        // When
        final GraphConfig config = new GraphConfig.Builder()
                .graphId(graphId)
                .view(view)
                .build();

        // Then
        assertEquals(view, config.getView());
        assertNotSame(view, config.getView());
    }

    @Override
    protected GraphConfig getTestObject() {
        final String graphId = "graphId";

        final GraphLibrary library = new HashMapGraphLibrary();

        final View view = new View.Builder()
                .globalElements(new GlobalViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();

        final GraphHook hook1 = new AddOperationsToChain();
        final GraphHook hook2 = new OperationChainLimiter();

        return new GraphConfig.Builder()
                .graphId(graphId)
                .library(library)
                .description("testGraphConfig")
                .addHook(hook1)
                .addHook(hook2)
                .view(view)
                .build();
    }
}
