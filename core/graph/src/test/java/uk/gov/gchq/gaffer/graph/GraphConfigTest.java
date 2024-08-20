/*
 * Copyright 2017-2024 Crown Copyright
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

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class GraphConfigTest extends JSONSerialisationTest<GraphConfig> {

    @Test
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
        assertEquals(obj.getOtelActive(), deserialisedObj.getOtelActive());
        assertEquals((List) obj.getHooks().stream().map(GraphHook::getClass).collect(Collectors.toList()), (List) deserialisedObj.getHooks().stream().map(GraphHook::getClass).collect(Collectors.toList()));
    }

    @Test
    void shouldJsonDeserialiseFromHookPaths(@TempDir Path tmpDir) throws IOException {
        // Given
        final Path hook1Path = tmpDir.resolve("hook1Path");
        final Path hook2Path = tmpDir.resolve("hook2Path");
        Files.write(
                hook1Path,
                Arrays.asList(new JSONObject().put("class", Log4jLogger.class.getName()).toString()),
                StandardCharsets.UTF_8);
        Files.write(
                hook2Path,
                Arrays.asList(new JSONObject().put("class", AddOperationsToChain.class.getName()).toString()),
                StandardCharsets.UTF_8);

        final String json = new JSONObject()
                .put("graphId", "graphId1")
                .put("hooks", new JSONArray()
                        .put(new JSONObject()
                                .put("class", "uk.gov.gchq.gaffer.graph.hook.GraphHookPath")
                                .put("path", hook1Path.toAbsolutePath().toString().replace('\\', '/')))
                        .put(new JSONObject()
                                .put("class", "uk.gov.gchq.gaffer.graph.hook.GraphHookPath")
                                .put("path", hook2Path.toAbsolutePath().toString().replace('\\', '/')))
                        .put(new JSONObject()
                                .put("class", "uk.gov.gchq.gaffer.graph.hook.NamedOperationResolver")))
                .toString();

        // When
        final GraphConfig deserialisedObj = fromJson(json.getBytes());

        // Then
        assertThat(deserialisedObj).isNotNull();
        assertThat((List) deserialisedObj.getHooks().stream().map(GraphHook::getClass).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        Log4jLogger.class,
                        AddOperationsToChain.class,
                        NamedOperationResolver.class);
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
        final GraphHook hook2 = new OperationChainLimiter("testSuffix");

        return new GraphConfig.Builder()
                .graphId(graphId)
                .library(library)
                .description("testGraphConfig")
                .otelActive(true)
                .addHook(hook1)
                .addHook(hook2)
                .view(view)
                .build();
    }
}
