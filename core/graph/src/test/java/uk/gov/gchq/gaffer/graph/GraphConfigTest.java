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

package uk.gov.gchq.gaffer.graph;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.data.elementdefinition.view.GlobalViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.hook.AddOperationsToChain;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.graph.hook.OperationChainLimiter;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GraphConfigTest extends JSONSerialisationTest<GraphConfig> {
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
        assertEquals((List) obj.getHooks().stream().map(GraphHook::getClass).collect(Collectors.toList()), (List) deserialisedObj.getHooks().stream().map(GraphHook::getClass).collect(Collectors.toList()));
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
                .addHook(hook1)
                .addHook(hook2)
                .view(view)
                .build();
    }
}