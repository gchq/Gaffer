/*
 * Copyright 2017-2023 Crown Copyright
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.cache.impl.HashMapCache;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.GraphSerialisable.Builder;
import uk.gov.gchq.gaffer.graph.hook.FunctionAuthoriser;
import uk.gov.gchq.gaffer.graph.hook.FunctionAuthoriserUtil;
import uk.gov.gchq.gaffer.graph.hook.NamedViewResolver;
import uk.gov.gchq.gaffer.integration.store.TestStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.serialisation.implementation.JavaSerialiser;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class GraphSerialisableTest {

    private GraphConfig config;
    private Schema schema;
    private Properties properties;
    private GraphSerialisable expected;

    @BeforeEach
    public void setUp() throws Exception {
        Mockito.reset(TestStore.mockStore);

        config = new GraphConfig.Builder()
                .graphId("testGraphId")
                .addHook(new NamedViewResolver())
                .addHook(new FunctionAuthoriser(FunctionAuthoriserUtil.DEFAULT_UNAUTHORISED_FUNCTIONS))
                .view(new View.Builder()
                        .entity("e1")
                        .build())
                .build();
        schema = new Schema.Builder()
                .entity("e1", new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .build())
                .type("string", String.class)
                .build();
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(TestStore.class);
        properties = storeProperties.getProperties();
        expected = new GraphSerialisable.Builder()
                .schema(schema)
                .properties(properties)
                .config(config)
                .build();
    }

    @AfterEach
    public void after() {
        Mockito.reset(TestStore.mockStore);
    }

    @Test
    public void shouldSerialiseAndDeserialise() throws Exception {
        // Given
        final JavaSerialiser javaSerialiser = new JavaSerialiser();
        final byte[] serialise = javaSerialiser.serialise(expected);
        final GraphSerialisable result = (GraphSerialisable) javaSerialiser.deserialise(serialise);

        // When / Then
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void shouldConsumeGraph() throws OperationException, StoreException {
        // Given
        final Graph graph = new Graph.Builder().addSchema(schema).addStoreProperties(new StoreProperties(properties)).config(config).build();
        final GraphSerialisable result = new GraphSerialisable.Builder(graph).build();

        // When / Then
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void shouldSerialiseWithJavaSerialiser() {
        // Given
        final HashMapCache<String, GraphSerialisable> cache = new HashMapCache<>(true);
        final String key = "key";
        final GraphSerialisable expected = new Builder().config(config).schema(schema).properties(properties).build();
        cache.put(key, expected);
        final GraphSerialisable actual = cache.get(key);

        // When / Then
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void shouldSerialiseWithJsonSerialiser() {
        // Given
        final HashMapCache<String, GraphSerialisable> cache = new HashMapCache<>(false);
        final String key = "key";
        final GraphSerialisable expected = new Builder().config(config).schema(schema).properties(properties).build();
        cache.put(key, expected);
        final GraphSerialisable actual = cache.get(key);

        // When / Then
        assertThat(actual).isEqualTo(expected);
    }
}
