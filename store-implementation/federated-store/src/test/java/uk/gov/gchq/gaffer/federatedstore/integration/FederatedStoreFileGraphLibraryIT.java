/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.integration;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.library.FileGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.CACHE_SERVICE_CLASS_STRING;

/**
 * This test was created for gh-3129 and ensures the dynamic schema
 * used in the FederatedStore works correctly with FileGraphLibrary
 */
@TestMethodOrder(OrderAnnotation.class)
public class FederatedStoreFileGraphLibraryIT {
    private static Graph federatedGraph;
    private static MapStoreProperties mapStoreProperties;
    private static Schema mapStoreSchema;
    @TempDir
    private static Path libraryPath;
    private static String schemaPath;
    private static final String EMPTY_SCHEMA = "{\"types\":{}}";

    @BeforeAll
    static void setup() {
        mapStoreProperties = MapStoreProperties.loadStoreProperties("properties/singleUseMapStore.properties");
        mapStoreSchema = Schema.fromJson(StreamUtil.openStream(Schema.class, "schema/basicEntitySchema.json"));
        federatedGraph = getFederatedGraphUsingFileGraphLibrary();
        schemaPath = libraryPath + "/federatedGraphSchema.json";
    }

    @Test
    @Order(1)
    void shouldHaveBlankGraphLibrarySchemaFile() {
        // Given / When
        final Path schemaFilePath = Paths.get(schemaPath);

        // Then
        assertThat(schemaFilePath).hasContent(EMPTY_SCHEMA);
    }

    @Test
    @Order(2)
    void shouldAddGraphToFederatedStore() throws OperationException {
        // Given
        AddGraph addGraphOp = new AddGraph.Builder()
                .graphId("map1")
                .storeProperties(mapStoreProperties)
                .schema(mapStoreSchema)
                .build();

        // When
        federatedGraph.execute(addGraphOp, new Context());

        // Then
        assertThat(federatedGraph.execute(new GetSchema(), new Context())).isEqualTo(mapStoreSchema);
    }

    @Test
    @Order(3)
    void shouldBeAbleToRecreateFederatedStore() throws OperationException {
        // This simulates a restart of Gaffer where a Federated Store is created
        // with graphs having already been added to the FileGraphLibrary

        // Given / When
        Graph federatedGraph2 = getFederatedGraphUsingFileGraphLibrary();

        // Then
        // Ensure the graph added earlier is still there
        assertThat(federatedGraph2.execute(new GetSchema(), new Context())).isEqualTo(mapStoreSchema);
    }

    @Test
    @Order(4)
    void shouldBeAbleToRecreateFederatedStoreWhenSchemaDeleted() throws IOException, OperationException {
        // This simulates a restart of Gaffer as above but where the FileGraphLibrary
        // Schema file was deleted. This was a workaround and should continue to work.

        // Given
        final Path schemaFilePath = Paths.get(schemaPath);
        final Graph federatedGraph3;

        // When
        FileUtils.delete(schemaFilePath.toFile());
        federatedGraph3 = getFederatedGraphUsingFileGraphLibrary();

        // Then
        assertThat(federatedGraph3.execute(new GetSchema(), new Context())).isEqualTo(mapStoreSchema);
        // In this situation the Schema file contains the previously added graphs and is not empty
        assertThat(schemaFilePath).content().doesNotContain(EMPTY_SCHEMA);
    }

    static Graph getFederatedGraphUsingFileGraphLibrary() {
        FederatedStoreProperties federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.set(HashMapCacheService.STATIC_CACHE, String.valueOf(true));
        federatedStoreProperties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);
        FileGraphLibrary fileGraphLibrary = new FileGraphLibrary(libraryPath.toString());
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("federatedGraph")
                        .library(fileGraphLibrary)
                        .build())
                .storeProperties(federatedStoreProperties)
                .build();
    }
}
