/*
 * Copyright 2021 Crown Copyright
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

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.rest.integration.server.GafferInstance;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test to ensure rest API calls either
 * - via MapStore rest api
 * - via FederatedStore -> ProxyStore -> MapStore
 * return the same results
 *
 *     (rest api)
 *     FederatedStore
 *        ProxyStore -----------> (rest api)
 *                                MapStore
 */
public class FederatedStoreToProxyStoreIT {

    private static final int FEDERATED_STORE_REST_API_PORT = 8081;
    private static final int MAP_STORE_REST_API_PORT = 8080;
    private GafferInstance federatedStoreInstance;
    private GafferInstance mapStoreInstance;

    @TempDir
    Path mapStoreInstanceDir;

    @TempDir
    Path federatedStoreInstanceDir;

    private static final Entity ENTITY = new Entity.Builder()
            .group("BasicEntity")
            .vertex("myVertex")
            .property("property1", 1)
            .build();

    @BeforeEach
    public void startInstances() throws Exception {
        final String pathToExecutable = GafferInstance.getPathToExecutable(FederatedStoreToProxyStoreIT.class, "/rest-api/spring-rest");
        federatedStoreInstance = new GafferInstance(
                pathToExecutable,
                FEDERATED_STORE_REST_API_PORT,
                "federatedstore",
                createFile(federatedStoreInstanceDir, "store.properties", "/singleUseFederatedStore.properties"),
                createFile(federatedStoreInstanceDir, "schema.json", "/schema/basicEntitySchema.json"));

        federatedStoreInstance.start();

        mapStoreInstance = new GafferInstance(
                pathToExecutable,
                MAP_STORE_REST_API_PORT,
                "mapstore",
                createFile(mapStoreInstanceDir, "store.properties", "/mapstore_without_visibility_support.properties"),
                createFile(mapStoreInstanceDir, "schema.json", "/schema/basicEntitySchema.json"));

        mapStoreInstance.start();
    }

    private Path createFile(final Path tempDir, final String fileName, final String resource) throws IOException {
        try (InputStream stream = FederatedStoreToProxyStoreIT.class.getResourceAsStream(resource)) {
            final Path tempFile = tempDir.resolve(fileName);
            IOUtils.copy(stream, Files.newOutputStream(tempFile));
            return tempFile;
        }
    }

    @AfterEach
    public void shutdownInstances() {
        if (federatedStoreInstance != null) {
            federatedStoreInstance.shutdown();
        }
        if (mapStoreInstance != null) {
            mapStoreInstance.shutdown();
        }
    }

    @Test
    public void launchApplication() throws Exception {
        loadDataIntoMapStoreInstance();
        connectFederatedStoreInstanceToMapStoreInstance();

        final String federatedResults = federatedStoreInstance.executeOperation(new GetAllElements.Builder().build(), String.class).getBody();
        final String mapResults = mapStoreInstance.executeOperation(new GetAllElements.Builder().build(), String.class).getBody();

        assertEquals(mapResults, federatedResults);
    }

    private void loadDataIntoMapStoreInstance() throws Exception {
        final AddElements addElements = new AddElements.Builder()
                .input(ENTITY)
                .build();

        mapStoreInstance.executeOperation(addElements, JSONObject.class);
    }

    private void connectFederatedStoreInstanceToMapStoreInstance() throws Exception {
        final ProxyProperties proxyProperties = new ProxyProperties();
        proxyProperties.setGafferContextRoot("rest");

        final AddGraph addGraph = new AddGraph.Builder()
                .graphId("remote_mapstore")
                .storeProperties(proxyProperties)
                .schema(new Schema())
                .build();

        federatedStoreInstance.executeOperation(addGraph, JSONObject.class);
    }
}
