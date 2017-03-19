/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.rest;

import org.apache.commons.io.FileUtils;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.junit.rules.TemporaryFolder;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.rest.application.ApplicationConfig;
import uk.gov.gchq.gaffer.rest.factory.DefaultGraphFactory;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import static org.junit.Assert.assertEquals;

public class RestApiTestUtil {
    public static final String REST_URI = "http://localhost:8080/rest/v1";
    public static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();
    private static final Client client = ClientBuilder.newClient();
    private static HttpServer server;

    private RestApiTestUtil() {
        // This class should not be constructed it only has utility methods
    }

    public static void stopServer() {
        if (null != server) {
            server.shutdownNow();
            server = null;
        }
    }

    public static boolean isRunning() {
        return null != server;
    }

    public static void reinitialiseGraph(final TemporaryFolder testFolder) throws IOException {
        reinitialiseGraph(testFolder, StreamUtil.SCHEMA, StreamUtil.STORE_PROPERTIES);
    }

    public static void reinitialiseGraph(final TemporaryFolder testFolder, final String schemaResourcePath, final String storePropertiesResourcePath) throws IOException {
        reinitialiseGraph(testFolder,
                Schema.fromJson(StreamUtil.openStream(RestApiTestUtil.class, schemaResourcePath)),
                StoreProperties.loadStoreProperties(StreamUtil.openStream(RestApiTestUtil.class, storePropertiesResourcePath))
        );
    }

    public static void reinitialiseGraph(final TemporaryFolder testFolder, final Schema schema, final StoreProperties storeProperties) throws IOException {
        FileUtils.writeByteArrayToFile(testFolder.newFile("schema.json"), schema.toJson(true));

        try (OutputStream out = new FileOutputStream(testFolder.newFile("store.properties"))) {
            storeProperties.getProperties().store(out, "This is an optional header comment string");
        }

        // set properties for REST service
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, testFolder.getRoot() + "/store.properties");
        System.setProperty(SystemProperty.SCHEMA_PATHS, testFolder.getRoot() + "/schema.json");

        reinitialiseGraph();
    }


    public static void reinitialiseGraph() throws IOException {
        DefaultGraphFactory.setGraph(null);

        startServer();
        checkRestServiceStatus();
    }

    public static void addElements(final Element... elements) throws IOException {
        executeOperation(new AddElements.Builder()
                .elements(elements)
                .build());
    }

    public static Response executeOperation(final Operation<?, ?> operation) throws IOException {
        startServer();
        return client.target(REST_URI)
                .path("/graph/doOperation")
                .request()
                .post(Entity.entity(JSON_SERIALISER.serialise(new OperationChain<>(operation)), MediaType.APPLICATION_JSON_TYPE));
    }

    public static Response executeOperationChain(final OperationChain opChain) throws IOException {
        startServer();
        return client.target(REST_URI)
                .path("/graph/doOperation")
                .request()
                .post(Entity.entity(JSON_SERIALISER.serialise(opChain), MediaType.APPLICATION_JSON_TYPE));
    }

    public static Response executeOperationChainChunked(final OperationChain opChain) throws IOException {
        startServer();
        return client.target(REST_URI)
                .path("/graph/doOperation/chunked")
                .request()
                .post(Entity.entity(JSON_SERIALISER.serialise(opChain), MediaType.APPLICATION_JSON_TYPE));
    }

    public static void startServer() throws IOException {
        if (null == server) {
            server = GrizzlyHttpServerFactory.createHttpServer(URI.create(REST_URI), new ApplicationConfig());
        }
    }

    private static void checkRestServiceStatus() {
        // Given
        final Response response = client.target(REST_URI)
                .path("status")
                .request()
                .get();

        // When
        final String statusMsg = response.readEntity(SystemStatus.class)
                .getDescription();

        // Then
        assertEquals("The system is working normally.", statusMsg);
    }

}
