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

package uk.gov.gchq.gaffer.rest;

import org.apache.commons.io.FileUtils;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.IOUtils;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.rest.application.ApplicationConfig;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class AbstractRestApiIT {
    protected final static JSONSerialiser JSON_SERIALISER = new JSONSerialiser();
    protected static final Element[] DEFAULT_ELEMENTS = {
            new uk.gov.gchq.gaffer.data.element.Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("1")
                    .property(TestPropertyNames.COUNT, 1)
                    .build(),
            new uk.gov.gchq.gaffer.data.element.Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("2")
                    .property(TestPropertyNames.COUNT, 2)
                    .build(),
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(TestPropertyNames.COUNT, 3)
                    .build()};
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRestApiIT.class);
    private static final Client client = ClientBuilder.newClient();
    private static final String REST_URI = "http://localhost:8080/rest/v1";
    private static HttpServer server;

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private final String storePropertiesResourcePath;
    private final String schemaResourcePath;

    public AbstractRestApiIT() {
        this(StreamUtil.SCHEMA, StreamUtil.STORE_PROPERTIES, DEFAULT_ELEMENTS);
    }

    public AbstractRestApiIT(final String schemaResourcePath, final String storePropertiesResourcePath, final Element... elements) {
        this.schemaResourcePath = schemaResourcePath;
        this.storePropertiesResourcePath = storePropertiesResourcePath;
    }

    @AfterClass
    public static void afterClass() {
        stopServer();
    }

    @Before
    public void before() throws IOException {
        reinitialiseGraph();
    }

    private static void stopServer() {
        if (null != server) {
            server.shutdownNow();
            server = null;
        }
    }

    protected void reinitialiseGraph() throws IOException {
        try (final InputStream stream = StreamUtil.openStream(AbstractRestApiIT.class, schemaResourcePath)) {
            FileUtils.writeByteArrayToFile(testFolder.newFile("schema.json"), IOUtils.readFully(stream, stream.available(), true));
        }

        try (final InputStream stream = StreamUtil.openStream(AbstractRestApiIT.class, storePropertiesResourcePath)) {
            FileUtils.writeByteArrayToFile(testFolder.newFile("store.properties"), IOUtils.readFully(stream, stream.available(), true));
        }

        // set properties for REST service
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, testFolder.getRoot() + "/store.properties");
        System.setProperty(SystemProperty.SCHEMA_PATHS, testFolder.getRoot() + "/schema.json");

        GraphFactory.setGraph(null);

        initialise();
        checkRestServiceStatus();
    }

    protected void addElements(final Element... elements) throws IOException {
        executeOperation(new AddElements.Builder()
                .elements(elements)
                .build());
    }

    protected Response executeOperation(final Operation<?, ?> operation) throws IOException {
        initialise();
        return client.target(REST_URI)
                .path("/graph/doOperation")
                .request()
                .post(Entity.entity(JSON_SERIALISER.serialise(new OperationChain<>(operation)), MediaType.APPLICATION_JSON_TYPE));
    }

    protected Response executeOperationChain(final OperationChain opChain) throws IOException {
        initialise();
        return client.target(REST_URI)
                .path("/graph/doOperation")
                .request()
                .post(Entity.entity(JSON_SERIALISER.serialise(opChain), MediaType.APPLICATION_JSON_TYPE));
    }

    protected Response executeOperationChainChunked(final OperationChain opChain) throws IOException {
        initialise();
        return client.target(REST_URI)
                .path("/graph/doOperation/chunked")
                .request()
                .post(Entity.entity(JSON_SERIALISER.serialise(opChain), MediaType.APPLICATION_JSON_TYPE));
    }

    protected void verifyElements(final Element[] expected, final List<Element> actual) {
        assertEquals(expected.length, actual.size());
        assertThat(actual, IsCollectionContaining.hasItems(expected));
    }

    private void initialise() throws IOException {
        if (null == server) {
            server = GrizzlyHttpServerFactory.createHttpServer(URI.create(REST_URI), new ApplicationConfig());
        }
    }

    private void checkRestServiceStatus() {
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
