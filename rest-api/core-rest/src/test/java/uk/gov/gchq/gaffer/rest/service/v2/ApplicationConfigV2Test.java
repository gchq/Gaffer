/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service.v2;

import org.apache.commons.io.FileUtils;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.rest.RestApiTestClient;
import uk.gov.gchq.gaffer.rest.SystemProperty;
import uk.gov.gchq.gaffer.rest.SystemStatus;
import uk.gov.gchq.gaffer.rest.application.ApplicationConfigV2;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import static org.junit.Assert.assertEquals;

public class ApplicationConfigV2Test {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private static final Client CLIENT = ClientBuilder.newClient();
    private HttpServer server;
    private String uriString = "http://localhost:8080/v2";

    @Before
    public void setUp() throws IOException {
        FileUtils.writeByteArrayToFile(testFolder.newFile("schema.json"), Schema.fromJson(StreamUtil.openStream(RestApiTestClient.class, StreamUtil.SCHEMA))
                .toJson(true));

        try (OutputStream out = new FileOutputStream(testFolder.newFile("store.properties"))) {
            StoreProperties.loadStoreProperties(StreamUtil.openStream(RestApiTestClient.class, StreamUtil.STORE_PROPERTIES)).getProperties()
                    .store(out, "This is an optional header comment string");
        }
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, testFolder.getRoot() + "/store.properties");
        System.setProperty(SystemProperty.SCHEMA_PATHS, testFolder.getRoot() + "/schema.json");
        System.setProperty(SystemProperty.GRAPH_ID, "graphId");
    }

    @After
    public void tearDown() {
        if (null != server) {
            server.shutdownNow();
            server = null;
        }
    }

    @Test
    public void shouldSetCorrectPathIfEmptyBasePathUsed() {
        System.setProperty(SystemProperty.BASE_PATH, "");
        server = GrizzlyHttpServerFactory.createHttpServer(URI.create(uriString), new ApplicationConfigV2());

        final SystemStatus status = CLIENT.target(uriString)
                .path("graph/status")
                .request()
                .get(SystemStatus.class);

        // Then
        assertEquals("The system is working normally.", status.getStatus().getDescription());
    }
}
