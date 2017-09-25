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
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.rest.factory.DefaultGraphFactory;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

public abstract class RestApiTestClient {
    protected final Client client = ClientBuilder.newClient();
    protected final ResourceConfig config;
    protected final String fullPath;
    protected final String root;
    protected final String path;
    protected final String versionString;
    protected final String uriString;
    protected HttpServer server;

    public RestApiTestClient(final String root, final String path, final String versionString, final ResourceConfig config) {
        this.root = root.replaceAll("/$", "");
        this.path = path.replaceAll("/$", "");
        this.versionString = versionString.replaceAll("/$", "");
        this.config = config;

        this.fullPath = this.path + '/' + versionString;
        this.uriString = this.root + '/' + this.fullPath;
    }

    public void stopServer() {
        if (null != server) {
            server.shutdownNow();
            server = null;
        }
    }

    public boolean isRunning() {
        return null != server;
    }

    public void reinitialiseGraph(final TemporaryFolder testFolder) throws IOException {
        reinitialiseGraph(testFolder, StreamUtil.SCHEMA, StreamUtil.STORE_PROPERTIES);
    }

    public void reinitialiseGraph(final TemporaryFolder testFolder, final String schemaResourcePath, final String storePropertiesResourcePath) throws IOException {
        reinitialiseGraph(testFolder,
                Schema.fromJson(StreamUtil.openStream(RestApiTestClient.class, schemaResourcePath)),
                StoreProperties.loadStoreProperties(StreamUtil.openStream(RestApiTestClient.class, storePropertiesResourcePath))
        );
    }

    public void reinitialiseGraph(final TemporaryFolder testFolder, final Schema schema, final StoreProperties storeProperties) throws IOException {
        FileUtils.writeByteArrayToFile(testFolder.newFile("schema.json"), schema
                .toJson(true));

        try (OutputStream out = new FileOutputStream(testFolder.newFile("store.properties"))) {
            storeProperties.getProperties()
                           .store(out, "This is an optional header comment string");
        }

        // set properties for REST service
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, testFolder.getRoot() + "/store.properties");
        System.setProperty(SystemProperty.SCHEMA_PATHS, testFolder.getRoot() + "/schema.json");
        System.setProperty(SystemProperty.GRAPH_ID, "graphId");

        reinitialiseGraph();
    }


    public void reinitialiseGraph() throws IOException {
        DefaultGraphFactory.setGraph(null);

        startServer();

        final SystemStatus status = getRestServiceStatus();

        if (SystemStatus.Status.UP != status.getStatus()) {
            throw new RuntimeException("The system status was not UP.");
        }
    }

    public void addElements(final Element... elements) throws IOException {
        executeOperation(new AddElements.Builder()
                .input(elements)
                .build());
    }

    public abstract Response executeOperation(final Operation operation) throws IOException;

    public abstract Response executeOperationChain(final OperationChain opChain) throws IOException;

    public abstract Response executeOperationChainChunked(final OperationChain opChain) throws IOException;

    public abstract Response executeOperationChunked(final Operation operation) throws IOException;

    public abstract SystemStatus getRestServiceStatus();

    public void startServer() throws IOException {
        if (null == server) {
            server = GrizzlyHttpServerFactory.createHttpServer(URI.create(uriString), config);
        }
    }

    public String getPath() {
        return path;
    }

    public String getVersionString() {
        return versionString;
    }

    public String getRoot() {
        return root;
    }

    public String getFullPath() {
        return fullPath;
    }

    public String getUriString() {
        return uriString;
    }
}
