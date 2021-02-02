/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.proxystore;

import org.junit.rules.TemporaryFolder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.rest.RestApiTestClient;
import uk.gov.gchq.gaffer.rest.factory.DefaultGraphFactory;
import uk.gov.gchq.gaffer.rest.service.v2.RestApiV2TestClient;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

/**
 * An extension of {@link ProxyStore} that starts a REST API backed by a
 * {@link SingleUseProxyStore} with the provided schema. This store
 * is useful for testing when there is no actual REST API to connect a ProxyStore to.
 * Each time this store is initialised it will reset the underlying graph, delete
 * any elements that had been added and initialise it with the new schema. The
 * server will not be restarted every time.
 * <p>
 * After using this store you must remember to call
 * SingleUseMapProxyStore.cleanUp to stop the server and delete the temporary folder.
 */
public abstract class SingleUseProxyStore extends ProxyStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleUseProxyStore.class);

    private TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    private static final Collection<RestApiTestClient> allClients = new HashSet<>();
    private static final Collection<TemporaryFolder> allFolders = new HashSet<>();
    private RestApiTestClient client;
    private boolean singletonGraph;

    public SingleUseProxyStore() {
        this(DefaultGraphFactory.DEFAULT_SINGLETON_GRAPH);
    }

    protected SingleUseProxyStore(boolean singletonGraph) {
        this.singletonGraph = singletonGraph;
    }

    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties proxyProps) throws StoreException {
        int gafferPort = ((ProxyProperties) proxyProps).getGafferPort();
        LOGGER.info("Initialising RestApiV2TestClient with proxyProperties port: " + gafferPort);
        client = new RestApiV2TestClient(gafferPort, singletonGraph);
        allClients.add(client);
        startRemoteStoreRestApi(schema);
        super.initialise(graphId, new Schema(), proxyProps);
        LOGGER.debug("testFolder = {}", testFolder.getRoot().toURI().toString());
    }

    protected void startRemoteStoreRestApi(final Schema schema) throws StoreException {
        try {
            testFolder.delete();
            testFolder.create();
        } catch (final IOException e) {
            throw new StoreException("Unable to create temporary folder", e);
        }

        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(
                StreamUtil.openStream(getClass(), getPathToDelegateProperties()));
        try {
            client.reinitialiseGraph(testFolder, schema, storeProperties);
        } catch (final IOException e) {
            throw new StoreException("Unable to reinitialise delegate graph", e);
        }
    }

    public static void cleanUp() {
        allFolders.forEach(TemporaryFolder::delete);
        allClients.forEach(RestApiTestClient::stopServer);
    }


    protected abstract String getPathToDelegateProperties();
}
