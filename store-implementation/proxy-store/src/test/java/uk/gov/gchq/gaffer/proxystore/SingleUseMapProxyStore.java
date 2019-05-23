/*
 * Copyright 2016-2019 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.rest.RestApiTestClient;
import uk.gov.gchq.gaffer.rest.service.v2.RestApiV2TestClient;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;

/**
 * An extension of {@link ProxyStore} that starts a REST API backed by a
 * {@link SingleUseMapProxyStore} with the provided schema. This store
 * is useful for testing when there is no actual REST API to connect a ProxyStore to.
 * Each time this store is initialised it will reset the underlying graph, delete
 * any elements that had been added and initialise it with the new schema. The
 * server will not be restarted every time.
 * <p>
 * After using this store you must remember to call
 * SingleUseMapProxyStore.cleanUp to stop the server and delete the temporary folder.
 */
public class SingleUseMapProxyStore extends ProxyStore {
    public static final TemporaryFolder TEST_FOLDER = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    private static final RestApiTestClient CLIENT = new RestApiV2TestClient();

    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties proxyProps) throws StoreException {
        startMapStoreRestApi(schema);
        super.initialise(graphId, new Schema(), proxyProps);
    }

    protected void startMapStoreRestApi(final Schema schema) throws StoreException {
        try {
            TEST_FOLDER.delete();
            TEST_FOLDER.create();
        } catch (final IOException e) {
            throw new StoreException("Unable to create temporary folder", e);
        }

        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(
                StreamUtil.openStream(getClass(), "map-store.properties"));
        try {
            CLIENT.reinitialiseGraph(TEST_FOLDER, schema, storeProperties);
        } catch (final IOException e) {
            throw new StoreException("Unable to reinitialise delegate graph", e);
        }
    }

    public static void cleanUp() {
        TEST_FOLDER.delete();
        CLIENT.stopServer();
    }
}
