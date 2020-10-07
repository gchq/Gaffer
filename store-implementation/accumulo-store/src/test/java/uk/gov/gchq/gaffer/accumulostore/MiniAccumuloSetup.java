/*
 * Copyright 2020 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore;


import com.google.common.io.Files;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * MiniAccumuloSetup sets up an accumulo cluster. It sets the cluster once per test session, so multiple test classes
 * can use the same Accumulo cluster without the need to restart it manually.
 *
 * To use this, you must use the following store properties:
 * accumulo.instance=accumuloInstance
 * accumulo.zookeepers=localhost
 * accumulo.user=root
 * accumulo.password=password
 *
 * For Junit 5 tests you can annotate your test class with:
 * {@code @ExtendsWith(MiniAccumuloSetup.class) }
 *
 * This will start the MiniAccumuloCluster for you should you need it.
 *
 * For Junit 4 tests you need to create a static instance and manually call the {@code beforeAll()} method in a
 * {@code @BeforeClass } annotated static method.
 */
public class MiniAccumuloSetup implements BeforeAllCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(MiniAccumuloSetup.class);
    private static final int ZOOKEEPER_PORT = 2181;
    private static final String ROOT_USER = "root";
    private static final String DEFAULT_PASSWORD = "password";
    public static final String INSTANCE = "accumuloInstance";
    private static MiniAccumuloCluster mac;
    private File tempDir = Files.createTempDir();

    private synchronized void setupAccumuloCluster() throws AccumuloSecurityException, AccumuloException {
        if (mac == null) {
            MiniAccumuloConfig config = new MiniAccumuloConfig(tempDir, DEFAULT_PASSWORD);
            config.setZooKeeperPort(ZOOKEEPER_PORT);
            config.setInstanceName(INSTANCE);

            try {
                mac = new MiniAccumuloCluster(config);
                mac.start();

                LOGGER.info("Started Accumulo Cluster");
                LOGGER.info("Instance Name: " + INSTANCE);
                LOGGER.info("Zookeeper running on " + ZOOKEEPER_PORT);

            } catch (InterruptedException | IOException e) {
                LOGGER.warn("Failed to start Accumulo cluster", e);
                return;
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    MiniAccumuloSetup.this.tearDownAccumuloCluster();
                } catch (InterruptedException | IOException e) {
                    LOGGER.error("Failed to stop accumulo cluster", e);
                }
            }));

            Authorizations testAuths = new Authorizations("vis1", "vis2", "publicVisibility", "privateVisibility", "public", "private");
            mac.getConnector(ROOT_USER, DEFAULT_PASSWORD).securityOperations()
                    .changeUserAuthorizations(ROOT_USER, testAuths);
        }
    }


    private void tearDownAccumuloCluster() throws IOException, InterruptedException {
        if (mac != null) {
            mac.stop();
            mac = null;
        }

        tempDir.delete();
    }

    @Override
    public void beforeAll(final ExtensionContext context) throws Exception {
        setupAccumuloCluster();
    }

    // For use with Junit 4 tests
    public void beforeAll() throws Exception {
        beforeAll(null);
    }
}
