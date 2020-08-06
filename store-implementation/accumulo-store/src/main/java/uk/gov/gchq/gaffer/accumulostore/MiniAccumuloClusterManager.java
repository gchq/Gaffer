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

package uk.gov.gchq.gaffer.accumulostore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.store.StoreProperties;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class MiniAccumuloClusterManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(MiniAccumuloClusterManager.class);
    private static final String BASE_DIRECTORY = "miniAccumuloStoreTest-";
    public static final String ROOTPW = "password";
    private MiniAccumuloCluster miniAccumuloCluster = null;
    private AccumuloProperties accumuloProperties = null;

    public MiniAccumuloClusterManager(final StoreProperties inputProperties, final String homeDirectory) {
        // Check if we need a mini cluster set up from reading the properties
        final String storeClass = inputProperties.getStoreClass();
        if (null == storeClass) {
            Class currentClass = new Object() { }.getClass().getEnclosingClass();
            throw new IllegalArgumentException(currentClass.getName() +
                    ": The Store class name was not found in the store properties for key: " +
                    StoreProperties.STORE_CLASS);
        }
        if (storeClass.equals(SingleUseMiniAccumuloStore.class.getName()) ||
            storeClass.equals(MiniAccumuloStore.class.getName())) {
            setUpTestCluster((AccumuloProperties) inputProperties, homeDirectory);
        }
    }

    public AccumuloProperties getStoreProperties() {
        return accumuloProperties;
    }

    // Provided for access purposes for creating users etc. where required for tests.
    public MiniAccumuloCluster getCluster() {
        return miniAccumuloCluster;
    }

    private void setUpTestCluster(final AccumuloProperties suppliedProperties, final String directory) {
        accumuloProperties = suppliedProperties;
        File targetDir = new File(directory);
        File baseDir = new File(targetDir, BASE_DIRECTORY + UUID.randomUUID());

        try {
            FileUtils.deleteDirectory(baseDir);
            MiniAccumuloConfig miniAccumuloConfig = new MiniAccumuloConfig(baseDir, ROOTPW);
            miniAccumuloConfig.setInstanceName(suppliedProperties.getInstance());
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    MiniAccumuloClusterManager.this.close();
                }
            });
            miniAccumuloCluster = new MiniAccumuloCluster(miniAccumuloConfig);
            miniAccumuloCluster.start();
        } catch (final IOException | InterruptedException e) {
            LOGGER.error("Failed to start test MiniAccumuloCluster: " + e.getMessage());
            this.close();
        }

        // Create the user specified in the properties (if not root)
        // together with the specified password and give them all authorisations
        try {
            if (!suppliedProperties.getUser().equalsIgnoreCase("root")) {
                miniAccumuloCluster.getConnector("root", ROOTPW).securityOperations()
                        .createLocalUser(suppliedProperties.getUser(), new PasswordToken(suppliedProperties.getPassword()));
                miniAccumuloCluster.getConnector("root", ROOTPW).securityOperations()
                        .grantSystemPermission(suppliedProperties.getUser(), SystemPermission.CREATE_TABLE);
                miniAccumuloCluster.getConnector("root", ROOTPW).securityOperations()
                        .grantSystemPermission(suppliedProperties.getUser(), SystemPermission.CREATE_NAMESPACE);
            }
            Authorizations auths = new Authorizations("public", "private", "publicVisibility", "privateVisibility", "vis1", "vis2");
            miniAccumuloCluster.getConnector("root", ROOTPW).securityOperations()
                    .changeUserAuthorizations(suppliedProperties.getUser(), auths);
        } catch (final AccumuloException | AccumuloSecurityException e) {
            LOGGER.error("Failed to complete setup of the test MiniAccumuloCluster: " + e.getMessage());
            this.close();
        }

        // Update the properties with the connection details
        suppliedProperties.setInstance(miniAccumuloCluster.getInstanceName());
        suppliedProperties.setZookeepers(miniAccumuloCluster.getZooKeepers());
    }

    public void close() {
        if (null == miniAccumuloCluster) {
            return;
        }
        for (int retries = 0; retries < 5; retries++) {
            try {
                // Try one more time.
                miniAccumuloCluster.stop();
                break;
            } catch (final IOException | InterruptedException e) {
                LOGGER.error("Failed to stop test MiniAccumuloCluster: " + e.getMessage());
            }
        }
        for (int retries = 0; retries < 3; retries++) {
            try {
                FileUtils.deleteDirectory(new File(miniAccumuloCluster.getConfig().getDir().getAbsolutePath()));
                break;
            } catch (final IOException e) {
                LOGGER.error("Failed to delete test MiniAccumuloCluster directory: " +
                        miniAccumuloCluster.getConfig().getDir().getAbsolutePath() +
                        " : " + e.getMessage());
            }
        }
        miniAccumuloCluster = null;
    }

}
