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

package uk.gov.gchq.gaffer.accumulostore.utils;

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

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.store.StoreException;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class MiniAccumuloClusterManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(MiniAccumuloClusterManager.class);
    private static final String BASE_DIRECTORY = "miniAccumuloStoreTest-";
    private static final String ROOTPW = "rootPW";
    private MiniAccumuloCluster miniAccumuloCluster = null;
    private MiniAccumuloConfig miniAccumuloConfig = null;
    private AccumuloProperties connectionProperties;

    public MiniAccumuloClusterManager (AccumuloProperties inputProperties) throws StoreException {
        connectionProperties = setUpTestDB(inputProperties);
    }

    public AccumuloProperties getProperties() {
        return connectionProperties;
    }

    private AccumuloProperties setUpTestDB(AccumuloProperties suppliedProperties) throws StoreException {

        File targetDir = new File("target");
        File baseDir;
        if (targetDir.exists() && targetDir.isDirectory()) {
            baseDir = new File(targetDir, BASE_DIRECTORY + UUID.randomUUID());
        } else {
            baseDir = new File(FileUtils.getTempDirectory(), BASE_DIRECTORY + UUID.randomUUID());
        }

        try {
            FileUtils.deleteDirectory(baseDir);
            miniAccumuloConfig = new MiniAccumuloConfig(baseDir, ROOTPW);
            miniAccumuloConfig.setInstanceName(suppliedProperties.getInstance());
            miniAccumuloCluster = new MiniAccumuloCluster(miniAccumuloConfig);
            miniAccumuloCluster.start();
        } catch (final IOException | InterruptedException e) {
            throw new StoreException(e.getMessage(), e);
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
            throw new StoreException(e.getMessage(), e);
        }

        // Create the new properties object to pass back, including connection items
        AccumuloProperties accumuloProperties = suppliedProperties.clone();
        accumuloProperties.setInstance(miniAccumuloCluster.getInstanceName());
        accumuloProperties.setZookeepers(miniAccumuloCluster.getZooKeepers());

        return accumuloProperties;
    }

    public void close() {
        if (null == miniAccumuloCluster) {
            return;
        }
        try {
            miniAccumuloCluster.stop();
        } catch (final IOException | InterruptedException e) {
            try {
                // Try one more time.
                miniAccumuloCluster.stop();
            } catch (final IOException | InterruptedException e2) {
                LOGGER.error("Failed to stop MiniAccumuloCluster: " + e2.getMessage());
            }
        }
        try {
            FileUtils.deleteDirectory(new File(miniAccumuloCluster.getConfig().getDir().getAbsolutePath()));
        } catch (final IOException e) {
            try {
                FileUtils.deleteDirectory(new File(miniAccumuloCluster.getConfig().getDir().getAbsolutePath()));
            } catch (final IOException e2) {
                LOGGER.error("Failed to delete MiniAccumuloCluster directory: " +
                        miniAccumuloCluster.getConfig().getDir().getAbsolutePath() +
                        " : " + e2.getMessage());
            }
        }
    }

}
