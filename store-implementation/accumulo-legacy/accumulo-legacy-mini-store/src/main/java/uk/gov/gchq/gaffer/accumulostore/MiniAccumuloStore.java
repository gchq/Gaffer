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
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

/**
 * A {@code MiniAccumuloStore} is an {@link AccumuloStore} which sets up an {@link MiniAccumuloCluster}
 * for each unique instance name. If you create two Accumulo stores with the same instance name,
 * the same cluster will be used for both. It's advisable to re-use an instance so that you don't
 * spend unnecessary time spinning up mini-clusters.
 *
 * It's dependencies mean it cannot be run in a REST API.
 *
 * If a user hasn't been created on the accumulo instance, it will be created for you with the
 * CREATE_NAMESPACE and CREATE_TABLE permissions.
 *
 * If you specify the authorisations in the store properties, the current authorisations for any
 * existing user will be overwritten.
 */
public class MiniAccumuloStore extends AccumuloStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(MiniAccumuloStore.class);

    private static final HashMap<String, MiniAccumuloCluster> CLUSTER_INSTANCES = new HashMap<>();
    private static final HashMap<String, File> ACCUMULO_DIRECTORIES = new HashMap<>();

    private static final String ROOT_PASSWORD_DEFAULT = "password";
    private static final int DEFAULT_ZOOKEEPER_PORT = 2181;

    private static final String ROOT_USER = "root";
    private static final String VISIBILITIES_PROPERTY = "accumulo.mini.visibilities";
    private static final String ROOT_PASSWORD_PROPERTY = "accumulo.mini.root.password";
    private static final String ACCUMULO_DIRECTORY_PROPERTY = "accumulo.mini.directory";


    @Override
    public void preInitialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        super.preInitialise(graphId, schema, properties);

        synchronized (this) {
            if (getCluster() == null) {
                try {
                    createCluster();
                } catch (final InterruptedException | IOException e) {
                    throw new StoreException("Failed to start accumulo cluster", e);
                }
            }
        }


        try {
            ensureUserExists(getCluster());
        } catch (final AccumuloException | AccumuloSecurityException e) {
            throw new StoreException("Failed to ensure user was added", e);
        }

    }

    private MiniAccumuloCluster getCluster() {
        return CLUSTER_INSTANCES.get(getProperties().getInstance());
    }

    private File getAccumuloDirectory() {
        return ACCUMULO_DIRECTORIES.get(getProperties().getInstance());
    }

    private void ensureUserExists(final MiniAccumuloCluster mac) throws AccumuloSecurityException, AccumuloException {
        String userName = getProperties().getUser();
        String rootPassword = getProperties().get(ROOT_PASSWORD_PROPERTY, ROOT_PASSWORD_DEFAULT);

        // Ensure user exists
        synchronized (this) {
            Set<String> currentUsers = mac.getConnector(ROOT_USER, rootPassword).securityOperations().listLocalUsers();
            if (!currentUsers.contains(userName)) {
                mac.getConnector(ROOT_USER, rootPassword).securityOperations().createLocalUser(
                        userName, new PasswordToken(getProperties().getPassword())
                );
                mac.getConnector(ROOT_USER, rootPassword).securityOperations()
                        .grantSystemPermission(userName, SystemPermission.CREATE_NAMESPACE);
                mac.getConnector(ROOT_USER, rootPassword).securityOperations()
                        .grantSystemPermission(userName, SystemPermission.CREATE_TABLE);
            }
        }

        // Add Auths
        String auths = getProperties().get(VISIBILITIES_PROPERTY);
        if (auths != null) {
            mac.getConnector(ROOT_USER, rootPassword).securityOperations()
                    .changeUserAuthorizations(userName, new Authorizations(auths.split(",")));
        }
    }

    @Override
    public Connector getConnection() throws StoreException {
        if (getCluster() == null) {
            throw new StoreException("MiniAccumuloCluster has not been initialised");
        }
        try {
            return getCluster().getConnector(getProperties().getUser(), getProperties().getPassword());
        } catch (final AccumuloSecurityException | AccumuloException e) {
            throw new StoreException("Failed to get Connection", e);
        }
    }

    private void createCluster() throws IOException, InterruptedException {
        String providedDirectory = getProperties().get(ACCUMULO_DIRECTORY_PROPERTY);
        if (providedDirectory == null) {
            ACCUMULO_DIRECTORIES.put(getProperties().getInstance(), Files.createTempDir());
        } else {
            ACCUMULO_DIRECTORIES.put(getProperties().getInstance(), new File(providedDirectory));
        }

        String rootUserPassword = getProperties().get(ROOT_PASSWORD_PROPERTY, ROOT_PASSWORD_DEFAULT);

        MiniAccumuloConfig config = new MiniAccumuloConfig(getAccumuloDirectory(), rootUserPassword);

        String[] zookeepers = getProperties().getZookeepers().split(":");
        if (zookeepers.length == 2) {
            config.setZooKeeperPort(Integer.parseInt(zookeepers[1]));
        } else {
            config.setZooKeeperPort(DEFAULT_ZOOKEEPER_PORT);
        }

        config.setInstanceName(getProperties().getInstance());

        CLUSTER_INSTANCES.put(getProperties().getInstance(), new MiniAccumuloCluster(config));
        getCluster().start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (this.getCluster() != null) {
                    this.getCluster().stop();
                }
            } catch (final InterruptedException | IOException e) {
                LOGGER.error("Failed to stop Accumulo", e);
            }
            getAccumuloDirectory().delete();
        }));
    }
}
