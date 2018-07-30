/*
 * Copyright 2016-2018 Crown Copyright
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
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;

import org.apache.commons.io.FileUtils;

import uk.gov.gchq.gaffer.accumulostore.utils.TableUtils;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.HashMap;
import java.util.Map;

/**
 * An {@link AccumuloStore} that uses an Accumulo {@link MiniAccumuloCluster} to
 * provide a {@link Connector}.
 */
public class MockAccumuloStore extends AccumuloStore implements AutoCloseable {
    private static final Map<String, MiniAccumuloCluster> MACS = new HashMap<>();
    private static final Map<String, Path> MAC_FILES = new HashMap<>();

    private Connector connection = null;

    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        super.initialise(graphId, schema, properties);
        String auths = properties.get("miniaccumulo.user.auths");
        if (null != auths) {
            setUserAuths(properties.get("miniaccumulo.user.auths").split(","));
        }
    }

    public void close() throws StoreException {
        MiniAccumuloCluster mac = MACS.get(getGraphId());
        if (null != mac) {
            try {
                mac.stop();
            } catch (final IOException | InterruptedException e) {
                throw new StoreException(e.getMessage(), e);
            }
            MACS.remove(getGraphId());
            deleteFiles(MAC_FILES.get(getGraphId()));
            MAC_FILES.remove(getGraphId());
        }
    }

    public Connector getConnection() throws StoreException {
        if (null == connection) {
            AccumuloProperties properties = createMacAndGetProperties(getProperties());
            connection = TableUtils.getConnector(properties.getInstance(), properties.getZookeepers(),
                    properties.getUser(), properties.getPassword());
        }
        return connection;
    }

    private AccumuloProperties createMacAndGetProperties(final AccumuloProperties properties) throws StoreException {
        MiniAccumuloCluster mac = MACS.get(getGraphId());
        if (mac != null) {
            properties.setInstance(mac.getInstanceName());
            properties.setZookeepers(mac.getZooKeepers());
            properties.setUser("root");
            properties.setPassword("password");
            return properties;
        }
        Path files;
        try {
            files = Files.createTempDirectory("accumulo", new FileAttribute[]{});
        } catch (final IOException e) {
            throw new StoreException(e.getMessage(), e);
        }
        MAC_FILES.put(getGraphId(), files);

        try {
            mac = new MiniAccumuloCluster(files.toFile(), "password");
        } catch (final IOException e) {
            throw new StoreException(e.getMessage(), e);
        }
        try {
            mac.start();
        } catch (final IOException | InterruptedException e) {
            throw new StoreException(e.getMessage(), e);
        }
        properties.setInstance(mac.getInstanceName());
        properties.setZookeepers(mac.getZooKeepers());
        properties.setUser("root");
        properties.setPassword("password");
        MACS.put(getGraphId(), mac);
        return properties;
    }

    protected static void deleteFiles(final Path path) throws StoreException {
        if (null != path && path.toFile().exists()) {
            try {
                FileUtils.deleteDirectory(path.toFile());
            } catch (final IOException e) {
                throw new StoreException(e.getMessage(), e);
            }
        }
    }

    public void setUserAuths(final String... auths) throws StoreException {
        try {
            getConnection().securityOperations().changeUserAuthorizations("root", new Authorizations(auths));
        } catch (final AccumuloException | AccumuloSecurityException e) {
            throw new StoreException(e.getMessage(), e);
        }
    }

    OperationHandler getOperationHandlerExposed(final Class<? extends Operation> opClass) {
        return super.getOperationHandler(opClass);
    }

}
