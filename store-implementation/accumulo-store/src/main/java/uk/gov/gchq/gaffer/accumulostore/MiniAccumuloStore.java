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
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * An {@link AccumuloStore} that uses an Accumulo {@link MiniAccumuloCluster} to
 * provide a {@link Connector}.
 */
public class MiniAccumuloStore extends AccumuloStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloStore.class);

    private static final PasswordToken PASSWORD_TOKEN = new PasswordToken("rootPW");
    private static final String BASE_DIRECTORY = "miniAccumuloStoreTest-";
    private MiniAccumuloCluster miniAccumuloCluster = null;
    private MiniAccumuloConfig miniAccumuloConfig = null;
    private Connector miniConnector;

    @Override
    public Connector getConnection() throws StoreException {
        try {
            miniConnector = miniAccumuloCluster.getConnector(
                    "root",        // TODO: not sure user and pw should be hard coded but it might be acceptable if mini is JUST a replacement for mock.
                    "rootPW");
        } catch (final AccumuloException | AccumuloSecurityException e) {
            throw new StoreException(e.getMessage(), e);
        }
        return miniConnector;
    }

    @Override
    public void preInitialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        setProperties(properties);

        File targetDir = new File("target");
        File baseDir;
        if (targetDir.exists() && targetDir.isDirectory()) {
            baseDir = new File(targetDir, BASE_DIRECTORY + UUID.randomUUID());
        } else {
            baseDir = new File(FileUtils.getTempDirectory(), BASE_DIRECTORY + UUID.randomUUID());
        }

        try {
            FileUtils.deleteDirectory(baseDir);
            miniAccumuloConfig = new MiniAccumuloConfig(baseDir, "rootPW");
            miniAccumuloConfig.setInstanceName(getProperties().getInstance());
            miniAccumuloCluster = new MiniAccumuloCluster(miniAccumuloConfig);
            miniAccumuloCluster.start();
        } catch (final IOException | InterruptedException e) {
            throw new StoreException(e.getMessage(), e);
        }

        super.preInitialise(graphId, schema, getProperties());
    }

    @Override
    protected void addUserToConfiguration(final Configuration conf) throws AccumuloSecurityException {
        InputConfigurator.setConnectorInfo(AccumuloInputFormat.class,
                conf,
                "root",
                PASSWORD_TOKEN);
    }

    @Override
    protected void addZookeeperToConfiguration(final Configuration conf) {
        InputConfigurator.setZooKeeperInstance(AccumuloInputFormat.class,
                conf,
                miniAccumuloCluster.getClientConfig());
    }

    public MiniAccumuloCluster getMiniAccumuloCluster() {
        return miniAccumuloCluster;
    }

    public Connector getMiniConnector() {
        return miniConnector;
    }

    OperationHandler getOperationHandlerExposed(final Class<? extends Operation> opClass) {
        return super.getOperationHandler(opClass);
    }

    public void closeMiniAccumuloStore() {
        if (null == miniAccumuloCluster) {
            return;
        }
        try {
            miniAccumuloCluster.stop();
        } catch (final IOException | InterruptedException e) {
            // Ignore any errors here;
        }
        try {
            FileUtils.deleteDirectory(new File(miniAccumuloCluster.getConfig().getDir().getAbsolutePath()));
        } catch (final IOException e) {
            // Ignore any errors here;
        }
    }
}
