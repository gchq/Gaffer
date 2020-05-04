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
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * An {@link AccumuloStore} that uses an Accumulo {@link MiniAccumuloCluster} to
 * provide a {@link Connector}.
 */
public class MiniAccumuloStore extends AccumuloStore {
    private static final String BASE_DIRECTORY = "miniAccumuloStoreTest-";
    private static final String ROOTPW = "rootPW";
    private MiniAccumuloCluster miniAccumuloCluster = null;
    private MiniAccumuloConfig miniAccumuloConfig = null;
    private Connector miniConnector;

    @Override
    public StoreProperties setUpTestDB(final StoreProperties properties) throws StoreException {
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
            miniAccumuloConfig = new MiniAccumuloConfig(baseDir, ROOTPW);
            miniAccumuloConfig.setInstanceName(getProperties().getInstance());
            miniAccumuloCluster = new MiniAccumuloCluster(miniAccumuloConfig);
            miniAccumuloCluster.start();
        } catch (final IOException | InterruptedException e) {
            throw new StoreException(e.getMessage(), e);
        }

        // Create the user specified in the properties together with the specified password and give them all authorisations
        try {
            miniAccumuloCluster.getConnector("root", ROOTPW).securityOperations()
                    .createLocalUser(getProperties().getUser(), new PasswordToken(getProperties().getPassword()));
            miniAccumuloCluster.getConnector("root", ROOTPW).securityOperations()
                    .grantSystemPermission(getProperties().getUser(), SystemPermission.CREATE_TABLE);
            Authorizations auths = new Authorizations("public", "private", "publicVisibility", "privateVisibility", "vis1", "vis2");
            miniAccumuloCluster.getConnector("root", ROOTPW).securityOperations()
                    .changeUserAuthorizations(getProperties().getUser(), auths);
        } catch (final AccumuloException | AccumuloSecurityException e) {
            throw new StoreException(e.getMessage(), e);
        }

        // Create the new properties object to pass back, including connection items
        AccumuloProperties accumuloProperties = (AccumuloProperties) properties.clone();
        accumuloProperties.setInstance(miniAccumuloCluster.getInstanceName());
        accumuloProperties.setZookeepers(miniAccumuloCluster.getZooKeepers());

        return accumuloProperties;
    }

    @Override
    public void tearDownTestDB() {
        this.closeMiniAccumuloStore();
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

    private void closeMiniAccumuloStore() {
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
