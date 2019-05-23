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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;

import java.io.File;
import java.io.IOException;

public final class MiniAccumuloClusterProvider {
    public static final String ROOT = "root";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    private static File tempFolder = new File(CommonTestConstants.TMP_DIRECTORY + File.separator
            + "MiniAccumuloCluster-spark-accumulo-library-tests");

    private static MiniAccumuloCluster cluster;
    private static AccumuloProperties accumuloProperties;

    private MiniAccumuloClusterProvider() {
        // private to prevent instantiation
    }

    public static synchronized MiniAccumuloCluster getMiniAccumuloCluster() throws IOException, InterruptedException,
            AccumuloSecurityException, AccumuloException {
        if (null == cluster) {
            createCluster();
        }
        return cluster;
    }

    public static synchronized AccumuloProperties getAccumuloProperties() throws IOException, InterruptedException,
            AccumuloException, AccumuloSecurityException {
        // Ensure cluster has been created - can ignore the result of the next method
        getMiniAccumuloCluster();
        return accumuloProperties;
    }

    private static void createCluster() throws IOException, InterruptedException, AccumuloSecurityException, AccumuloException {
        if (tempFolder.exists()) {
            FileUtils.cleanDirectory(tempFolder);
        }
        final MiniAccumuloConfig miniAccumuloConfig = new MiniAccumuloConfig(tempFolder, PASSWORD);
        cluster = new MiniAccumuloCluster(miniAccumuloConfig);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    cluster.stop();
                    tempFolder.delete();
                } catch (final IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        cluster.start();
        // Create user USER with permissions to create a table
        cluster.getConnector(ROOT, PASSWORD).securityOperations().createLocalUser(USER, new PasswordToken(PASSWORD));
        cluster.getConnector(ROOT, PASSWORD).securityOperations().grantSystemPermission(USER, SystemPermission.CREATE_TABLE);
        // Create properties
        accumuloProperties = new AccumuloProperties();
        accumuloProperties.setStoreClass(AccumuloStore.class);
        accumuloProperties.setInstance(cluster.getInstanceName());
        accumuloProperties.setZookeepers(cluster.getZooKeepers());
        accumuloProperties.setUser(MiniAccumuloClusterProvider.USER);
        accumuloProperties.setPassword(MiniAccumuloClusterProvider.PASSWORD);
        accumuloProperties.setOperationDeclarationPaths("sparkAccumuloOperationsDeclarations.json");
    }
}
