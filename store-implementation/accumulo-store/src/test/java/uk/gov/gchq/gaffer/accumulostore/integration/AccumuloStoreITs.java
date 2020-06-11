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
package uk.gov.gchq.gaffer.accumulostore.integration;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.hdfs.integration.loader.AddElementsFromHdfsLoaderIT;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import java.io.IOException;

public class AccumuloStoreITs extends AbstractStoreITs {
    private static final AccumuloProperties STORE_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreITs.class));
    private static MiniAccumuloCluster cluster;

    public AccumuloStoreITs() {
        this(STORE_PROPERTIES);
    }

    protected AccumuloStoreITs(final AccumuloProperties storeProperties) {
        super(storeProperties);
        addExtraTest(AddElementsFromHdfsLoaderIT.class);
    }

    @BeforeClass
    public static void ensureClusterExists() {
        // Do a check first before setting this up. This is so that when we run this on a full cluster,
        // the MiniCluster doesn't get created.
        final TemporaryFolder miniAccumuloDirectory;
        try {
            // should use some other method for this. As create() technically isn't supposed to be used.
            miniAccumuloDirectory = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
            miniAccumuloDirectory.create();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        AccumuloProperties props = STORE_PROPERTIES.clone();
        MiniAccumuloConfig config = new MiniAccumuloConfig(miniAccumuloDirectory.getRoot(), props.getPassword());
        config.setInstanceName(props.getInstance());
        config.setZooKeeperPort(2181);

        try {
            cluster = new MiniAccumuloCluster(config);
            cluster.start();
            // As this is just for demo purposes, I've changed the usernames to "root" but if you like,
            // You can set the original up here.
            cluster.getConnector(props.getUser(), props.getPassword()).securityOperations()
                    .changeUserAuthorizations(props.getUser(), new Authorizations("vis1","vis2","publicVisibility","privateVisibility","public","private"));
        } catch (IOException | InterruptedException | AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void tearDownAccumuloCluster() {
        for(int retries = 0; retries < 3; retries++) {
            try {
                cluster.stop();
                break;
            } catch (IOException e) {
                e.printStackTrace(); // don't actually do this - use loggers.
                continue;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            throw new RuntimeException("Failed to close accumulo cluster"); // probably don't do this either. Just tell the user it's failed.
        }

    }


}
