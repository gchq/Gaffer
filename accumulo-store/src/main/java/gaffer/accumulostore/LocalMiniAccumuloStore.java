/*
 * Copyright 2016 Crown Copyright
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

package gaffer.accumulostore;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gaffer.store.StoreException;
import gaffer.store.StoreProperties;
import gaffer.store.schema.Schema;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * An {@link AccumuloStore} that creates an {@link MiniAccumuloCluster} running
 * locally.
 */
public class LocalMiniAccumuloStore extends AccumuloStore {
    public static final String USER_NAME = "root";
    public static final String TEMP_DIR_NAME = "accumuloMiniCluster";

    private MiniAccumuloCluster cluster;
    private Path tempDirectory;

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "The properties should always be AccumuloProperties")
    @Override
    public void initialise(final Schema schema, final StoreProperties properties) throws StoreException {
        final AccumuloProperties accProps = (AccumuloProperties) properties;
        cluster = createMiniCluster(accProps);

        accProps.setZookeepers(cluster.getZooKeepers());
        accProps.setUserName(USER_NAME);
        super.initialise(schema, properties);
    }

    public Path getTempDirectory() {
        return tempDirectory;
    }

    public void stop() {
        if (null != cluster) {
            try {
                cluster.stop();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    protected MiniAccumuloCluster createMiniCluster(final AccumuloProperties accProps) {
        final MiniAccumuloCluster newCluster;
        try {
            tempDirectory = Files.createTempDirectory(TEMP_DIR_NAME);
            tempDirectory.toFile().deleteOnExit();

            final MiniAccumuloConfig config = new MiniAccumuloConfig(tempDirectory.toFile(), accProps.getPassword());
            config.setInstanceName(accProps.getInstanceName());
            newCluster = new MiniAccumuloCluster(config);
            newCluster.start();
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }

        return newCluster;
    }

    protected MiniAccumuloCluster getMiniCluster() {
        return cluster;
    }
}
