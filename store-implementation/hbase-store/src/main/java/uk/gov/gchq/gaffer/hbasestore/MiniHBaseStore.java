/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.hbasestore;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.codec.KeyValueCodecWithTags;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.visibility.ScanLabelGenerator;
import org.apache.hadoop.hbase.security.visibility.SimpleScanLabelGenerator;
import org.apache.hadoop.hbase.security.visibility.VisibilityClient;
import org.apache.hadoop.hbase.security.visibility.VisibilityConstants;
import org.apache.hadoop.hbase.security.visibility.VisibilityTestUtil;
import org.apache.hadoop.hbase.security.visibility.VisibilityUtils;
import uk.gov.gchq.gaffer.hbasestore.utils.TableUtils;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class MiniHBaseStore extends HBaseStore {
    private static HBaseTestingUtility utility;
    private static Connection connection;

    @SuppressFBWarnings({"DE_MIGHT_IGNORE", "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD"})
    @Override
    public void initialise(final Schema schema, final StoreProperties properties)
            throws StoreException {
        if (!(properties instanceof HBaseProperties)) {
            throw new StoreException("Store must be initialised with HBaseProperties");
        }

        if (null == utility) {
            try {
                final Configuration conf = setupConf();
                utility = new HBaseTestingUtility(conf);
                utility.startMiniCluster();
                utility.waitTableEnabled(VisibilityConstants.LABELS_TABLE_NAME.getName(), 50000);
                conf.set("fs.defaultFS", "file:///");
                addLabels(((HBaseProperties) properties).getMiniHBaseVisibilities());
            } catch (final Exception e) {
                throw new StoreException(e);
            }
        }

        super.initialise(schema, properties);

        try {
            TableUtils.deleteAllRows(this, getProperties().getMiniHBaseVisibilities());
        } catch (final StoreException e) {
            TableUtils.dropTable(this);
            TableUtils.createTable(this);
        }
    }

    @Override
    public Configuration getConfiguration() {
        if (null == utility) {
            throw new RuntimeException("Store has not yet been initialised");
        }
        return utility.getConfiguration();
    }

    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    @Override
    public Connection getConnection() throws StoreException {
        if (null == utility) {
            throw new StoreException("Store has not yet been initialised");
        }

        if (null == connection || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(utility.getConfiguration());
            } catch (final IOException e) {
                throw new StoreException(e);
            }
        }
        return connection;
    }

    @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
    public void addLabels(final String... visibilities) throws IOException, InterruptedException {
        if (visibilities.length > 0) {
            PrivilegedExceptionAction<VisibilityLabelsProtos.VisibilityLabelsResponse> action =
                    new PrivilegedExceptionAction<VisibilityLabelsProtos.VisibilityLabelsResponse>() {
                        public VisibilityLabelsProtos.VisibilityLabelsResponse run() throws Exception {
                            try (Connection conn = ConnectionFactory.createConnection(utility.getConfiguration())) {
                                VisibilityClient.addLabels(conn, visibilities);
                            } catch (final Throwable t) {
                                throw new IOException(t);
                            }
                            return null;
                        }
                    };

            final User superUser = User.createUserForTesting(getConfiguration(), "admin", new String[]{"supergroup"});
            superUser.runAs(action);
        }
    }

    private Configuration setupConf() throws IOException {
        final Configuration conf = HBaseConfiguration.create();
        VisibilityTestUtil.enableVisiblityLabels(conf);
        conf.set("hbase.superuser", "admin");
        conf.setInt("hfile.format.version", 3);
        conf.set("mapreduce.jobtracker.address", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.setClass(VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS, SimpleScanLabelGenerator.class,
                ScanLabelGenerator.class);
        conf.set(HConstants.REPLICATION_CODEC_CONF_KEY, KeyValueCodecWithTags.class.getName());
        return conf;
    }
}
