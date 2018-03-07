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

package uk.gov.gchq.gaffer.hbasestore;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.codec.KeyValueCodecWithTags;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.visibility.ScanLabelGenerator;
import org.apache.hadoop.hbase.security.visibility.SimpleScanLabelGenerator;
import org.apache.hadoop.hbase.security.visibility.VisibilityClient;
import org.apache.hadoop.hbase.security.visibility.VisibilityConstants;
import org.apache.hadoop.hbase.security.visibility.VisibilityTestUtil;
import org.apache.hadoop.hbase.security.visibility.VisibilityUtils;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Uses {@link HBaseTestingUtility} to create a mini hbase instance for testing.
 * Due to the additional dependencies required for the mini hbase instance this store
 * will not work with the Gaffer REST API. There are conflicting versions of JAX-RS
 * - the hbase testing utility requires JAX-RS 1 but the REST API needs JAX-RS 2.
 */
public class MiniHBaseStore extends HBaseStore {
    /**
     * Comma separated visibilities for use with the Mini HBase store.
     */
    public static final String MINI_HBASE_VISIBILITIES = "hbase.mini.visibilities";
    public static final String ADMIN_USERNAME = "admin";

    private static HBaseTestingUtility utility;
    private static Connection connection;

    @SuppressFBWarnings({"DE_MIGHT_IGNORE", "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD"})
    @Override
    public void preInitialise(final String graphId, final Schema schema, final StoreProperties properties)
            throws StoreException {
        setProperties(properties);

        if (null == utility) {
            try {
                final Configuration conf = setupConf();
                utility = new HBaseTestingUtility(conf);
                utility.startMiniCluster();
                utility.waitTableEnabled(VisibilityConstants.LABELS_TABLE_NAME.getName(), 50000);
                conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);
                addLabels(getMiniHBaseVisibilities());
            } catch (final Exception e) {
                throw new StoreException(e);
            }
        }

        super.preInitialise(graphId, schema, getProperties());
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
                        @Override
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

    OperationHandler getOperationHandlerExposed(final Class<? extends Operation> opClass) {
        return super.getOperationHandler(opClass);
    }

    private Configuration setupConf() throws IOException {
        final Configuration conf = HBaseConfiguration.create();
        VisibilityTestUtil.enableVisiblityLabels(conf);
        conf.set(Superusers.SUPERUSER_CONF_KEY, ADMIN_USERNAME);
        conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_TAGS);
        conf.set(JTConfig.JT_IPC_ADDRESS, JTConfig.LOCAL_FRAMEWORK_NAME);
        conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);
        conf.setClass(VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS, SimpleScanLabelGenerator.class,
                ScanLabelGenerator.class);
        conf.set(HConstants.REPLICATION_CODEC_CONF_KEY, KeyValueCodecWithTags.class.getName());
        return conf;
    }

    private String[] getMiniHBaseVisibilities() {
        final String visibilityCsv = getProperties().get(MINI_HBASE_VISIBILITIES);
        if (null == visibilityCsv) {
            return new String[0];
        }

        final String[] visibilities = visibilityCsv.split(",");
        for (int i = 0; i < visibilities.length; i++) {
            visibilities[i] = visibilities[i].trim();
        }

        return visibilities;
    }
}
