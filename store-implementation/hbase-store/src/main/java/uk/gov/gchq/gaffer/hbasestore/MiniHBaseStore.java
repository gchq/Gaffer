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

package uk.gov.gchq.gaffer.hbasestore;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.visibility.ScanLabelGenerator;
import org.apache.hadoop.hbase.security.visibility.SimpleScanLabelGenerator;
import org.apache.hadoop.hbase.security.visibility.VisibilityClient;
import org.apache.hadoop.hbase.security.visibility.VisibilityConstants;
import org.apache.hadoop.hbase.security.visibility.VisibilityTestUtil;
import org.apache.hadoop.hbase.security.visibility.VisibilityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class MiniHBaseStore extends HBaseStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseStore.class);

    private static HBaseTestingUtility utility;
    private User superUser;
    private Configuration conf;

    @SuppressFBWarnings("DE_MIGHT_IGNORE")
    @Override
    public void initialise(final Schema schema, final StoreProperties properties)
            throws StoreException {
        if (!(properties instanceof HBaseProperties)) {
            throw new StoreException("Store must be initialised with HBaseProperties");
        }

        if (null == utility) {
            try {
                setupConf();
                utility = new HBaseTestingUtility(conf);
                utility.startMiniCluster();
                utility.waitTableEnabled(VisibilityConstants.LABELS_TABLE_NAME.getName(), 50000);

                addLabels();
            } catch (Throwable e) {
                throw new StoreException(e);
            }
        }

        // Initialise is deliberately called both before and after the deletion of the table.
        // The first call sets up a connection to the HBase
        // The second call is used to re-create the table
        super.initialise(schema, properties);
        try {
            getConnection().getAdmin().deleteTable(getProperties().getTable());
        } catch (Exception e) {
            // no action required.
        }
        super.initialise(schema, properties);
    }

    private void setupConf() throws IOException {
        conf = HBaseConfiguration.create();
        VisibilityTestUtil.enableVisiblityLabels(conf);
        conf.set("hbase.superuser", "admin");
        conf.setInt("hfile.format.version", 3);
        conf.setClass(VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS, SimpleScanLabelGenerator.class,
                ScanLabelGenerator.class);
        superUser = User.createUserForTesting(conf, "admin", new String[]{"supergroup"});
    }

    @Override
    public Connection getConnection() throws StoreException {
        try {
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }

    public void addLabels() throws Exception {
        PrivilegedExceptionAction<VisibilityLabelsProtos.VisibilityLabelsResponse> action =
                new PrivilegedExceptionAction<VisibilityLabelsProtos.VisibilityLabelsResponse>() {
                    public VisibilityLabelsProtos.VisibilityLabelsResponse run() throws Exception {
                        //TODO Should these labels come from properties?
                        String[] labels = {"public", "private"};
                        try (Connection conn = ConnectionFactory.createConnection(conf)) {
                            VisibilityClient.addLabels(conn, labels);
                        } catch (Throwable t) {
                            throw new IOException(t);
                        }
                        return null;
                    }
                };
        superUser.runAs(action);
    }
}
