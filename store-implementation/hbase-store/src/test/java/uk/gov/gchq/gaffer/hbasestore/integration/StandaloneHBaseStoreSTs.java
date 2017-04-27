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
package uk.gov.gchq.gaffer.hbasestore.integration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.hbasestore.HBaseProperties;
import uk.gov.gchq.gaffer.hbasestore.utils.TableUtils;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import java.io.IOException;

public class StandaloneHBaseStoreSTs extends AbstractStoreITs {
    private static final HBaseProperties STORE_PROPERTIES = (HBaseProperties) StoreProperties.loadStoreProperties(StreamUtil.openStream(StandaloneHBaseStoreSTs.class, "standalone.store.properties"));

    public StandaloneHBaseStoreSTs() throws StoreException {
        super(STORE_PROPERTIES);
        dropExistingTable();
    }

    private void dropExistingTable() throws StoreException {
        final Configuration conf = HBaseConfiguration.create();
        if (null != STORE_PROPERTIES.getZookeepers()) {
            conf.set("hbase.zookeeper.quorum", STORE_PROPERTIES.getZookeepers());
        }

        final Connection connection;
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (final IOException e) {
            throw new StoreException("Unable to create connection", e);
        }

        TableUtils.dropTable(connection, STORE_PROPERTIES);
    }
}
