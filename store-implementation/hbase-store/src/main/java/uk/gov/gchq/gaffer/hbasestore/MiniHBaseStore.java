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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.IOException;

public class MiniHBaseStore extends HBaseStore {
    private static HBaseTestingUtility utility;

    @SuppressFBWarnings("DE_MIGHT_IGNORE")
    @Override
    public void initialise(final Schema schema, final StoreProperties properties)
            throws StoreException {
        if (!(properties instanceof HBaseProperties)) {
            throw new StoreException("Store must be initialised with HBaseProperties");
        }

        if (null == utility) {
            utility = new HBaseTestingUtility();
            try {
                utility.startMiniCluster();
            } catch (Exception e) {
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

    @Override
    public Connection getConnection() throws StoreException {
        try {
            return utility.getConnection();
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }
}
