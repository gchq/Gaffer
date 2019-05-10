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

package uk.gov.gchq.gaffer.hbasestore;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import uk.gov.gchq.gaffer.hbasestore.utils.TableUtils;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

/**
 * Uses {@link org.apache.hadoop.hbase.HBaseTestingUtility} to create a mini hbase instance for testing.
 * Due to the additional dependencies required for the mini hbase instance this store
 * will not work with the Gaffer REST API. There are conflicting versions of JAX-RS
 * - the hbase testing utility requires JAX-RS 1 but the REST API needs JAX-RS 2.
 * <p>
 * This single use mini hbase store will delete an existing hbase table when it
 * is initialised
 * </p>
 */
public class SingleUseMiniHBaseStore extends MiniHBaseStore {
    @SuppressFBWarnings({"DE_MIGHT_IGNORE", "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD"})
    @Override
    public void preInitialise(final String graphId, final Schema schema, final StoreProperties properties)
            throws StoreException {
        try {
            super.preInitialise(graphId, schema, properties);
        } catch (final StoreException e) {
            // This is due to an invalid table, but the table is about to be deleted to we can ignore it.
        }

        TableUtils.dropTable(this);
        super.preInitialise(graphId, schema, getProperties());
    }
}
