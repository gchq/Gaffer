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
import org.apache.accumulo.core.client.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

public class SingleUseMiniAccumuloStore extends MiniAccumuloStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleUseMiniAccumuloStore.class);

    @Override
    public void preInitialise(final String graphId, final Schema schema, final StoreProperties properties)
            throws StoreException {
        // Initialise is deliberately called both before and after the deletion of the table.
        // The first call sets up a connection to the MiniAccumulo instance
        // The second call is used to re-create the table

        try {
            super.preInitialise(graphId, schema, properties);
        } catch (final StoreException e) {
            // This is due to an invalid table, but the table is about to be deleted to we can ignore it.
            LOGGER.info("Invalid table: no action required as it is being deleted anyway: " + e.getMessage());
        }

        try {
            getConnection().tableOperations().delete(getTableName());
        } catch (final StoreException | AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            LOGGER.warn("Table deletion failed: " + e.getMessage());
        }
        super.preInitialise(graphId, schema, properties);
    }

}
