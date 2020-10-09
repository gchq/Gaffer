/*
 * Copyright 2020 Crown Copyright
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

/**
 * A {@link MiniAccumuloStore} which deletes the table on initialisation.
 */
public class SingleUseMiniAccumuloStore extends MiniAccumuloStore {
    // Initialise is deliberately called both before and after the deletion of the table.
    // The first call sets up a connection to the Accumulo instance
    // The second call is used to re-create the table
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleUseMiniAccumuloStore.class);


    @Override
    public synchronized void preInitialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        try {
            super.preInitialise(graphId, schema, properties);
        } catch (final StoreException e) {
            LOGGER.info("Table may be invalid. Will be deleted now anyway", e);
        }

        try {
            getConnection().tableOperations().delete(getTableName());
        } catch (final StoreException | AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            LOGGER.info("Failed to delete the table", e);
        }
        super.preInitialise(graphId, schema, properties);
    }
}
