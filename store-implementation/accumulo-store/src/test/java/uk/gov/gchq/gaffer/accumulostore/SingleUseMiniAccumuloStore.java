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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link MiniAccumuloStore} which deletes the table on initialisation.
 */
public class SingleUseMiniAccumuloStore extends MiniAccumuloStore {
    // Initialise is deliberately called both before and after the deletion of the table.
    // The first call sets up a connection to the Accumulo instance
    // The second call is used to re-create the table
    private int restartAttempts = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleUseMiniAccumuloStore.class);


    @Override
    public synchronized void preInitialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        try {
            super.preInitialise(graphId, schema, properties);
        } catch (final StoreException e) {
            LOGGER.info("Table may be invalid. Will be deleted now anyway", e);
        }

        try {
            List<Throwable> errors = new ArrayList<>();
            if (getConnection().tableOperations().exists(getTableName())) {
                Thread tableDeleteThread = new Thread(() -> {
                    try {
                        this.getConnection().tableOperations().delete(getTableName());
                    } catch (final Exception e) {
                        errors.add(e);
                    }
                });

                long thirtySecondsFromNow = System.currentTimeMillis() + 1000 * 30;
                boolean succeeded = false;
                tableDeleteThread.start();
                while (System.currentTimeMillis() < thirtySecondsFromNow) {
                    if (tableDeleteThread.isAlive()) {
                        // Still doing it's thing
                        Thread.sleep(1000);
                    } else {
                        if (errors.size() == 0) {
                            succeeded = true;
                        }
                        break;
                    }
                }
                if (!succeeded) {
                    if (errors.size() == 0) {
                        throw new StoreException("Failed to connect to Zookeeper in 30 seconds. It's likely down");
                    } else {
                        throw new StoreException("Failed to delete table", errors.get(0));
                    }
                }
            }
        } catch (final StoreException | InterruptedException e) {
            LOGGER.warn("Failed to delete the table", e);
            if (restartAttempts < 3) {
                restartAttempts++;
                reset();
            } else {
                throw new RuntimeException("Attempted to restart MiniAccumulo store 3 times. Exiting.", e);
            }
        }
        super.preInitialise(graphId, schema, properties);
        restartAttempts = 0;
    }
}
