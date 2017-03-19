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

package uk.gov.gchq.gaffer.accumulostore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

/**
 * An {@link AccumuloStore} that uses an Accumulo {@link org.apache.accumulo.core.client.mock.MockInstance} to
 * provide a {@link org.apache.accumulo.core.client.Connector}.
 * For the SingleUseMockAccumuloStore each time initialise is called the underlying table as set in the store properties
 * is deleted.
 */
public class SingleUseMockAccumuloStore extends MockAccumuloStore {
    @Override
    public void initialise(final Schema schema, final StoreProperties properties)
            throws StoreException {
        // Initialise is deliberately called both before and after the deletion of the table: the first creates
        // the MockInstance and then creates the table. This ensures the getConnection() method works. The table is
        // then deleted, and recreated in the following initialise call.
        super.initialise(schema, properties);
        try {
            getConnection().tableOperations().delete(getProperties().getTable());
        } catch (StoreException | AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            // no action required
        }
        super.initialise(schema, properties);
    }

}
