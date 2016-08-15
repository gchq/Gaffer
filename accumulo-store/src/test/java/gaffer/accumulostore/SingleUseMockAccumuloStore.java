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

package gaffer.accumulostore;

import gaffer.store.StoreException;
import gaffer.store.StoreProperties;
import gaffer.store.schema.Schema;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;

/**
 * An {@link AccumuloStore} that uses an Accumulo {@link MockInstance} to
 * provide a {@link Connector}.
 *
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
        }
        super.initialise(schema, properties);
    }

}
