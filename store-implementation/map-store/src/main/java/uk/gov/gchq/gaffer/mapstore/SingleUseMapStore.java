/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.mapstore;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

/**
 * A simple {@link MapStore} instance suitable for testing.
 */
public class SingleUseMapStore extends MapStore {

    @SuppressFBWarnings(value = "DE_MIGHT_IGNORE", justification = "Exception ignored while clearing previous maps before reinitialising.")
    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        MapStore.resetStaticMap();

        try {
            super.initialise(graphId, schema, properties);
        } catch (final Exception e) {
            // Ignore errors as it will be reinitialised just below.
        }

        if (null != getMapImpl()) {
            getMapImpl().clear();
        }

        super.initialise(graphId, schema, properties);
    }
}
