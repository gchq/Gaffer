/*
 * Copyright 2017-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore;

import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Collections;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO_WITH_EDGES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO_WITH_ENTITIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.addGraph;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;

public class PredefinedFederatedStore extends FederatedStore {

    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        resetForFederatedTests();

        super.initialise(graphId, schema, properties);

        try {
            // Accumulo store just contains edges
            addGraph(this, GRAPH_ID_ACCUMULO_WITH_EDGES, false,
                    new Schema.Builder()
                            .merge(schema.clone())
                            //delete Entities
                            .entities(Collections.emptyMap())
                            .build());

            // Accumulo store just contains entities
            addGraph(this, GRAPH_ID_ACCUMULO_WITH_ENTITIES, false,
                    new Schema.Builder()
                            .merge(schema.clone())
                            //delete Edges
                            .edges(Collections.emptyMap())
                            .build());

        } catch (final StorageException e) {
            throw new StoreException(e.getMessage(), e);
        }
    }
}
