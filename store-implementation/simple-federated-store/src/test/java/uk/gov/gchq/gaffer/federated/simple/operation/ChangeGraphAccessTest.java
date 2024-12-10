/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple.operation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.access.predicate.NoAccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.UnrestrictedAccessPredicate;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.access.GraphAccess;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class ChangeGraphAccessTest {

    @AfterEach
    void reset() {
        CacheServiceLoader.shutdown();
    }

    @Test
    void shouldChangeAccessOfGraph() throws StoreException, OperationException, CacheOperationException {
        final String federatedGraphId = "federated";
        final String graphId = "shouldChangeAccessOfGraph";

        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(federatedGraphId, null, new StoreProperties());

        // Add a graph with no restrictions
        final AddGraph addGraph = new AddGraph.Builder()
                .graphConfig(new GraphConfig(graphId))
                .schema(new Schema())
                .properties(new StoreProperties().getProperties())
                .owner("oldOwner")
                .isPublic(true)
                .readPredicate(new UnrestrictedAccessPredicate())
                .writePredicate(new UnrestrictedAccessPredicate())
                .build();

        // Change the graph access
        final ChangeGraphAccess changeGraphAccess = new ChangeGraphAccess.Builder()
                .graphId(graphId)
                .owner("newOwner")
                .isPublic(false)
                .readPredicate(new NoAccessPredicate())
                .writePredicate(new NoAccessPredicate())
                .build();

        // When
        federatedStore.execute(addGraph, new Context());
        federatedStore.execute(changeGraphAccess, new Context());

        GraphAccess updatedAccess = federatedStore.getGraphAccess(graphId);

        // Then
        assertThat(updatedAccess.getOwner()).isEqualTo("newOwner");
        assertThat(updatedAccess.isPublic()).isFalse();
        assertThat(updatedAccess.getReadAccessPredicate()).isInstanceOf(NoAccessPredicate.class);
        assertThat(updatedAccess.getWriteAccessPredicate()).isInstanceOf(NoAccessPredicate.class);
    }

    @Test
    void shouldNotChangeAccessOfAccessControlledGraph() throws StoreException, OperationException {
        final String federatedGraphId = "federated";
        final String graphId = "shouldNotChangeAccessOfAccessControlledGraph";

        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(federatedGraphId, null, new StoreProperties());

        // Add a graph that no one can edit
        final AddGraph addGraph = new AddGraph.Builder()
                .graphConfig(new GraphConfig(graphId))
                .schema(new Schema())
                .properties(new StoreProperties().getProperties())
                .isPublic(false)
                .writePredicate(new NoAccessPredicate())
                .build();

        final ChangeGraphAccess changeGraphAccess = new ChangeGraphAccess.Builder()
            .graphId(graphId)
            .isPublic(true)
            .build();

        // When
        federatedStore.execute(addGraph, new Context());

        // Then
        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> federatedStore.execute(changeGraphAccess, new Context()))
                .withMessageContaining("does not have write permissions");
    }
}
