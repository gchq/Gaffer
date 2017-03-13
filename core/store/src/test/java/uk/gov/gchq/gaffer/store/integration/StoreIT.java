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

package uk.gov.gchq.gaffer.store.integration;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.STORE_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;

public class StoreIT {
    @Test
    public void shouldCreateStoreAndValidateSchemas() throws IOException, SchemaException, StoreException {
        // Given
        final TestStore testStore = new TestStore();
        final Schema schema = Schema.fromJson(StreamUtil.schemas(getClass()));

        // When
        testStore.initialise(schema, new StoreProperties());

        // Then
        assertTrue(testStore.getSchema().getEdges().containsKey(TestGroups.EDGE));
        assertTrue(testStore.getSchema().getEdges().containsKey(TestGroups.EDGE));

        assertTrue(testStore.getSchema().getEntities().containsKey(TestGroups.ENTITY));
        assertTrue(testStore.getSchema().getEntities().containsKey(TestGroups.ENTITY));

        assertFalse(testStore.getSchema().getEdges().containsKey(TestGroups.EDGE_2));
        assertFalse(testStore.getSchema().getEntities().containsKey(TestGroups.ENTITY_2));

        assertFalse(testStore.getSchema().getEdges().containsKey(TestGroups.EDGE_2));
        assertFalse(testStore.getSchema().getEntities().containsKey(TestGroups.ENTITY_2));

        assertTrue(testStore.getSchema().validate());
    }

    private class TestStore extends Store {
        private final Set<StoreTrait> TRAITS = new HashSet<>(Arrays.asList(STORE_AGGREGATION, PRE_AGGREGATION_FILTERING, TRANSFORMATION));

        @Override
        public Set<StoreTrait> getTraits() {
            return TRAITS;
        }

        @Override
        protected void addAdditionalOperationHandlers() {
        }

        @Override
        protected OperationHandler<GetElements<ElementSeed, Element>, CloseableIterable<Element>> getGetElementsHandler() {
            return null;
        }

        @Override
        protected OperationHandler<GetAllElements<Element>, CloseableIterable<Element>> getGetAllElementsHandler() {
            return null;
        }

        @Override
        protected OperationHandler<? extends GetAdjacentEntitySeeds, CloseableIterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
            return null;
        }

        @Override
        protected OperationHandler<? extends AddElements, Void> getAddElementsHandler() {
            return null;
        }

        @Override
        protected <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation, final Context context) {
            return null;
        }

        @Override
        public boolean isValidationRequired() {
            return false;
        }
    }
}
