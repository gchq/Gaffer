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

package gaffer.store.integration;

import static gaffer.store.StoreTrait.AGGREGATION;
import static gaffer.store.StoreTrait.FILTERING;
import static gaffer.store.StoreTrait.TRANSFORMATION;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import gaffer.commonutil.StreamUtil;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.operation.Operation;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.operation.impl.get.GetElements;
import gaffer.store.Context;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.StoreProperties;
import gaffer.store.StoreTrait;
import gaffer.store.operation.handler.OperationHandler;
import gaffer.store.schema.Schema;
import org.junit.Test;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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
        private final Set<StoreTrait> TRAITS = new HashSet<>(Arrays.asList(AGGREGATION, FILTERING, TRANSFORMATION));

        @Override
        public Set<StoreTrait> getTraits() {
            return TRAITS;
        }

        @Override
        protected void addAdditionalOperationHandlers() {
        }

        @Override
        protected OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> getGetElementsHandler() {
            return null;
        }

        @Override
        protected OperationHandler<GetAllElements<Element>, Iterable<Element>> getGetAllElementsHandler() {
            return null;
        }

        @Override
        protected OperationHandler<? extends GetAdjacentEntitySeeds, Iterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
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
