/*
 * Copyright 2016-2024 Crown Copyright
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.DeleteAllData;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.handler.GetTraitsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;

public class StoreIT {
    @BeforeEach
    void before() {
        CacheServiceLoader.shutdown();
    }

    @Test
    public void shouldCreateStoreAndValidateSchemas() throws SchemaException, StoreException {
        // Given
        final TestStore testStore = new TestStore();
        final Schema schema = Schema.fromJson(StreamUtil.schemas(getClass()));

        // When
        testStore.initialise("graphId", schema, new StoreProperties());

        // Then
        assertThat(testStore.getSchema().getEdges()).containsKey(TestGroups.EDGE)
                                                    .containsKey(TestGroups.EDGE);

        assertThat(testStore.getSchema().getEntities()).containsKey(TestGroups.ENTITY)
                                                       .containsKey(TestGroups.ENTITY);

        assertThat(testStore.getSchema().getEdges()).doesNotContainKey(TestGroups.EDGE_2);
        assertThat(testStore.getSchema().getEntities()).doesNotContainKey(TestGroups.ENTITY_2);

        assertThat(testStore.getSchema().getEdges()).doesNotContainKey(TestGroups.EDGE_2);
        assertThat(testStore.getSchema().getEntities()).doesNotContainKey(TestGroups.ENTITY_2);

        assertThat(testStore.getSchema().validate().isValid()).isTrue();
    }

    @Test
    public void shouldCreateStoreWithSpecifiedCaches() throws SchemaException, StoreException {
        // Given
        final Store testStore = new TestStore();

        // When
        testStore.initialise("testGraph", new Schema(), StoreProperties.loadStoreProperties("allCaches.properties"));

        // Then
        assertThat(CacheServiceLoader.isDefaultEnabled()).isFalse();
        assertThat(CacheServiceLoader.isEnabled("JobTracker")).isTrue();
        assertThat(CacheServiceLoader.isEnabled("NamedView")).isTrue();
        assertThat(CacheServiceLoader.isEnabled("NamedOperation")).isTrue();
    }

    @Test
    public void shouldCreateStoreWithoutAnyCaches() throws SchemaException, StoreException {
        // Given
        final Store testStore = new TestStore();
        final StoreProperties properties = new StoreProperties();
        properties.setJobTrackerEnabled(false);
        properties.setNamedViewEnabled(false);
        properties.setNamedOperationEnabled(false);

        // When
        testStore.initialise("testGraph", new Schema(), properties);

        // Then
        assertThat(CacheServiceLoader.isDefaultEnabled()).isFalse();
        assertThat(CacheServiceLoader.isEnabled("JobTracker")).isFalse();
        assertThat(CacheServiceLoader.isEnabled("NamedView")).isFalse();
        assertThat(CacheServiceLoader.isEnabled("NamedOperation")).isFalse();
    }

    private class TestStore extends Store {
        private final Set<StoreTrait> traits = new HashSet<>(Arrays.asList(INGEST_AGGREGATION, PRE_AGGREGATION_FILTERING, TRANSFORMATION));

        @Override
        protected void addAdditionalOperationHandlers() {
        }

        @Override
        protected OutputOperationHandler<GetElements, Iterable<? extends Element>> getGetElementsHandler() {
            return null;
        }

        @Override
        protected OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getGetAllElementsHandler() {
            return null;
        }

        @Override
        protected OutputOperationHandler<? extends GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler() {
            return null;
        }

        @Override
        protected OperationHandler<? extends AddElements> getAddElementsHandler() {
            return null;
        }

        @Override
        protected OperationHandler<? extends DeleteElements> getDeleteElementsHandler() {
            return null;
        }

        @Override
        protected OperationHandler<DeleteAllData> getDeleteAllDataHandler() {
            return null;
        }

        @Override
        protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
            return new GetTraitsHandler(traits);
        }

        @SuppressWarnings({"rawtypes"})
        @Override
        protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
            return ToBytesSerialiser.class;
        }
    }
}
